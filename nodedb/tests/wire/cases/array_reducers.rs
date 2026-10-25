// SPDX-License-Identifier: BUSL-1.1

//! The exact `ARRAY_AGG` reducers on a four-core server, so a read merges
//! partials across cores and vShards before it finalizes.
//!
//! The fixture is a 16x16 array `red` with dims `t`, `x` and tiles of 4x4,
//! so its cells spread over 16 tiles and every core. Cell `(t, x)` holds
//! `v = (7 * t + 3 * x) % 11` as `INT16`, half the cells before a flush
//! and half after. Every expected value is computed here from the same
//! formula, exactly, then compared.

use crate::harness::TestServer;

fn value(t: i64, x: i64) -> i64 {
    (7 * t + 3 * x) % 11
}

fn cells() -> impl Iterator<Item = (i64, i64)> {
    (0..16).flat_map(|t| (0..16).map(move |x| (t, x)))
}

async fn create_red(server: &TestServer) {
    server
        .exec(
            "CREATE ARRAY red DIMS (t INT64 [0..15], x INT64 [0..15]) \
             ATTRS (v INT16) TILE_EXTENTS (4, 4)",
        )
        .await
        .expect("CREATE ARRAY red");
    let all: Vec<(i64, i64)> = cells().collect();
    let (first, second) = all.split_at(all.len() / 2);
    for (batch, flush) in [(first, true), (second, false)] {
        let rows: Vec<String> = batch
            .iter()
            .map(|&(t, x)| format!("COORDS ({t}, {x}) VALUES ({})", value(t, x)))
            .collect();
        server
            .exec(&format!("INSERT INTO ARRAY red {}", rows.join(", ")))
            .await
            .expect("INSERT INTO ARRAY red");
        if flush {
            server
                .exec("SELECT ARRAY_FLUSH('red')")
                .await
                .expect("ARRAY_FLUSH");
        }
    }
}

/// `percentile_cont(p)` of `values`: linear interpolation at rank
/// `p * (n - 1)` of the sorted values.
fn percentile(values: &[i64], p: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let h = p * (sorted.len() - 1) as f64;
    let (lo, hi) = (h.floor() as usize, h.ceil() as usize);
    sorted[lo] as f64 + (h - h.floor()) * (sorted[hi] - sorted[lo]) as f64
}

/// The most frequent value, the smallest among ties.
fn mode(values: &[i64]) -> f64 {
    let mut counts = std::collections::BTreeMap::new();
    for v in values {
        *counts.entry(*v).or_insert(0u64) += 1;
    }
    let best = counts.values().copied().max().unwrap_or(0);
    counts
        .into_iter()
        .find(|(_, n)| *n == best)
        .map(|(v, _)| v as f64)
        .unwrap_or(f64::NAN)
}

/// Population and sample variance.
fn variances(values: &[i64]) -> (f64, f64) {
    let n = values.len() as f64;
    let mean = values.iter().sum::<i64>() as f64 / n;
    let m2: f64 = values.iter().map(|v| (*v as f64 - mean).powi(2)).sum();
    (m2 / n, m2 / (n - 1.0))
}

/// The scalar `result` of `ARRAY_AGG('red', 'v', <rest>)`.
async fn scalar(server: &TestServer, rest: &str) -> f64 {
    let sql = format!("SELECT * FROM ARRAY_AGG('red', 'v', {rest})");
    let rows = server
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
    rows[0]["result"]
        .parse()
        .unwrap_or_else(|_| panic!("{sql}: {rows:?}"))
}

/// `group -> result` of `ARRAY_AGG('red', 'v', <rest>, 't')`.
async fn grouped(server: &TestServer, reducer: &str) -> std::collections::BTreeMap<i64, f64> {
    let sql = format!("SELECT * FROM ARRAY_AGG('red', 'v', {reducer}, 't')");
    server
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| {
            let g: i64 = row["group"].parse().expect("group");
            let r: f64 = row["result"].parse().expect("result");
            (g, r)
        })
        .collect()
}

fn close(got: f64, want: f64, what: &str) {
    assert!((got - want).abs() < 1e-9, "{what}: {got} != {want}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_reducer_is_exact_across_cores() {
    let server = TestServer::start_multicores(4).await;
    create_red(&server).await;
    let all: Vec<i64> = cells().map(|(t, x)| value(t, x)).collect();

    close(
        scalar(&server, "'median'").await,
        percentile(&all, 0.5),
        "median",
    );
    for p in [0.0, 0.1, 0.25, 0.9, 1.0] {
        close(
            scalar(&server, &format!("'percentile', p => {p}")).await,
            percentile(&all, p),
            &format!("percentile {p}"),
        );
    }
    close(scalar(&server, "'mode'").await, mode(&all), "mode");
    let (pop, samp) = variances(&all);
    close(scalar(&server, "'var_pop'").await, pop, "var_pop");
    close(scalar(&server, "'var_samp'").await, samp, "var_samp");
    close(scalar(&server, "'variance'").await, samp, "variance");
    close(
        scalar(&server, "'stddev_pop'").await,
        pop.sqrt(),
        "stddev_pop",
    );
    close(scalar(&server, "'stddev'").await, samp.sqrt(), "stddev");
}

/// A reducer's expected value over one group's values.
type GroupCheck = fn(&[i64]) -> f64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_reducer_groups_by_a_dim() {
    let server = TestServer::start_multicores(4).await;
    create_red(&server).await;
    let of_group = |t: i64| -> Vec<i64> { (0..16).map(|x| value(t, x)).collect() };

    let checks: [(&str, GroupCheck); 4] = [
        ("'median'", |v| percentile(v, 0.5)),
        ("'mode'", mode),
        ("'var_pop'", |v| variances(v).0),
        ("'stddev_samp'", |v| variances(v).1.sqrt()),
    ];
    for (reducer, want) in checks {
        let got = grouped(&server, reducer).await;
        assert_eq!(got.len(), 16, "{reducer}: {got:?}");
        for (t, r) in got {
            close(r, want(&of_group(t)), &format!("{reducer} group {t}"));
        }
    }
    let p90 = grouped(&server, "'percentile', p => 0.9").await;
    for (t, r) in p90 {
        close(r, percentile(&of_group(t), 0.9), &format!("p90 group {t}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn histogram_answers_one_row_per_bin() {
    let server = TestServer::start_multicores(4).await;
    create_red(&server).await;
    let all: Vec<i64> = cells().map(|(t, x)| value(t, x)).collect();

    // Values are 0..=10. Bins of width 2.5 over [0, 10]; 10 falls in the last.
    let sql = "SELECT * FROM ARRAY_AGG('red', 'v', 'histogram', bins => 4, lo => 0, hi => 10)";
    let rows = server
        .query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 4, "{rows:?}");
    for (i, row) in rows.iter().enumerate() {
        let lo: f64 = row["bin_lo"].parse().expect("bin_lo");
        let hi: f64 = row["bin_hi"].parse().expect("bin_hi");
        let count: u64 = row["count"].parse().expect("count");
        close(lo, 2.5 * i as f64, "bin_lo");
        close(hi, 2.5 * (i + 1) as f64, "bin_hi");
        let want = all
            .iter()
            .filter(|&&v| {
                let v = v as f64;
                v >= lo && (v < hi || (i == 3 && v <= hi))
            })
            .count() as u64;
        assert_eq!(count, want, "bin {i}");
    }

    // Grouped: one row per bin per group, zero-count bins included.
    let sql =
        "SELECT * FROM ARRAY_AGG('red', 'v', 'histogram', 't', bins => 2, lo => 100, hi => 200)";
    let rows = server
        .query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 32, "16 groups of 2 bins");
    assert!(
        rows.iter().all(|r| r["count"] == "0"),
        "no value is in range"
    );

    // A malformed spec is refused.
    for bad in [
        "SELECT * FROM ARRAY_AGG('red', 'v', 'histogram', bins => 0, lo => 0, hi => 1)",
        "SELECT * FROM ARRAY_AGG('red', 'v', 'histogram', bins => 4, lo => 1, hi => 1)",
        "SELECT * FROM ARRAY_AGG('red', 'v', 'histogram', bins => 4)",
        "SELECT * FROM ARRAY_AGG('red', 'v', 'percentile', p => 2)",
        "SELECT * FROM ARRAY_AGG('red', 'v', 'median', p => 0.5)",
    ] {
        assert!(server.exec(bad).await.is_err(), "{bad}");
    }
}
