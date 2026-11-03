// SPDX-License-Identifier: BUSL-1.1

//! An ordered read through a secondary index on a strict collection.
//!
//! The rows that pass the index and the residual sort on narrow rows, and
//! only the rows the sort keeps convert in full. The answers must match a
//! full sort: same order, same `OFFSET` cut, whole rows for the kept ones.

use crate::harness::TestServer;

const ROWS: usize = 40;

/// 40 rows in group `g` with ranks in a shuffled order, tags alternating,
/// and 10 rows in group `h` that the index never returns.
async fn seed(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION sn TYPE strict (id STRING PRIMARY KEY, grp STRING NOT NULL, \
             rank INT NOT NULL, tag STRING, embedding VECTOR(4))",
        )
        .await
        .unwrap();
    server.exec("CREATE INDEX ON sn FIELDS grp").await.unwrap();
    for i in 0..ROWS {
        let rank = (i * 7) % ROWS;
        let tag = if i % 2 == 0 { "even" } else { "odd" };
        server
            .exec(&format!(
                "INSERT INTO sn (id, grp, rank, tag, embedding) VALUES \
                 ('r{i}', 'g', {rank}, '{tag}', ARRAY[{i}.0, 1.0, 2.0, 3.0])"
            ))
            .await
            .unwrap();
    }
    for i in 0..10 {
        server
            .exec(&format!(
                "INSERT INTO sn (id, grp, rank, tag, embedding) VALUES \
                 ('h{i}', 'h', {}, 'even', ARRAY[0.0, 0.0, 0.0, 0.0])",
                1000 + i
            ))
            .await
            .unwrap();
    }
}

async fn ids(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

/// `r<i>` ids of group `g` ordered by rank, the answer a full sort gives.
fn expected(descending: bool, keep: impl Fn(usize) -> bool) -> Vec<String> {
    let mut rows: Vec<(usize, usize)> = (0..ROWS)
        .filter(|&i| keep(i))
        .map(|i| ((i * 7) % ROWS, i))
        .collect();
    rows.sort();
    if descending {
        rows.reverse();
    }
    rows.into_iter().map(|(_, i)| format!("r{i}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sorted_indexed_read_orders_and_cuts_like_a_full_sort() {
    let server = TestServer::start().await;
    seed(&server).await;

    let desc = expected(true, |_| true);
    assert_eq!(
        ids(
            &server,
            "SELECT id FROM sn WHERE grp = 'g' ORDER BY rank DESC LIMIT 5"
        )
        .await,
        desc[..5]
    );
    assert_eq!(
        ids(
            &server,
            "SELECT id FROM sn WHERE grp = 'g' ORDER BY rank DESC LIMIT 5 OFFSET 7"
        )
        .await,
        desc[7..12]
    );
    let asc = expected(false, |_| true);
    assert_eq!(
        ids(
            &server,
            "SELECT id FROM sn WHERE grp = 'g' ORDER BY rank LIMIT 3"
        )
        .await,
        asc[..3]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_residual_filter_applies_before_the_sort_cut() {
    let server = TestServer::start().await;
    seed(&server).await;

    let even = expected(true, |i| i % 2 == 0);
    assert_eq!(
        ids(
            &server,
            "SELECT id FROM sn WHERE grp = 'g' AND tag = 'even' ORDER BY rank DESC LIMIT 4"
        )
        .await,
        even[..4]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_computed_sort_key_reads_its_columns() {
    let server = TestServer::start().await;
    seed(&server).await;

    // `-rank` ascending is `rank` descending.
    let desc = expected(true, |_| true);
    assert_eq!(
        ids(
            &server,
            "SELECT id FROM sn WHERE grp = 'g' ORDER BY 0 - rank LIMIT 5"
        )
        .await,
        desc[..5]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_rows_a_sort_keeps_carry_every_column() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, grp, rank, tag, embedding FROM sn WHERE grp = 'g' \
             ORDER BY rank DESC LIMIT 2",
        )
        .await
        .unwrap();
    let top = expected(true, |_| true);
    assert_eq!(rows.len(), 2);
    for (row, want) in rows.iter().zip(&top) {
        let i: usize = want[1..].parse().unwrap();
        assert_eq!(&row[0], want);
        assert_eq!(row[1], "g");
        assert_eq!(row[2], ((i * 7) % ROWS).to_string());
        assert!(
            row[4].starts_with(&format!("{{{i},1,2,3")),
            "embedding of {want}: {}",
            row[4]
        );
    }
}
