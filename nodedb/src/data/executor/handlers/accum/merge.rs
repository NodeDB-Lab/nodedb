// SPDX-License-Identifier: BUSL-1.1

//! `merge_from` implementations for `AggAccum` and `GroupState`.

use super::state::{ARRAY_AGG_CAP, AggAccum, GroupState};

impl AggAccum {
    /// Merge a partial accumulator `other` into `self` (used by tests).
    #[cfg(test)]
    pub(crate) fn merge_from(&mut self, other: AggAccum) {
        merge_accum(self, other);
    }
}

/// Merge the partial accumulator `other` (from a spilled run) into `dst`.
///
/// Both `dst` and `other` must be the same variant — they always come from
/// the same aggregate-spec position in the same query.  A variant mismatch
/// is an internal programming error and will panic via `unreachable!`.
pub(super) fn merge_accum(dst: &mut AggAccum, other: AggAccum) {
    match (dst, other) {
        (AggAccum::Count { n: a }, AggAccum::Count { n: b }) => {
            *a += b;
        }
        (AggAccum::SumAvg { sum: a }, AggAccum::SumAvg { sum: b }) => {
            // Integer parts add exactly; float parts add compensated.
            a.merge(&b);
        }
        (AggAccum::SumAvgDistinct { seen: a }, AggAccum::SumAvgDistinct { seen: b }) => {
            // Union the deduped value maps; the first-seen number wins (all
            // instances of the same key carry the same value, so the choice
            // is immaterial). The sum is re-derived at finalize.
            for (key, value) in b {
                a.entry(key).or_insert(value);
            }
        }
        (AggAccum::Min { best: a }, AggAccum::Min { best: b }) => merge_extremum(a, b, false),
        (AggAccum::Max { best: a }, AggAccum::Max { best: b }) => merge_extremum(a, b, true),
        (AggAccum::CountDistinct { seen: a }, AggAccum::CountDistinct { seen: b }) => {
            a.extend(b);
        }
        (
            AggAccum::Welford {
                n: na,
                mean: ma,
                m2: m2a,
            },
            AggAccum::Welford {
                n: nb,
                mean: mb,
                m2: m2b,
            },
        ) => {
            // Parallel Welford merge formula.
            let n_new = *na + nb;
            if n_new == 0 {
                return;
            }
            let delta = mb - *ma;
            let mean_new = *ma + delta * (nb as f64 / n_new as f64);
            let m2_new = *m2a + m2b + delta * delta * (*na as f64) * (nb as f64) / n_new as f64;
            *na = n_new;
            *ma = mean_new;
            *m2a = m2_new;
        }
        (AggAccum::Hll { hll: a }, AggAccum::Hll { hll: b }) => {
            a.merge(&b);
        }
        (AggAccum::TDigest { digest: a }, AggAccum::TDigest { digest: b }) => {
            a.merge(&b);
        }
        (AggAccum::TopK { ss: a, .. }, AggAccum::TopK { ss: b, .. }) => {
            a.merge(&b);
        }
        (AggAccum::ArrayAgg { values: a }, AggAccum::ArrayAgg { values: b }) => {
            let remaining = ARRAY_AGG_CAP.saturating_sub(a.len());
            a.extend(b.into_iter().take(remaining));
        }
        (
            AggAccum::ArrayAggDistinct {
                seen: sa,
                values: va,
            },
            AggAccum::ArrayAggDistinct {
                seen: sb,
                values: vb,
            },
        ) => {
            for (bytes_key, value) in sb.into_iter().zip(vb) {
                if va.len() >= ARRAY_AGG_CAP {
                    break;
                }
                if sa.insert(bytes_key) {
                    va.push(value);
                }
            }
        }
        (
            AggAccum::PercentileCont { values: a, .. },
            AggAccum::PercentileCont { values: b, .. },
        ) => {
            let remaining = ARRAY_AGG_CAP.saturating_sub(a.len());
            a.extend(b.into_iter().take(remaining));
        }
        (AggAccum::StringAgg { parts: a }, AggAccum::StringAgg { parts: b }) => {
            let remaining = ARRAY_AGG_CAP.saturating_sub(a.len());
            a.extend(b.into_iter().take(remaining));
        }
        _ => {
            // Invariant: same query → same aggregate-spec position → same variant.
            unreachable!(
                "AggAccum::merge_from: variant mismatch — \
                 both operands must be the same variant (same aggregate spec, same query)"
            );
        }
    }
}

/// Merge a partial MIN (`want_max` false) or MAX (`want_max` true) extreme
/// into `dst`. Exact comparison; a NaN extreme yields to any candidate, and
/// a NaN candidate never replaces a number.
fn merge_extremum(
    dst: &mut Option<nodedb_types::Value>,
    other: Option<nodedb_types::Value>,
    want_max: bool,
) {
    if let Some(candidate) = other
        && nodedb_query::window::extremum::value_replaces(&candidate, dst.as_ref(), want_max)
    {
        *dst = Some(candidate);
    }
}

/// Merge all accumulators from `other` into `dst` element-wise.
pub(super) fn merge_group_state(dst: &mut GroupState, other: GroupState) {
    assert_eq!(
        dst.accums.len(),
        other.accums.len(),
        "GroupState::merge_from: accum count mismatch — \
         both GroupState values must come from the same aggregate spec list"
    );
    for (a, b) in dst.accums.iter_mut().zip(other.accums) {
        merge_accum(a, b);
    }
}

/// Round-trip tests: feed → finalize, and feed → split → merge → finalize.
#[cfg(test)]
mod tests {
    use super::AggAccum;
    use nodedb_physical::physical_plan::AggregateSpec;
    use nodedb_types::Value;

    fn make_spec(func: &str, field: &str) -> AggregateSpec {
        AggregateSpec {
            function: func.to_string(),
            field: field.to_string(),
            alias: format!("{func}({field})"),
            user_alias: None,
            expr: None,
        }
    }

    /// Build a minimal bare-msgpack doc with one integer field.
    ///
    /// Uses `value_to_msgpack` (not `zerompk::to_msgpack_vec`) so the output
    /// is a standard msgpack map that `extract_field` / `map_header` can scan.
    fn make_doc_i64(field: &str, value: i64) -> Vec<u8> {
        let mut map = std::collections::HashMap::new();
        map.insert(field.to_string(), Value::Integer(value));
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode doc")
    }

    fn make_doc_f64(field: &str, value: f64) -> Vec<u8> {
        let mut map = std::collections::HashMap::new();
        map.insert(field.to_string(), Value::Float(value));
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode doc")
    }

    fn make_doc_str(field: &str, value: &str) -> Vec<u8> {
        let mut map = std::collections::HashMap::new();
        map.insert(field.to_string(), Value::String(value.to_string()));
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode doc")
    }

    #[test]
    fn merge_from_count() {
        let spec = make_spec("count", "*");
        let docs_a: Vec<Vec<u8>> = (0..5).map(|_| make_doc_i64("x", 1)).collect();
        let docs_b: Vec<Vec<u8>> = (0..7).map(|_| make_doc_i64("x", 2)).collect();

        let mut combined = AggAccum::new(&spec);
        for d in &docs_a {
            combined.feed(&spec, d).unwrap();
        }
        for d in &docs_b {
            combined.feed(&spec, d).unwrap();
        }

        let mut a = AggAccum::new(&spec);
        for d in &docs_a {
            a.feed(&spec, d).unwrap();
        }
        let mut b = AggAccum::new(&spec);
        for d in &docs_b {
            b.feed(&spec, d).unwrap();
        }
        a.merge_from(b);

        assert_eq!(
            combined.finalize(&spec),
            a.finalize(&spec),
            "merge_from count"
        );
    }

    #[test]
    fn merge_from_sum_avg() {
        let sum_spec = make_spec("sum", "v");
        let avg_spec = make_spec("avg", "v");

        let vals_a: Vec<f64> = [1.0, 2.0, 3.0].into();
        let vals_b: Vec<f64> = [4.0, 5.0, 6.0].into();

        let docs_a: Vec<Vec<u8>> = vals_a.iter().map(|&v| make_doc_f64("v", v)).collect();
        let docs_b: Vec<Vec<u8>> = vals_b.iter().map(|&v| make_doc_f64("v", v)).collect();

        // Combined baseline.
        let mut combined_sum = AggAccum::new(&sum_spec);
        let mut combined_avg = AggAccum::new(&avg_spec);
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined_sum.feed(&sum_spec, d).unwrap();
            combined_avg.feed(&avg_spec, d).unwrap();
        }

        // Merge path.
        let mut a_sum = AggAccum::new(&sum_spec);
        let mut a_avg = AggAccum::new(&avg_spec);
        for d in &docs_a {
            a_sum.feed(&sum_spec, d).unwrap();
            a_avg.feed(&avg_spec, d).unwrap();
        }
        let mut b_sum = AggAccum::new(&sum_spec);
        let mut b_avg = AggAccum::new(&avg_spec);
        for d in &docs_b {
            b_sum.feed(&sum_spec, d).unwrap();
            b_avg.feed(&avg_spec, d).unwrap();
        }
        a_sum.merge_from(b_sum);
        a_avg.merge_from(b_avg);

        let Value::Float(cs) = combined_sum.finalize(&sum_spec).unwrap() else {
            panic!("expected float");
        };
        let Value::Float(ms) = a_sum.finalize(&sum_spec).unwrap() else {
            panic!("expected float");
        };
        assert!((cs - ms).abs() < 1e-9, "sum mismatch: {cs} vs {ms}");

        let Value::Float(ca) = combined_avg.finalize(&avg_spec).unwrap() else {
            panic!("expected float");
        };
        let Value::Float(ma) = a_avg.finalize(&avg_spec).unwrap() else {
            panic!("expected float");
        };
        assert!((ca - ma).abs() < 1e-9, "avg mismatch: {ca} vs {ma}");
    }

    #[test]
    fn merge_from_sum_avg_distinct() {
        let sum_spec = make_spec("sum_distinct", "v");
        let avg_spec = make_spec("avg_distinct", "v");

        // Overlapping value sets: distinct union is {1,2,3,4,5} → sum 15, avg 3.
        let docs_a: Vec<Vec<u8>> = [1i64, 2, 2, 3]
            .iter()
            .map(|&v| make_doc_i64("v", v))
            .collect();
        let docs_b: Vec<Vec<u8>> = [3i64, 4, 4, 5]
            .iter()
            .map(|&v| make_doc_i64("v", v))
            .collect();

        // Combined baseline.
        let mut combined_sum = AggAccum::new(&sum_spec);
        let mut combined_avg = AggAccum::new(&avg_spec);
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined_sum.feed(&sum_spec, d).unwrap();
            combined_avg.feed(&avg_spec, d).unwrap();
        }

        // Spilled-run merge path.
        let mut a_sum = AggAccum::new(&sum_spec);
        let mut a_avg = AggAccum::new(&avg_spec);
        for d in &docs_a {
            a_sum.feed(&sum_spec, d).unwrap();
            a_avg.feed(&avg_spec, d).unwrap();
        }
        let mut b_sum = AggAccum::new(&sum_spec);
        let mut b_avg = AggAccum::new(&avg_spec);
        for d in &docs_b {
            b_sum.feed(&sum_spec, d).unwrap();
            b_avg.feed(&avg_spec, d).unwrap();
        }
        a_sum.merge_from(b_sum);
        a_avg.merge_from(b_avg);

        assert_eq!(combined_sum.finalize(&sum_spec), Ok(Value::Integer(15)));
        assert_eq!(combined_avg.finalize(&avg_spec), Ok(Value::Float(3.0)));
        assert_eq!(
            a_sum.finalize(&sum_spec),
            Ok(Value::Integer(15)),
            "sum_distinct merge"
        );
        assert_eq!(
            a_avg.finalize(&avg_spec),
            Ok(Value::Float(3.0)),
            "avg_distinct merge"
        );
    }

    /// Feed `docs_a` and `docs_b` into one accumulator, and separately into
    /// two partials merged after; return both finalized results.
    fn single_and_merged(spec: &AggregateSpec, docs_a: &[Vec<u8>], docs_b: &[Vec<u8>]) -> [Value; 2] {
        let mut combined = AggAccum::new(spec);
        for d in docs_a.iter().chain(docs_b) {
            combined.feed(spec, d).unwrap();
        }
        let mut a = AggAccum::new(spec);
        for d in docs_a {
            a.feed(spec, d).unwrap();
        }
        let mut b = AggAccum::new(spec);
        for d in docs_b {
            b.feed(spec, d).unwrap();
        }
        a.merge_from(b);
        [combined.finalize(spec).unwrap(), a.finalize(spec).unwrap()]
    }

    const ABOVE: i64 = 9_007_199_254_740_993;
    const AT: i64 = 9_007_199_254_740_992;

    /// A one-field doc whose value is a raw msgpack `uint64`.
    fn make_doc_u64(field: &str, value: u64) -> Vec<u8> {
        let mut doc = vec![0x81, 0xa0 | field.len() as u8];
        doc.extend_from_slice(field.as_bytes());
        doc.push(0xcf);
        doc.extend_from_slice(&value.to_be_bytes());
        doc
    }

    #[test]
    fn sum_min_max_keep_integers_above_2_pow_53_exact_through_merge() {
        let a = [make_doc_i64("v", ABOVE)];
        let b = [make_doc_i64("v", AT)];
        for got in single_and_merged(&make_spec("sum", "v"), &a, &b) {
            assert_eq!(got, Value::Integer(ABOVE + AT));
        }
        for got in single_and_merged(&make_spec("min", "v"), &a, &b) {
            assert_eq!(got, Value::Integer(AT));
        }
        for got in single_and_merged(&make_spec("max", "v"), &a, &b) {
            assert_eq!(got, Value::Integer(ABOVE));
        }
        for got in single_and_merged(&make_spec("avg", "v"), &a, &b) {
            assert_eq!(got, Value::Float(AT as f64));
        }
    }

    #[test]
    fn nanosecond_timestamps_sum_and_extremes_exactly() {
        let a = [make_doc_i64("v", 1_700_000_000_000_000_002)];
        let b = [
            make_doc_i64("v", 1_700_000_000_000_000_001),
            make_doc_i64("v", 1_700_000_000_000_000_003),
        ];
        for got in single_and_merged(&make_spec("sum", "v"), &a, &b) {
            assert_eq!(got, Value::Integer(5_100_000_000_000_000_006));
        }
        for got in single_and_merged(&make_spec("min", "v"), &a, &b) {
            assert_eq!(got, Value::Integer(1_700_000_000_000_000_001));
        }
        for got in single_and_merged(&make_spec("max", "v"), &a, &b) {
            assert_eq!(got, Value::Integer(1_700_000_000_000_000_003));
        }
    }

    #[test]
    fn sum_past_i64_is_decimal_through_merge() {
        let a = [make_doc_i64("v", i64::MAX)];
        let b = [make_doc_i64("v", i64::MAX), make_doc_u64("v", u64::MAX)];
        let want = rust_decimal::Decimal::from_i128_with_scale(
            2 * i128::from(i64::MAX) + i128::from(u64::MAX),
            0,
        );
        for got in single_and_merged(&make_spec("sum", "v"), &a, &b) {
            assert_eq!(got, Value::Decimal(want));
        }
        for got in single_and_merged(&make_spec("max", "v"), &a, &b) {
            assert_eq!(got, Value::Decimal(rust_decimal::Decimal::from(u64::MAX)));
        }
    }

    #[test]
    fn sum_mixed_int_float_is_float() {
        let a = [make_doc_i64("v", 2)];
        let b = [make_doc_f64("v", 0.5)];
        for got in single_and_merged(&make_spec("sum", "v"), &a, &b) {
            assert_eq!(got, Value::Float(2.5));
        }
        for got in single_and_merged(&make_spec("max", "v"), &a, &b) {
            assert_eq!(got, Value::Integer(2));
        }
    }

    #[test]
    fn nan_extreme_yields_to_numbers_in_feed_and_merge() {
        let a = [make_doc_f64("v", f64::NAN), make_doc_i64("v", 5)];
        let b = [make_doc_i64("v", 3)];
        for got in single_and_merged(&make_spec("min", "v"), &a, &b) {
            assert_eq!(got, Value::Integer(3));
        }
        for got in single_and_merged(&make_spec("max", "v"), &a, &b) {
            assert_eq!(got, Value::Integer(5));
        }
        // A NaN-only partial merged into a numeric one never wins.
        let nan_only = [make_doc_f64("v", f64::NAN)];
        let nums = [make_doc_i64("v", 7)];
        for got in single_and_merged(&make_spec("max", "v"), &nan_only, &nums) {
            assert_eq!(got, Value::Integer(7));
        }
        for got in single_and_merged(&make_spec("min", "v"), &nums, &nan_only) {
            assert_eq!(got, Value::Integer(7));
        }
    }

    #[test]
    fn merge_from_min_max() {
        let min_spec = make_spec("min", "v");
        let max_spec = make_spec("max", "v");

        let docs_a: Vec<Vec<u8>> = [3i64, 1, 7].iter().map(|&v| make_doc_i64("v", v)).collect();
        let docs_b: Vec<Vec<u8>> = [2i64, 9, 4].iter().map(|&v| make_doc_i64("v", v)).collect();

        let mut combined_min = AggAccum::new(&min_spec);
        let mut combined_max = AggAccum::new(&max_spec);
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined_min.feed(&min_spec, d).unwrap();
            combined_max.feed(&max_spec, d).unwrap();
        }

        let mut a_min = AggAccum::new(&min_spec);
        let mut a_max = AggAccum::new(&max_spec);
        for d in &docs_a {
            a_min.feed(&min_spec, d).unwrap();
            a_max.feed(&max_spec, d).unwrap();
        }
        let mut b_min = AggAccum::new(&min_spec);
        let mut b_max = AggAccum::new(&max_spec);
        for d in &docs_b {
            b_min.feed(&min_spec, d).unwrap();
            b_max.feed(&max_spec, d).unwrap();
        }
        a_min.merge_from(b_min);
        a_max.merge_from(b_max);

        assert_eq!(
            combined_min.finalize(&min_spec),
            a_min.finalize(&min_spec),
            "min"
        );
        assert_eq!(
            combined_max.finalize(&max_spec),
            a_max.finalize(&max_spec),
            "max"
        );
    }

    #[test]
    fn merge_from_welford() {
        let spec = make_spec("variance", "v");

        let vals_a: Vec<f64> = (1..=10).map(|i| i as f64).collect();
        let vals_b: Vec<f64> = (11..=20).map(|i| i as f64).collect();
        let docs_a: Vec<Vec<u8>> = vals_a.iter().map(|&v| make_doc_f64("v", v)).collect();
        let docs_b: Vec<Vec<u8>> = vals_b.iter().map(|&v| make_doc_f64("v", v)).collect();

        let mut combined = AggAccum::new(&spec);
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined.feed(&spec, d).unwrap();
        }

        let mut a = AggAccum::new(&spec);
        for d in &docs_a {
            a.feed(&spec, d).unwrap();
        }
        let mut b = AggAccum::new(&spec);
        for d in &docs_b {
            b.feed(&spec, d).unwrap();
        }
        a.merge_from(b);

        let Value::Float(cv) = combined.finalize(&spec) else {
            panic!("expected float");
        };
        let Value::Float(mv) = a.finalize(&spec) else {
            panic!("expected float");
        };
        let rel = (cv - mv).abs() / cv.abs().max(1e-12);
        assert!(
            rel < 1e-9,
            "Welford merge variance: {cv} vs {mv} (rel={rel})"
        );
    }

    #[test]
    fn merge_from_count_distinct() {
        let spec = make_spec("count_distinct", "v");

        let docs_a: Vec<Vec<u8>> = ["a", "b", "c"]
            .iter()
            .map(|&s| make_doc_str("v", s))
            .collect();
        let docs_b: Vec<Vec<u8>> = ["c", "d", "e"]
            .iter()
            .map(|&s| make_doc_str("v", s))
            .collect();

        let mut combined = AggAccum::new(&spec);
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined.feed(&spec, d).unwrap();
        }
        let mut a = AggAccum::new(&spec);
        for d in &docs_a {
            a.feed(&spec, d).unwrap();
        }
        let mut b = AggAccum::new(&spec);
        for d in &docs_b {
            b.feed(&spec, d).unwrap();
        }
        a.merge_from(b);

        assert_eq!(
            combined.finalize(&spec),
            a.finalize(&spec),
            "count_distinct"
        );
    }

    #[test]
    fn merge_from_hll() {
        let spec = make_spec("approx_count_distinct", "v");

        let docs_a: Vec<Vec<u8>> = (0..500u64).map(|i| make_doc_i64("v", i as i64)).collect();
        let docs_b: Vec<Vec<u8>> = (500..1000u64)
            .map(|i| make_doc_i64("v", i as i64))
            .collect();

        let mut combined = AggAccum::new(&spec);
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined.feed(&spec, d).unwrap();
        }

        let mut a = AggAccum::new(&spec);
        for d in &docs_a {
            a.feed(&spec, d).unwrap();
        }
        let mut b = AggAccum::new(&spec);
        for d in &docs_b {
            b.feed(&spec, d).unwrap();
        }
        a.merge_from(b);

        let Value::Integer(cv) = combined.finalize(&spec) else {
            panic!("expected int");
        };
        let Value::Integer(mv) = a.finalize(&spec) else {
            panic!("expected int");
        };
        // HLL is approximate; require within 5% of expected 1000.
        let diff = (cv - mv).abs() as f64 / 1000.0;
        assert!(diff < 0.05, "HLL merge: combined={cv}, merged={mv}");
    }

    #[test]
    fn merge_from_tdigest() {
        let spec = make_spec("approx_percentile", "0.5:v");

        let docs_a: Vec<Vec<u8>> = (0..100).map(|i| make_doc_f64("v", i as f64)).collect();
        let docs_b: Vec<Vec<u8>> = (100..200).map(|i| make_doc_f64("v", i as f64)).collect();

        let mut a = AggAccum::new(&spec);
        for d in &docs_a {
            a.feed(&spec, d).unwrap();
        }
        let mut b = AggAccum::new(&spec);
        for d in &docs_b {
            b.feed(&spec, d).unwrap();
        }
        a.merge_from(b);

        // p50 of 0..200 should be close to 100.
        let Value::Float(p50) = a.finalize(&spec) else {
            panic!("expected float");
        };
        assert!((50.0..150.0).contains(&p50), "TDigest merge p50={p50}");
    }

    #[test]
    fn merge_from_topk() {
        let spec = make_spec("approx_topk", "3:v");
        // TopK is heuristic; just verify the top item count is right.
        let docs_a: Vec<Vec<u8>> = (0..50).map(|i| make_doc_i64("v", i % 3)).collect();
        let docs_b: Vec<Vec<u8>> = (0..50).map(|i| make_doc_i64("v", i % 3)).collect();

        let mut a = AggAccum::new(&spec);
        for d in &docs_a {
            a.feed(&spec, d).unwrap();
        }
        let mut b = AggAccum::new(&spec);
        for d in &docs_b {
            b.feed(&spec, d).unwrap();
        }
        a.merge_from(b);

        let Value::Array(arr) = a.finalize(&spec) else {
            panic!("expected array");
        };
        assert_eq!(arr.len(), 3, "TopK should return k=3 items");
    }

    #[test]
    fn merge_from_array_agg() {
        let spec = make_spec("array_agg", "v");

        let docs_a: Vec<Vec<u8>> = [1i64, 2, 3].iter().map(|&v| make_doc_i64("v", v)).collect();
        let docs_b: Vec<Vec<u8>> = [4i64, 5, 6].iter().map(|&v| make_doc_i64("v", v)).collect();

        let mut combined = AggAccum::new(&spec);
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined.feed(&spec, d).unwrap();
        }

        let mut a = AggAccum::new(&spec);
        for d in &docs_a {
            a.feed(&spec, d).unwrap();
        }
        let mut b = AggAccum::new(&spec);
        for d in &docs_b {
            b.feed(&spec, d).unwrap();
        }
        a.merge_from(b);

        assert_eq!(combined.finalize(&spec), a.finalize(&spec), "array_agg");
    }

    #[test]
    fn merge_from_string_agg() {
        let spec = make_spec("string_agg", "v");

        let docs_a: Vec<Vec<u8>> = ["hello", "world"]
            .iter()
            .map(|&s| make_doc_str("v", s))
            .collect();
        let docs_b: Vec<Vec<u8>> = ["foo", "bar"]
            .iter()
            .map(|&s| make_doc_str("v", s))
            .collect();

        let mut combined = AggAccum::new(&spec);
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined.feed(&spec, d).unwrap();
        }

        let mut a = AggAccum::new(&spec);
        for d in &docs_a {
            a.feed(&spec, d).unwrap();
        }
        let mut b = AggAccum::new(&spec);
        for d in &docs_b {
            b.feed(&spec, d).unwrap();
        }
        a.merge_from(b);

        assert_eq!(combined.finalize(&spec), a.finalize(&spec), "string_agg");
    }

    #[test]
    fn merge_from_percentile_cont() {
        let spec = make_spec("percentile_cont", "0.5:v");

        let docs_a: Vec<Vec<u8>> = [1.0f64, 3.0, 5.0]
            .iter()
            .map(|&v| make_doc_f64("v", v))
            .collect();
        let docs_b: Vec<Vec<u8>> = [2.0f64, 4.0, 6.0]
            .iter()
            .map(|&v| make_doc_f64("v", v))
            .collect();

        let mut combined = AggAccum::new(&spec);
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined.feed(&spec, d).unwrap();
        }

        let mut a = AggAccum::new(&spec);
        for d in &docs_a {
            a.feed(&spec, d).unwrap();
        }
        let mut b = AggAccum::new(&spec);
        for d in &docs_b {
            b.feed(&spec, d).unwrap();
        }
        a.merge_from(b);

        assert_eq!(
            combined.finalize(&spec),
            a.finalize(&spec),
            "percentile_cont"
        );
    }
}
