// SPDX-License-Identifier: BUSL-1.1

//! `merge_from` implementations for `AggAccum` and `GroupState`.

use super::state::{AggAccum, GroupState};

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
        (AggAccum::Welford { moments: a }, AggAccum::Welford { moments: b }) => {
            *a = a.merge(b);
        }
        (AggAccum::Hll { hll: a }, AggAccum::Hll { hll: b }) => {
            a.merge(&b);
        }
        (AggAccum::TDigest { digest: a, .. }, AggAccum::TDigest { digest: b, .. }) => {
            a.merge(&b);
        }
        (AggAccum::TopK { ss: a, .. }, AggAccum::TopK { ss: b, .. }) => {
            a.merge(&b);
        }
        (AggAccum::ArrayAgg { values: a }, AggAccum::ArrayAgg { values: b }) => {
            a.extend(b);
        }
        (
            AggAccum::ArrayAggDistinct {
                seen: sa,
                values: va,
            },
            AggAccum::ArrayAggDistinct {
                seen: _,
                values: vb,
            },
        ) => {
            // Each value travels with its own key, so a merge dedups on the
            // same bytes the feed did.
            for (bytes_key, value) in vb {
                if !sa.contains(&bytes_key) {
                    sa.insert(bytes_key.clone());
                    va.push((bytes_key, value));
                }
            }
        }
        (
            AggAccum::PercentileCont { values: a, .. },
            AggAccum::PercentileCont { values: b, .. },
        ) => {
            a.extend(b);
        }
        (AggAccum::StringAgg { parts: a }, AggAccum::StringAgg { parts: b }) => {
            a.extend(b);
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
/// into `dst`. Exact comparison. NaN sorts above every number, as in
/// PostgreSQL: it wins MAX and loses MIN.
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

    /// A spec over `field`. A parameterized function gets a fixed parameter:
    /// the median, or the top 3.
    fn make_spec(func: &str, field: &str) -> AggregateSpec {
        use nodedb_physical::physical_plan::AggregateParam;
        let param = match func {
            "approx_topk" => Some(AggregateParam::TopK { k: 3 }),
            "approx_percentile" | "percentile_cont" => {
                Some(AggregateParam::Percentile { fraction: 0.5 })
            }
            _ => None,
        };
        AggregateSpec {
            function: func.to_string(),
            field: field.to_string(),
            alias: format!("{func}({field})"),
            user_alias: None,
            expr: None,
            param,
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

    /// Materializing aggregates keep every value: a group past 10,000 rows
    /// is never cut to a prefix.
    #[test]
    fn materializing_aggregates_keep_every_value_past_ten_thousand() {
        const ROWS: i64 = 10_500;
        for func in ["array_agg", "array_agg_distinct", "string_agg"] {
            let spec = make_spec(func, "x");
            let mut accum = AggAccum::new(&spec).unwrap();
            for i in 0..ROWS {
                let doc = if func == "string_agg" {
                    make_doc_str("x", &i.to_string())
                } else {
                    make_doc_i64("x", i)
                };
                accum
                    .feed(&spec, &doc, &nodedb_crs::CrsContext::builtin_only())
                    .expect("feed");
            }
            let count = match &accum {
                AggAccum::ArrayAgg { values } => values.len(),
                AggAccum::ArrayAggDistinct { values, .. } => values.len(),
                AggAccum::StringAgg { parts } => parts.len(),
                _ => panic!("{func} built an unexpected accumulator"),
            };
            assert_eq!(count, ROWS as usize, "{func} dropped values");
        }
    }

    /// A distinct array merged from two runs dedups by each value's own key,
    /// so a value both runs saw appears once and no value is paired with
    /// another value's key.
    #[test]
    fn merge_from_array_agg_distinct_dedups_by_value() {
        let spec = make_spec("array_agg_distinct", "x");
        let mut a = AggAccum::new(&spec).unwrap();
        for v in [1, 2, 3] {
            a.feed(
                &spec,
                &make_doc_i64("x", v),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .expect("feed a");
        }
        let mut b = AggAccum::new(&spec).unwrap();
        for v in [3, 4, 5, 6, 7, 8] {
            b.feed(
                &spec,
                &make_doc_i64("x", v),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .expect("feed b");
        }
        a.merge_from(b);
        let Value::Array(mut values) = a.finalize(&spec).expect("finalize") else {
            panic!("array_agg_distinct finalizes to an array");
        };
        values.sort_by_key(|v| match v {
            Value::Integer(i) => *i,
            other => panic!("unexpected value {other:?}"),
        });
        assert_eq!(values, (1..=8).map(Value::Integer).collect::<Vec<_>>());
    }

    #[test]
    fn merge_from_count() {
        let spec = make_spec("count", "*");
        let docs_a: Vec<Vec<u8>> = (0..5).map(|_| make_doc_i64("x", 1)).collect();
        let docs_b: Vec<Vec<u8>> = (0..7).map(|_| make_doc_i64("x", 2)).collect();

        let mut combined = AggAccum::new(&spec).unwrap();
        for d in &docs_a {
            combined
                .feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        for d in &docs_b {
            combined
                .feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }

        let mut a = AggAccum::new(&spec).unwrap();
        for d in &docs_a {
            a.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b = AggAccum::new(&spec).unwrap();
        for d in &docs_b {
            b.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
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
        let mut combined_sum = AggAccum::new(&sum_spec).unwrap();
        let mut combined_avg = AggAccum::new(&avg_spec).unwrap();
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined_sum
                .feed(&sum_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
            combined_avg
                .feed(&avg_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }

        // Merge path.
        let mut a_sum = AggAccum::new(&sum_spec).unwrap();
        let mut a_avg = AggAccum::new(&avg_spec).unwrap();
        for d in &docs_a {
            a_sum
                .feed(&sum_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
            a_avg
                .feed(&avg_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b_sum = AggAccum::new(&sum_spec).unwrap();
        let mut b_avg = AggAccum::new(&avg_spec).unwrap();
        for d in &docs_b {
            b_sum
                .feed(&sum_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
            b_avg
                .feed(&avg_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
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
        let mut combined_sum = AggAccum::new(&sum_spec).unwrap();
        let mut combined_avg = AggAccum::new(&avg_spec).unwrap();
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined_sum
                .feed(&sum_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
            combined_avg
                .feed(&avg_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }

        // Spilled-run merge path.
        let mut a_sum = AggAccum::new(&sum_spec).unwrap();
        let mut a_avg = AggAccum::new(&avg_spec).unwrap();
        for d in &docs_a {
            a_sum
                .feed(&sum_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
            a_avg
                .feed(&avg_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b_sum = AggAccum::new(&sum_spec).unwrap();
        let mut b_avg = AggAccum::new(&avg_spec).unwrap();
        for d in &docs_b {
            b_sum
                .feed(&sum_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
            b_avg
                .feed(&avg_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
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
    fn single_and_merged(
        spec: &AggregateSpec,
        docs_a: &[Vec<u8>],
        docs_b: &[Vec<u8>],
    ) -> [Value; 2] {
        let mut combined = AggAccum::new(spec).unwrap();
        for d in docs_a.iter().chain(docs_b) {
            combined
                .feed(spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut a = AggAccum::new(spec).unwrap();
        for d in docs_a {
            a.feed(spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b = AggAccum::new(spec).unwrap();
        for d in docs_b {
            b.feed(spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
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

    /// NaN sorts above every number, as in PostgreSQL: MAX is NaN and MIN
    /// skips it, in a single pass and across a merge.
    #[test]
    fn nan_is_the_largest_extreme_in_feed_and_merge() {
        let is_nan = |v: &Value| matches!(v, Value::Float(f) if f.is_nan());
        let a = [make_doc_f64("v", f64::NAN), make_doc_i64("v", 5)];
        let b = [make_doc_i64("v", 3)];
        for got in single_and_merged(&make_spec("min", "v"), &a, &b) {
            assert_eq!(got, Value::Integer(3));
        }
        for got in single_and_merged(&make_spec("max", "v"), &a, &b) {
            assert!(is_nan(&got), "got {got:?}");
        }
        let nan_only = [make_doc_f64("v", f64::NAN)];
        let nums = [make_doc_i64("v", 7)];
        for got in single_and_merged(&make_spec("max", "v"), &nan_only, &nums) {
            assert!(is_nan(&got), "got {got:?}");
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

        let mut combined_min = AggAccum::new(&min_spec).unwrap();
        let mut combined_max = AggAccum::new(&max_spec).unwrap();
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined_min
                .feed(&min_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
            combined_max
                .feed(&max_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }

        let mut a_min = AggAccum::new(&min_spec).unwrap();
        let mut a_max = AggAccum::new(&max_spec).unwrap();
        for d in &docs_a {
            a_min
                .feed(&min_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
            a_max
                .feed(&max_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b_min = AggAccum::new(&min_spec).unwrap();
        let mut b_max = AggAccum::new(&max_spec).unwrap();
        for d in &docs_b {
            b_min
                .feed(&min_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
            b_max
                .feed(&max_spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
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

        let mut combined = AggAccum::new(&spec).unwrap();
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined
                .feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }

        let mut a = AggAccum::new(&spec).unwrap();
        for d in &docs_a {
            a.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b = AggAccum::new(&spec).unwrap();
        for d in &docs_b {
            b.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        a.merge_from(b);

        let Value::Float(cv) = combined.finalize(&spec).expect("finalize") else {
            panic!("expected float");
        };
        let Value::Float(mv) = a.finalize(&spec).expect("finalize") else {
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

        let mut combined = AggAccum::new(&spec).unwrap();
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined
                .feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut a = AggAccum::new(&spec).unwrap();
        for d in &docs_a {
            a.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b = AggAccum::new(&spec).unwrap();
        for d in &docs_b {
            b.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
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

        let mut combined = AggAccum::new(&spec).unwrap();
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined
                .feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }

        let mut a = AggAccum::new(&spec).unwrap();
        for d in &docs_a {
            a.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b = AggAccum::new(&spec).unwrap();
        for d in &docs_b {
            b.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        a.merge_from(b);

        let Value::Integer(cv) = combined.finalize(&spec).expect("finalize") else {
            panic!("expected int");
        };
        let Value::Integer(mv) = a.finalize(&spec).expect("finalize") else {
            panic!("expected int");
        };
        // HLL is approximate; require within 5% of expected 1000.
        let diff = (cv - mv).abs() as f64 / 1000.0;
        assert!(diff < 0.05, "HLL merge: combined={cv}, merged={mv}");
    }

    #[test]
    fn merge_from_tdigest() {
        let spec = make_spec("approx_percentile", "v");

        let docs_a: Vec<Vec<u8>> = (0..100).map(|i| make_doc_f64("v", i as f64)).collect();
        let docs_b: Vec<Vec<u8>> = (100..200).map(|i| make_doc_f64("v", i as f64)).collect();

        let mut a = AggAccum::new(&spec).unwrap();
        for d in &docs_a {
            a.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b = AggAccum::new(&spec).unwrap();
        for d in &docs_b {
            b.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        a.merge_from(b);

        // p50 of 0..200 should be close to 100.
        let Value::Float(p50) = a.finalize(&spec).expect("finalize") else {
            panic!("expected float");
        };
        assert!((50.0..150.0).contains(&p50), "TDigest merge p50={p50}");
    }

    #[test]
    fn merge_from_topk() {
        let spec = make_spec("approx_topk", "v");
        // TopK is heuristic; just verify the top item count is right.
        let docs_a: Vec<Vec<u8>> = (0..50).map(|i| make_doc_i64("v", i % 3)).collect();
        let docs_b: Vec<Vec<u8>> = (0..50).map(|i| make_doc_i64("v", i % 3)).collect();

        let mut a = AggAccum::new(&spec).unwrap();
        for d in &docs_a {
            a.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b = AggAccum::new(&spec).unwrap();
        for d in &docs_b {
            b.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        a.merge_from(b);

        let Value::Array(arr) = a.finalize(&spec).expect("finalize") else {
            panic!("expected array");
        };
        assert_eq!(arr.len(), 3, "TopK should return k=3 items");
    }

    #[test]
    fn merge_from_array_agg() {
        let spec = make_spec("array_agg", "v");

        let docs_a: Vec<Vec<u8>> = [1i64, 2, 3].iter().map(|&v| make_doc_i64("v", v)).collect();
        let docs_b: Vec<Vec<u8>> = [4i64, 5, 6].iter().map(|&v| make_doc_i64("v", v)).collect();

        let mut combined = AggAccum::new(&spec).unwrap();
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined
                .feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }

        let mut a = AggAccum::new(&spec).unwrap();
        for d in &docs_a {
            a.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b = AggAccum::new(&spec).unwrap();
        for d in &docs_b {
            b.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
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

        let mut combined = AggAccum::new(&spec).unwrap();
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined
                .feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }

        let mut a = AggAccum::new(&spec).unwrap();
        for d in &docs_a {
            a.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b = AggAccum::new(&spec).unwrap();
        for d in &docs_b {
            b.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        a.merge_from(b);

        assert_eq!(combined.finalize(&spec), a.finalize(&spec), "string_agg");
    }

    #[test]
    fn merge_from_percentile_cont() {
        let spec = make_spec("percentile_cont", "v");

        let docs_a: Vec<Vec<u8>> = [1.0f64, 3.0, 5.0]
            .iter()
            .map(|&v| make_doc_f64("v", v))
            .collect();
        let docs_b: Vec<Vec<u8>> = [2.0f64, 4.0, 6.0]
            .iter()
            .map(|&v| make_doc_f64("v", v))
            .collect();

        let mut combined = AggAccum::new(&spec).unwrap();
        for d in docs_a.iter().chain(docs_b.iter()) {
            combined
                .feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }

        let mut a = AggAccum::new(&spec).unwrap();
        for d in &docs_a {
            a.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        let mut b = AggAccum::new(&spec).unwrap();
        for d in &docs_b {
            b.feed(&spec, d, &nodedb_crs::CrsContext::builtin_only())
                .unwrap();
        }
        a.merge_from(b);

        assert_eq!(
            combined.finalize(&spec),
            a.finalize(&spec),
            "percentile_cont"
        );
    }
}
