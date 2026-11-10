// SPDX-License-Identifier: BUSL-1.1

//! Hash join probe: emit join rows by probing a [`HashIndex`].

pub(super) use super::hash_index::HashIndex;
use super::{binary_row_matches_filters, merge_join_docs_binary};

/// The physical join type of a null-aware anti join: SQL `NOT IN
/// (subquery)`. A NULL in the non-empty subquery rejects every outer row,
/// and an outer NULL key is rejected unless the subquery is empty. Plain
/// `"anti"` is `NOT EXISTS`, where NULL keys only fail to match.
pub(super) const NULL_AWARE_ANTI: &str = "anti_null_aware";

/// The residual ON predicates of a join and the CRS context they evaluate in.
#[derive(Clone, Copy)]
pub(super) struct JoinResidual<'a> {
    pub(super) filters: &'a [crate::bridge::scan_filter::ScanFilter],
    pub(super) crs: &'a nodedb_crs::CrsContext<'a>,
}

impl JoinResidual<'_> {
    /// Whether the merged row `row` passes every residual predicate.
    pub(super) fn admits(&self, row: &[u8]) -> Result<bool, nodedb_query::EvalError> {
        binary_row_matches_filters(row, self.filters, self.crs)
    }
}

/// Whether `row` passes `residual`. A join without a residual admits it.
fn residual_admits(
    residual: Option<&JoinResidual<'_>>,
    row: &[u8],
) -> Result<bool, nodedb_query::EvalError> {
    residual.map_or(Ok(true), |r| r.admits(row))
}

/// Parameters for probing a hash join index.
pub(super) struct ProbeParams<'a> {
    pub(super) probe_docs: &'a [(String, Vec<u8>)],
    pub(super) index: &'a HashIndex,
    pub(super) index_docs: &'a [(String, Vec<u8>)],
    pub(super) probe_keys: &'a [&'a str],
    pub(super) join_type: &'a str,
    pub(super) limit: usize,
    pub(super) probe_collection: &'a str,
    pub(super) index_collection: &'a str,
    /// Residual ON predicates. Candidates that fail these predicates do not
    /// count as matches for outer-join null extension. `None` when the join
    /// has no residual predicate.
    pub(super) join_filters: Option<JoinResidual<'a>>,
    /// For broadcast RIGHT/FULL joins: only the designated core should emit
    /// unmatched right-side rows. Other cores set this to `false` to avoid
    /// N× duplication of unmatched rows across cores.
    pub(super) emit_unmatched_right: bool,
}

/// Probe a hash index with probe-side documents and produce join results.
///
/// Returns binary msgpack rows — no JSON decode.
/// Matches keys by their canonical identity.
///
/// This is the single-shot (fully-materialized probe side) entry point. It is
/// composed from the streaming-friendly pieces [`probe_rows_into`] and
/// [`emit_unmatched_right_into`] so that the grace-hash streaming path can reuse
/// the EXACT same emission logic batch-by-batch without duplicating it. The
/// composed behavior here is byte-identical to passing the whole probe side in a
/// single batch.
///
/// A division/modulo-by-zero in a join's residual ON predicate PROPAGATES
/// as an `EvalError` (surfaced to the client as SQLSTATE 22012) rather than
/// folding to "no match": this fn is fallible and threads the error up to
/// the bridge Response boundary.
pub(super) fn probe_hash_index(
    p: &ProbeParams<'_>,
) -> Result<Vec<Vec<u8>>, nodedb_query::EvalError> {
    let is_right = p.join_type == "right" || p.join_type == "full";
    let is_cross = p.join_type == "cross";

    // Cross join: cartesian product (no hash lookup needed).
    if is_cross {
        let mut results = Vec::new();
        for (_, left_val) in p.probe_docs {
            for (_, right_val) in p.index_docs {
                if results.len() >= p.limit {
                    return Ok(results);
                }
                let merged = merge_join_docs_binary(
                    left_val,
                    Some(right_val),
                    p.probe_collection,
                    p.index_collection,
                )?;
                // A division/modulo-by-zero in a join's residual ON
                // predicate PROPAGATES here (SQLSTATE 22012) rather than
                // folding to "no match".
                if residual_admits(p.join_filters.as_ref(), &merged)? {
                    results.push(merged);
                }
            }
        }
        return Ok(results);
    }

    // For RIGHT/FULL joins, pre-allocate a complete tracking vector so we
    // never miss marking a matched index-side row (even if we hit the limit
    // during the probe loop). This prevents the cartesian product bug where
    // incomplete tracking causes matched rows to be emitted as unmatched.
    let mut index_matched: Vec<bool> = if is_right {
        vec![false; p.index_docs.len()]
    } else {
        Vec::new()
    };
    let mut results = Vec::new();

    probe_rows_into(p, &mut results, &mut index_matched)?;

    // RIGHT/FULL: emit unmatched index-side rows.
    if is_right && p.emit_unmatched_right {
        emit_unmatched_right_into(p, &mut results, &index_matched)?;
    }

    Ok(results)
}

/// Append join output for the probe rows in `p.probe_docs` to `results`,
/// marking `index_matched` for RIGHT/FULL hits.
///
/// This is the per-probe-row emission loop body factored out of
/// [`probe_hash_index`]. It does NOT handle the cross-join cartesian product
/// (the caller short-circuits that) and does NOT perform the unmatched-right
/// sweep (call [`emit_unmatched_right_into`] separately, once, after all probe
/// batches are processed).
///
/// `p.limit` is honored against the SHARED `results.len()`, so this is safe to
/// call repeatedly with the same `results` for successive probe batches: the
/// global output limit is enforced across batches, not per batch. Likewise
/// `index_matched` is shared across batches so RIGHT/FULL match tracking
/// accumulates correctly.
///
/// `index_matched` must be sized `p.index_docs.len()` for RIGHT/FULL joins (and
/// may be empty otherwise — it is only indexed when the join is RIGHT/FULL).
///
/// A division/modulo-by-zero in a residual ON predicate PROPAGATES as an
/// `EvalError` (SQLSTATE 22012) rather than folding to "no match".
pub(super) fn probe_rows_into(
    p: &ProbeParams<'_>,
    results: &mut Vec<Vec<u8>>,
    index_matched: &mut [bool],
) -> Result<(), nodedb_query::EvalError> {
    let is_left = p.join_type == "left" || p.join_type == "full";
    let is_right = p.join_type == "right" || p.join_type == "full";
    let is_semi = p.join_type == "semi";
    let null_aware = p.join_type == NULL_AWARE_ANTI;
    let is_anti = p.join_type == "anti" || null_aware;

    // `x NOT IN (s)` with a NULL in a non-empty `s` is never true.
    if null_aware && p.index.rejects_every_null_aware_probe() {
        return Ok(());
    }

    for (_, value) in p.probe_docs {
        // For RIGHT/FULL joins, we must complete the full probe to populate
        // index_matched, even after we have enough result rows.
        if !is_right && results.len() >= p.limit {
            break;
        }
        // A composite `NOT IN` compares row-wise: the row is dropped when
        // some build tuple possibly equals it, else emitted as unmatched.
        if null_aware && p.probe_keys.len() > 1 {
            if !p
                .index
                .composite_not_in_rejects(value, p.probe_keys, p.index_docs)?
                && results.len() < p.limit
            {
                results.push(merge_join_docs_binary(value, None, p.probe_collection, "")?);
            }
            continue;
        }
        let (null_key, candidate_indices) = p.index.probe_row(value, p.probe_keys, p.index_docs)?;
        // `NULL NOT IN (s)` is unknown for a non-empty `s`: the row is
        // excluded. Over an empty `s` it is true, and the row is kept below.
        if null_aware && null_key && p.index.rows > 0 {
            continue;
        }
        // A closure can't use `?`, so filter with an explicit loop: a div-by-zero
        // in the residual ON predicate returns `Err` from this fn (SQLSTATE
        // 22012) instead of folding to "no match".
        let mut matched_indices: Vec<usize> = Vec::new();
        for index in candidate_indices {
            let Some(residual) = p.join_filters.as_ref() else {
                matched_indices.push(index);
                continue;
            };
            let merged = merge_join_docs_binary(
                value,
                Some(&p.index_docs[index].1),
                p.probe_collection,
                p.index_collection,
            )?;
            if residual.admits(&merged)? {
                matched_indices.push(index);
            }
        }

        if !matched_indices.is_empty() {
            if is_semi {
                if results.len() < p.limit {
                    results.push(merge_join_docs_binary(value, None, p.probe_collection, "")?);
                }
            } else if is_anti {
                // Skip — has match.
            } else {
                for &mi in &matched_indices {
                    if is_right {
                        index_matched[mi] = true;
                    }
                    if results.len() < p.limit {
                        results.push(merge_join_docs_binary(
                            value,
                            Some(&p.index_docs[mi].1),
                            p.probe_collection,
                            p.index_collection,
                        )?);
                    }
                }
            }
        } else if is_anti && results.len() < p.limit {
            results.push(merge_join_docs_binary(value, None, p.probe_collection, "")?);
        } else if is_left && results.len() < p.limit {
            results.push(merge_join_docs_binary(
                value,
                None,
                p.probe_collection,
                p.index_collection,
            )?);
        }
    }
    Ok(())
}

/// Emit unmatched index-side (build/right) rows for RIGHT/FULL joins into
/// `results`, honoring `p.limit` against the shared `results.len()`.
///
/// Call this exactly ONCE, after every probe batch has been fed through
/// [`probe_rows_into`] with the shared `index_matched`. In broadcast mode only
/// the designated core sets `p.emit_unmatched_right`; the caller is responsible
/// for gating on that flag (this function unconditionally sweeps).
pub(super) fn emit_unmatched_right_into(
    p: &ProbeParams<'_>,
    results: &mut Vec<Vec<u8>>,
    index_matched: &[bool],
) -> Result<(), nodedb_query::EvalError> {
    for (i, (_, bytes)) in p.index_docs.iter().enumerate() {
        if results.len() >= p.limit {
            break;
        }
        if !index_matched[i] {
            results.push(merge_join_docs_binary(
                &[],
                Some(bytes),
                "",
                p.index_collection,
            )?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;

    use super::*;

    fn row(fields: &[(&str, Value)]) -> Value {
        Value::Object(
            fields
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
        )
    }

    fn native(fields: &[(&str, Value)]) -> Vec<u8> {
        zerompk::to_msgpack_vec(&row(fields)).expect("native row")
    }

    fn plain(fields: &[(&str, Value)]) -> Vec<u8> {
        nodedb_types::value_to_msgpack(&row(fields)).expect("plain row")
    }

    /// Output row count of a hash join of `probe` against `build` on `k`.
    fn join_rows(
        probe: &[(String, Vec<u8>)],
        build: &[(String, Vec<u8>)],
        join_type: &str,
    ) -> usize {
        let index = HashIndex::build(build, &["k"]).expect("build");
        probe_hash_index(&ProbeParams {
            probe_docs: probe,
            index: &index,
            index_docs: build,
            probe_keys: &["k"],
            join_type,
            limit: usize::MAX,
            probe_collection: "l",
            index_collection: "r",
            join_filters: None,
            emit_unmatched_right: true,
        })
        .expect("probe")
        .len()
    }

    /// NULL and missing keys match nothing: inner and semi drop them, anti
    /// keeps the probe row, and outer joins null-extend them.
    #[test]
    fn null_keys_never_match() {
        let empty = zerompk::to_msgpack_vec(&Value::Object(Default::default())).expect("empty");
        let probe = vec![
            (String::new(), native(&[("k", Value::Null)])),
            (String::new(), empty.clone()),
            (String::new(), native(&[("k", Value::Integer(1))])),
        ];
        let build = vec![
            (String::new(), plain(&[("k", Value::Null)])),
            (String::new(), empty),
            (String::new(), plain(&[("k", Value::Integer(1))])),
        ];
        assert_eq!(join_rows(&probe, &build, "inner"), 1);
        assert_eq!(join_rows(&probe, &build, "semi"), 1);
        assert_eq!(join_rows(&probe, &build, "anti"), 2);
        assert_eq!(join_rows(&probe, &build, "left"), 3);
        assert_eq!(join_rows(&probe, &build, "right"), 3);
        assert_eq!(join_rows(&probe, &build, "full"), 5);
    }

    fn keyed(value: Value) -> (String, Vec<u8>) {
        (String::new(), native(&[("k", value)]))
    }

    /// `NOT IN`: a NULL in the non-empty subquery rejects every outer row,
    /// an outer NULL is rejected unless the subquery is empty, and other
    /// rows follow anti semantics. `NOT EXISTS` (`"anti"`) keeps NULL rows.
    #[test]
    fn null_aware_anti_follows_not_in_semantics() {
        let outer = vec![
            keyed(Value::Integer(1)),
            keyed(Value::Integer(2)),
            keyed(Value::Null),
        ];
        let plain_sub = vec![keyed(Value::Integer(1))];
        assert_eq!(join_rows(&outer, &plain_sub, NULL_AWARE_ANTI), 1);
        assert_eq!(join_rows(&outer, &plain_sub, "anti"), 2);

        let null_sub = vec![keyed(Value::Integer(1)), keyed(Value::Null)];
        assert_eq!(join_rows(&outer, &null_sub, NULL_AWARE_ANTI), 0);
        assert_eq!(join_rows(&outer, &null_sub, "anti"), 2);

        let missing_sub = vec![(
            String::new(),
            zerompk::to_msgpack_vec(&Value::Object(Default::default())).expect("empty"),
        )];
        assert_eq!(join_rows(&outer, &missing_sub, NULL_AWARE_ANTI), 0);

        assert_eq!(join_rows(&outer, &[], NULL_AWARE_ANTI), 3);
        assert_eq!(join_rows(&outer, &[], "anti"), 3);
    }

    /// A composite NOT IN key follows SQL row comparison: a NULL component
    /// is unknown, so it rejects only when the other components agree.
    #[test]
    fn null_aware_anti_compares_composite_keys_row_wise() {
        let row = |a: Value, b: Value| {
            (
                String::new(),
                zerompk::to_msgpack_vec(&Value::Object(
                    [("a".to_owned(), a), ("b".to_owned(), b)]
                        .into_iter()
                        .collect(),
                ))
                .expect("row"),
            )
        };
        let count = |outer: &[(String, Vec<u8>)], sub: &[(String, Vec<u8>)]| {
            let index = HashIndex::build(sub, &["a", "b"]).expect("build");
            probe_hash_index(&ProbeParams {
                probe_docs: outer,
                index: &index,
                index_docs: sub,
                probe_keys: &["a", "b"],
                join_type: NULL_AWARE_ANTI,
                limit: usize::MAX,
                probe_collection: "l",
                index_collection: "r",
                join_filters: None,
                emit_unmatched_right: true,
            })
            .expect("probe")
            .len()
        };
        use Value::{Integer as I, Null};
        let one = |a: Value, b: Value| vec![row(a, b)];
        // SQL row comparison: NULL components are unknown, not "always equal".
        assert_eq!(
            count(&one(I(1), I(1)), &one(I(2), Null)),
            1,
            "(1,1) NOT IN ((2,NULL))"
        );
        assert_eq!(
            count(&one(I(1), I(1)), &one(I(1), Null)),
            0,
            "(1,1) NOT IN ((1,NULL))"
        );
        assert_eq!(
            count(&one(I(1), Null), &one(I(2), I(2))),
            1,
            "(1,NULL) NOT IN ((2,2))"
        );
        assert_eq!(
            count(&one(I(1), Null), &one(I(1), I(2))),
            0,
            "(1,NULL) NOT IN ((1,2))"
        );
        assert_eq!(
            count(&one(I(1), Null), &[]),
            1,
            "empty subquery keeps every row"
        );
        assert_eq!(count(&one(I(1), I(1)), &one(I(1), I(1))), 0, "exact match");
    }

    #[test]
    fn null_aware_anti_name_matches_the_planner() {
        assert_eq!(
            nodedb_sql::types::JoinType::AntiNullAware.as_str(),
            NULL_AWARE_ANTI
        );
    }
}
