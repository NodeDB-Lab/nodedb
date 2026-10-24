// SPDX-License-Identifier: BUSL-1.1

//! Hash join core data structures and algorithm — index build and probe.

use nodedb_query::msgpack_scan;

use super::{binary_row_matches_filters, merge_join_docs_binary};

/// Hash a join key from raw msgpack bytes — zero String allocation.
///
/// For single-field keys: hashes the raw value bytes directly.
/// For composite keys: hashes each field's raw bytes sequentially.
/// Returns `(hash, key_ranges)` — the ranges are kept for collision resolution via memcmp.
pub(super) fn hash_join_key(
    doc: &[u8],
    keys: &[&str],
    state: &std::collections::hash_map::RandomState,
) -> (u64, Vec<(usize, usize)>) {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = state.build_hasher();
    let mut ranges = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some((start, end)) = extract_join_key_range(doc, key) {
            hasher.write(&doc[start..end]);
            ranges.push((start, end));
        } else {
            // Missing field — hash a sentinel.
            hasher.write_u8(0xc0); // NIL tag
            ranges.push((0, 0));
        }
    }
    (hasher.finish(), ranges)
}

pub(super) fn extract_join_key_range(doc: &[u8], key: &str) -> Option<(usize, usize)> {
    msgpack_scan::extract_field(doc, 0, key).or_else(|| {
        key.rsplit_once('.')
            .and_then(|(_, field)| msgpack_scan::extract_field(doc, 0, field))
    })
}

/// (doc_index, key_ranges) for a single hash bucket entry.
type BucketEntry = (usize, Vec<(usize, usize)>);

/// Build side of hash join: hash index keys, store (hash → doc indices + key ranges).
pub(super) struct HashIndex {
    pub(super) buckets: std::collections::HashMap<u64, Vec<BucketEntry>>,
    pub(super) state: std::collections::hash_map::RandomState,
}

impl HashIndex {
    pub(super) fn build(docs: &[(String, Vec<u8>)], keys: &[&str]) -> Self {
        let state = std::collections::hash_map::RandomState::new();
        let mut buckets: std::collections::HashMap<u64, Vec<BucketEntry>> =
            std::collections::HashMap::with_capacity(docs.len());
        for (i, (_, value)) in docs.iter().enumerate() {
            let (hash, ranges) = hash_join_key(value, keys, &state);
            buckets.entry(hash).or_default().push((i, ranges));
        }
        Self { buckets, state }
    }

    /// Find all doc indices whose key bytes match the probe key.
    ///
    /// A join with no equi-key at all (`ON a.x > b.y`, or a cross join) has
    /// nothing to hash on: every build row is a candidate, and the residual ON
    /// predicate decides which pairs survive. Treating "no keys requested" as
    /// "no key bytes matched" would drop every pair and answer such a join with
    /// zero rows.
    pub(super) fn probe(
        &self,
        probe_doc: &[u8],
        probe_keys: &[&str],
        build_docs: &[(String, Vec<u8>)],
    ) -> (u64, Vec<(usize, usize)>, Vec<usize>) {
        if probe_keys.is_empty() {
            return (0, Vec::new(), (0..build_docs.len()).collect());
        }

        let (hash, probe_ranges) = hash_join_key(probe_doc, probe_keys, &self.state);
        let mut matched = Vec::new();
        if let Some(bucket) = self.buckets.get(&hash) {
            for (doc_idx, idx_ranges) in bucket {
                // Verify actual byte ranges match — hash collisions are possible.
                let mut all_match = !probe_ranges.is_empty();
                for (i, &(ps, pe)) in probe_ranges.iter().enumerate() {
                    if let Some(&(bs, be)) = idx_ranges.get(i) {
                        let build_doc = &build_docs[*doc_idx].1;
                        if pe - ps != be - bs || probe_doc[ps..pe] != build_doc[bs..be] {
                            all_match = false;
                            break;
                        }
                    } else {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    matched.push(*doc_idx);
                }
            }
        }
        (hash, probe_ranges, matched)
    }
}

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
/// Uses u64 hash keys — zero String allocation for key matching.
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
                );
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
        emit_unmatched_right_into(p, &mut results, &index_matched);
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
    let is_anti = p.join_type == "anti";

    for (_, value) in p.probe_docs {
        // For RIGHT/FULL joins, we must complete the full probe to populate
        // index_matched, even after we have enough result rows.
        if !is_right && results.len() >= p.limit {
            break;
        }
        let (_, _, candidate_indices) = p.index.probe(value, p.probe_keys, p.index_docs);
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
            );
            if residual.admits(&merged)? {
                matched_indices.push(index);
            }
        }

        if !matched_indices.is_empty() {
            if is_semi {
                if results.len() < p.limit {
                    results.push(merge_join_docs_binary(value, None, p.probe_collection, ""));
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
                        ));
                    }
                }
            }
        } else if is_anti && results.len() < p.limit {
            results.push(merge_join_docs_binary(value, None, p.probe_collection, ""));
        } else if is_left && results.len() < p.limit {
            results.push(merge_join_docs_binary(
                value,
                None,
                p.probe_collection,
                p.index_collection,
            ));
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
) {
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
            ));
        }
    }
}
