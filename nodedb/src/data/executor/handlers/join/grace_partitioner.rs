// SPDX-License-Identifier: BUSL-1.1

//! In-memory grace partitioner for the hash join.
//!
//! This is the building block that an upcoming integration step will wire into
//! `execute_hash_join` with spill + cap removal. It does NOT touch
//! `execute_hash_join` and adds NO spill — it is purely in-memory and produces
//! results IDENTICAL (as a multiset) to today's single-index hash join for
//! every join type.
//!
//! ## Why partitioning is correct
//!
//! Today's join matches rows by byte-equality of the extracted key ranges
//! (see `hash.rs::HashIndex::probe` — equal byte ranges via memcmp). The
//! partitioner here hashes those SAME extracted value bytes with a FIXED-SEED
//! hasher, so two rows whose key bytes are equal always produce the same
//! `partition_hash` and therefore land in the same partition. Any pair that
//! *could* match is never separated across partitions. Hash collisions between
//! non-matching rows are harmless: the per-partition `probe_hash_index` still
//! memcmp-rejects them exactly as the un-partitioned path does.

use super::hash::{HashIndex, ProbeParams, probe_hash_index};

/// Immutable configuration for a grace-hash join — the join-key fields on each
/// side, the join type, the output limit, the collection/alias prefixes used to
/// qualify emitted columns, and whether unmatched build-side rows are emitted.
///
/// Bundled into one struct (like `ProbeParams`) so callers pass named fields
/// rather than a long, transposition-prone positional argument list.
pub(super) struct GraceSpec<'a> {
    pub(super) build_keys: &'a [&'a str],
    pub(super) probe_keys: &'a [&'a str],
    pub(super) join_type: &'a str,
    pub(super) limit: usize,
    pub(super) probe_collection: &'a str,
    pub(super) index_collection: &'a str,
    pub(super) emit_unmatched_right: bool,
}

/// Stable, fixed-seed partition hash over a document's join-key value bytes.
///
/// Mirrors `hash_join_key`'s extraction and missing-field handling EXACTLY:
/// - present field → hash the raw extracted value bytes (`doc[start..end]`);
/// - missing field → hash the `0xc0` (msgpack NIL) sentinel.
///
/// Only the VALUE bytes are hashed (never the field name), in `keys` order.
///
/// Uses [`std::hash::DefaultHasher`] (deterministic, fixed keys) rather than
/// `RandomState`: build and probe sides MUST hash identically across calls so
/// that equal-key rows co-locate in the same partition.
///
/// `keys` is generic over any `AsRef<str>` element type so callers can pass
/// `&[&str]` or `&[String]` without an intermediate `Vec<&str>` allocation.
///
/// This is the TOP-LEVEL partitioning hash and is defined as
/// `partition_hash_seeded(doc, keys, 0)` — the seed-`0` behavior is fixed and
/// must NOT change, since the spiller's top-level partition routing depends on
/// it being stable across calls.
pub(super) fn partition_hash<S: AsRef<str>>(doc: &[u8], keys: &[S]) -> u64 {
    // Delegate to the shared `nodedb-query` routing hash so the node-local grace
    // join and the cross-node shuffle producer compute byte-identical partitions
    // (build/probe co-location is a correctness invariant, not an optimization).
    nodedb_query::partition_hash(doc, keys)
}

/// Seeded variant of [`partition_hash`] used for RECURSIVE re-partitioning of a
/// skewed partition.
///
/// Re-partitioning a partition that overflowed its memory budget with the SAME
/// hash would deterministically reproduce the same split (every row lands in the
/// same sub-bucket), making no progress. Mixing a fresh `seed` into the hasher
/// BEFORE the key bytes redistributes DISTINCT keys across the sub-buckets while
/// preserving the co-location invariant WITHIN that seed: two rows with equal
/// key bytes still hash identically (same seed, same bytes), so any pair that
/// could match still co-locates in the same sub-partition. (Identical-key skew
/// is therefore unsplittable at any seed — the depth cap in `grace_spill`
/// handles that case with a clean `MemoryExhausted`.)
///
/// The seed is written first so that the same `(seed, keys)` pair on the build
/// and probe sides produces matching routing.
pub(super) fn partition_hash_seeded<S: AsRef<str>>(doc: &[u8], keys: &[S], seed: u64) -> u64 {
    nodedb_query::partition_hash_seeded(doc, keys, seed)
}

/// In-memory grace join: partition both inputs by `partition_hash`, then run
/// the reference `probe_hash_index` per partition and union the results.
///
/// Consumes OWNED Vecs and drains them into partition buffers BY MOVE (zero
/// clone in the hot path). In the integration step these owned Vecs come
/// straight from `execute_hash_join`.
///
/// `build_docs` is the RIGHT (index) side and `build_keys` its join-key fields;
/// `probe_docs` is the LEFT side and `probe_keys` its join-key fields,
/// positionally aligned with `build_keys`.
///
/// `limit` is the GLOBAL output limit. It is applied ONCE, after unioning all
/// partitions, via `results.truncate(limit)`. The per-partition probe is run
/// with `usize::MAX` so that no single partition prematurely truncates — using
/// the real limit per partition could emit up to `P × limit` rows.
///
/// ## Correctness notes
///
/// - Degenerate / cross joins are NOT partitioned. A cross join (or a keyless
///   join) is a cartesian product: every left row must still see every right
///   row, so hash-partitioning by key would break it. These run as a single
///   partition.
/// - Per-partition unmatched-right tracking is correct WITHOUT cross-partition
///   aggregation: a right row can only be matched by probe rows that hash to
///   its partition, so if it is unmatched within its partition it is globally
///   unmatched. `probe_hash_index` therefore emits exactly the right set of
///   unmatched-right rows per partition.
/// - `HashIndex`'s internal `doc_index` is relative to the slice passed to
///   `build`; we pass the same partition slice as `index_docs` to
///   `probe_hash_index`, so the indices align.
///
/// Reference (owned-Vec, fully in-memory) grace join, used as the
/// multiset-equivalence ORACLE by the streamed/spilling production path's tests.
///
/// The production grace path streams via `PartitionedSpiller`
/// (`grace_spill.rs`) / `drive_grace_build`; this version materializes both
/// sides and is intentionally simple so it can be trusted as the reference.
#[allow(dead_code)]
pub(super) fn grace_join_in_memory(
    build_docs: Vec<(String, Vec<u8>)>,
    probe_docs: Vec<(String, Vec<u8>)>,
    partitions: usize,
    spec: &GraceSpec,
) -> Vec<Vec<u8>> {
    let build_keys = spec.build_keys;
    let probe_keys = spec.probe_keys;
    let join_type = spec.join_type;
    let limit = spec.limit;
    let probe_collection = spec.probe_collection;
    let index_collection = spec.index_collection;
    let emit_unmatched_right = spec.emit_unmatched_right;

    // Degenerate / cross → single partition. Partitioning a cross/keyless join
    // by hash would break the cartesian product (every left row must see every
    // right row), so run the whole thing through one `HashIndex` / probe.
    if join_type == "cross" || build_keys.is_empty() || probe_keys.is_empty() || partitions <= 1 {
        let index = HashIndex::build(&build_docs, build_keys);
        return probe_hash_index(&ProbeParams {
            probe_docs: &probe_docs,
            index: &index,
            index_docs: &build_docs,
            probe_keys,
            join_type,
            limit,
            probe_collection,
            index_collection,
            join_filters: &[],
            emit_unmatched_right,
        });
    }

    // Drain both inputs into `partitions` buffers BY MOVE — no clone.
    let mut build_part: Vec<Vec<(String, Vec<u8>)>> = vec![vec![]; partitions];
    let mut probe_part: Vec<Vec<(String, Vec<u8>)>> = vec![vec![]; partitions];

    for row in build_docs {
        let idx = (partition_hash(&row.1, build_keys) % partitions as u64) as usize;
        build_part[idx].push(row);
    }
    for row in probe_docs {
        let idx = (partition_hash(&row.1, probe_keys) % partitions as u64) as usize;
        probe_part[idx].push(row);
    }

    // Probe each partition independently. Use `usize::MAX` as the per-partition
    // limit — NEVER the real limit (else up to P×limit rows). Truncate once,
    // globally, after unioning.
    let mut results: Vec<Vec<u8>> = Vec::new();
    for i in 0..partitions {
        let index = HashIndex::build(&build_part[i], build_keys);
        let mut part_results = probe_hash_index(&ProbeParams {
            probe_docs: &probe_part[i],
            index: &index,
            index_docs: &build_part[i],
            probe_keys,
            join_type,
            limit: usize::MAX,
            probe_collection,
            index_collection,
            join_filters: &[],
            emit_unmatched_right,
        });
        results.append(&mut part_results);
    }

    results.truncate(limit);
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A side of a join: `(doc_id, raw msgpack bytes)` pairs — the shape
    /// `execute_hash_join` materializes and `grace_join_in_memory` consumes.
    type DocSet = Vec<(String, Vec<u8>)>;

    /// Build a msgpack map fixture using the same helper the existing join
    /// tests use (`nodedb_types::json_to_msgpack`), NOT serde_json directly.
    fn msgpack_row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).unwrap()
    }

    /// Sort a result set so it can be compared as a MULTISET (duplicates must
    /// be preserved, order must not matter).
    fn as_multiset(mut rows: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        rows.sort();
        rows
    }

    /// Reference: the un-partitioned single-index probe — exactly what
    /// `execute_hash_join` does today.
    fn reference(
        build_docs: &[(String, Vec<u8>)],
        probe_docs: &[(String, Vec<u8>)],
        spec: &GraceSpec,
    ) -> Vec<Vec<u8>> {
        let index = HashIndex::build(build_docs, spec.build_keys);
        probe_hash_index(&ProbeParams {
            probe_docs,
            index: &index,
            index_docs: build_docs,
            probe_keys: spec.probe_keys,
            join_type: spec.join_type,
            limit: spec.limit,
            probe_collection: spec.probe_collection,
            index_collection: spec.index_collection,
            join_filters: &[],
            emit_unmatched_right: spec.emit_unmatched_right,
        })
    }

    /// Single-key fixtures: matches, non-matches, duplicate keys (count must be
    /// preserved), and a row MISSING the key field on each side.
    fn single_key_fixtures() -> (DocSet, DocSet) {
        // Build (RIGHT) side.
        let build = vec![
            (
                "b1".into(),
                msgpack_row(&[("k", serde_json::json!(1)), ("rv", serde_json::json!("r1"))]),
            ),
            (
                "b2".into(),
                msgpack_row(&[
                    ("k", serde_json::json!(1)),
                    ("rv", serde_json::json!("r1dup")),
                ]),
            ), // dup key 1
            (
                "b3".into(),
                msgpack_row(&[("k", serde_json::json!(2)), ("rv", serde_json::json!("r2"))]),
            ),
            (
                "b4".into(),
                msgpack_row(&[("k", serde_json::json!(9)), ("rv", serde_json::json!("r9"))]),
            ), // no match
            (
                "b5".into(),
                msgpack_row(&[("rv", serde_json::json!("r-nokey"))]),
            ), // missing key
        ];
        // Probe (LEFT) side.
        let probe = vec![
            (
                "p1".into(),
                msgpack_row(&[("k", serde_json::json!(1)), ("lv", serde_json::json!("l1"))]),
            ),
            (
                "p2".into(),
                msgpack_row(&[
                    ("k", serde_json::json!(1)),
                    ("lv", serde_json::json!("l1dup")),
                ]),
            ), // dup key 1
            (
                "p3".into(),
                msgpack_row(&[("k", serde_json::json!(2)), ("lv", serde_json::json!("l2"))]),
            ),
            (
                "p4".into(),
                msgpack_row(&[("k", serde_json::json!(7)), ("lv", serde_json::json!("l7"))]),
            ), // no match
            (
                "p5".into(),
                msgpack_row(&[("lv", serde_json::json!("l-nokey"))]),
            ), // missing key
        ];
        (build, probe)
    }

    const ALL_JOIN_TYPES: [&str; 7] = ["inner", "left", "right", "full", "semi", "anti", "cross"];

    #[test]
    fn multiset_equivalence_all_join_types_all_partition_counts() {
        let (build, probe) = single_key_fixtures();
        let build_keys = ["k"];
        let probe_keys = ["k"];

        for jt in ALL_JOIN_TYPES {
            let spec = GraceSpec {
                build_keys: &build_keys,
                probe_keys: &probe_keys,
                join_type: jt,
                limit: usize::MAX,
                probe_collection: "l",
                index_collection: "r",
                emit_unmatched_right: true,
            };
            let want = as_multiset(reference(&build, &probe, &spec));

            for p in [1usize, 2, 4, 8] {
                let candidate = grace_join_in_memory(build.clone(), probe.clone(), p, &spec);
                assert_eq!(
                    want,
                    as_multiset(candidate),
                    "join_type={jt} partitions={p} multiset mismatch"
                );
            }
        }
    }

    #[test]
    fn composite_key_equivalence_inner_and_left() {
        let build = vec![
            (
                "b1".into(),
                msgpack_row(&[
                    ("a", serde_json::json!(1)),
                    ("b", serde_json::json!("x")),
                    ("rv", serde_json::json!("r1")),
                ]),
            ),
            (
                "b2".into(),
                msgpack_row(&[
                    ("a", serde_json::json!(1)),
                    ("b", serde_json::json!("y")),
                    ("rv", serde_json::json!("r2")),
                ]),
            ),
            (
                "b3".into(),
                msgpack_row(&[
                    ("a", serde_json::json!(2)),
                    ("b", serde_json::json!("x")),
                    ("rv", serde_json::json!("r3")),
                ]),
            ),
            (
                "b4".into(),
                msgpack_row(&[
                    ("a", serde_json::json!(1)),
                    ("b", serde_json::json!("x")),
                    ("rv", serde_json::json!("r1dup")),
                ]),
            ), // dup composite
        ];
        let probe = vec![
            (
                "p1".into(),
                msgpack_row(&[
                    ("a", serde_json::json!(1)),
                    ("b", serde_json::json!("x")),
                    ("lv", serde_json::json!("l1")),
                ]),
            ),
            (
                "p2".into(),
                msgpack_row(&[
                    ("a", serde_json::json!(2)),
                    ("b", serde_json::json!("x")),
                    ("lv", serde_json::json!("l3")),
                ]),
            ),
            (
                "p3".into(),
                msgpack_row(&[
                    ("a", serde_json::json!(5)),
                    ("b", serde_json::json!("z")),
                    ("lv", serde_json::json!("nomatch")),
                ]),
            ),
        ];
        let build_keys = ["a", "b"];
        let probe_keys = ["a", "b"];

        for jt in ["inner", "left"] {
            let spec = GraceSpec {
                build_keys: &build_keys,
                probe_keys: &probe_keys,
                join_type: jt,
                limit: usize::MAX,
                probe_collection: "l",
                index_collection: "r",
                emit_unmatched_right: true,
            };
            let want = as_multiset(reference(&build, &probe, &spec));
            for p in [1usize, 2, 4, 8] {
                let candidate = grace_join_in_memory(build.clone(), probe.clone(), p, &spec);
                assert_eq!(
                    want,
                    as_multiset(candidate),
                    "composite join_type={jt} partitions={p}"
                );
            }
        }
    }

    #[test]
    fn empty_build_docs_matches_reference() {
        let (_, probe) = single_key_fixtures();
        let build: Vec<(String, Vec<u8>)> = Vec::new();
        let build_keys = ["k"];
        let probe_keys = ["k"];

        for jt in ALL_JOIN_TYPES {
            let spec = GraceSpec {
                build_keys: &build_keys,
                probe_keys: &probe_keys,
                join_type: jt,
                limit: usize::MAX,
                probe_collection: "l",
                index_collection: "r",
                emit_unmatched_right: true,
            };
            let want = as_multiset(reference(&build, &probe, &spec));
            for p in [1usize, 2, 4, 8] {
                let candidate = grace_join_in_memory(build.clone(), probe.clone(), p, &spec);
                assert_eq!(
                    want,
                    as_multiset(candidate),
                    "empty build join_type={jt} p={p}"
                );
            }
        }
    }

    #[test]
    fn empty_probe_docs_matches_reference() {
        let (build, _) = single_key_fixtures();
        let probe: Vec<(String, Vec<u8>)> = Vec::new();
        let build_keys = ["k"];
        let probe_keys = ["k"];

        for jt in ALL_JOIN_TYPES {
            let spec = GraceSpec {
                build_keys: &build_keys,
                probe_keys: &probe_keys,
                join_type: jt,
                limit: usize::MAX,
                probe_collection: "l",
                index_collection: "r",
                emit_unmatched_right: true,
            };
            let want = as_multiset(reference(&build, &probe, &spec));
            for p in [1usize, 2, 4, 8] {
                let candidate = grace_join_in_memory(build.clone(), probe.clone(), p, &spec);
                assert_eq!(
                    want,
                    as_multiset(candidate),
                    "empty probe join_type={jt} p={p}"
                );
            }
        }
    }

    #[test]
    fn limit_truncation_caps_output() {
        let (build, probe) = single_key_fixtures();
        let build_keys = ["k"];
        let probe_keys = ["k"];

        // Reference (unbounded) inner-join count, to pick a smaller limit.
        let unbounded_spec = GraceSpec {
            build_keys: &build_keys,
            probe_keys: &probe_keys,
            join_type: "inner",
            limit: usize::MAX,
            probe_collection: "l",
            index_collection: "r",
            emit_unmatched_right: true,
        };
        let full = reference(&build, &probe, &unbounded_spec);
        assert!(full.len() >= 2, "fixture must produce >= 2 inner rows");
        let limit = full.len() - 1;

        let limited_spec = GraceSpec {
            limit,
            ..unbounded_spec
        };
        for p in [1usize, 2, 4, 8] {
            let candidate = grace_join_in_memory(build.clone(), probe.clone(), p, &limited_spec);
            assert_eq!(candidate.len(), limit, "limit truncation p={p}");
        }
    }

    #[test]
    fn partition_hash_is_stable_for_equal_key_bytes() {
        // Same key value bytes → identical partition hash across calls, even
        // when surrounding fields differ. This is the co-location invariant.
        let a = msgpack_row(&[("k", serde_json::json!(42)), ("x", serde_json::json!("a"))]);
        let b = msgpack_row(&[
            ("k", serde_json::json!(42)),
            ("y", serde_json::json!("different")),
        ]);
        let keys = ["k"];
        assert_eq!(partition_hash(&a, &keys), partition_hash(&b, &keys));

        // Missing key on both sides hashes the NIL sentinel identically.
        let m1 = msgpack_row(&[("other", serde_json::json!(1))]);
        let m2 = msgpack_row(&[("nope", serde_json::json!(2))]);
        assert_eq!(partition_hash(&m1, &keys), partition_hash(&m2, &keys));
    }

    #[test]
    fn partition_hash_delegates_to_seed_zero() {
        // `partition_hash` MUST be exactly `partition_hash_seeded(.., 0)` so the
        // spiller's top-level routing is unchanged by the seeded refactor.
        let keys = ["k"];
        for v in 0..32i64 {
            let row = msgpack_row(&[("k", serde_json::json!(v))]);
            assert_eq!(
                partition_hash(&row, &keys),
                partition_hash_seeded(&row, &keys, 0),
                "partition_hash must equal seed=0 for v={v}"
            );
        }
    }

    #[test]
    fn partition_hash_seeded_redistributes_distinct_keys() {
        // Distinct keys that collide into a small number of buckets under one
        // seed should land in a DIFFERENT bucket distribution under another
        // seed — i.e. re-partitioning makes progress for distinct keys.
        let keys = ["k"];
        const BUCKETS: u64 = 8;

        let dist = |seed: u64| -> Vec<u64> {
            (0..64i64)
                .map(|v| {
                    let row = msgpack_row(&[("k", serde_json::json!(v))]);
                    partition_hash_seeded(&row, &keys, seed) % BUCKETS
                })
                .collect()
        };

        let d0 = dist(0);
        let d1 = dist(1);
        // The two seedings must not produce identical per-key bucket assignments
        // for every key (that would mean re-partition made no progress).
        assert_ne!(d0, d1, "seed change must redistribute distinct keys");

        // Equal key bytes still co-locate WITHIN a seed (co-location invariant).
        let a = msgpack_row(&[("k", serde_json::json!(42)), ("x", serde_json::json!("a"))]);
        let b = msgpack_row(&[("k", serde_json::json!(42)), ("y", serde_json::json!("b"))]);
        for seed in [0u64, 1, 7, 99] {
            assert_eq!(
                partition_hash_seeded(&a, &keys, seed),
                partition_hash_seeded(&b, &keys, seed),
                "equal key bytes must co-locate within seed={seed}"
            );
        }
    }
}
