// SPDX-License-Identifier: BUSL-1.1

//! Streaming partition-spiller for the grace-hash join.
//!
//! Where [`super::grace_partitioner::grace_join_in_memory`] takes OWNED Vecs of
//! every row (whole build + probe side in RAM at once), this module bounds
//! memory: rows are fed ONE AT A TIME via [`PartitionedSpiller::push_build`] /
//! [`PartitionedSpiller::push_probe`], and the moment a partition's in-memory
//! buffer exceeds `per_partition_budget` it is drained to a
//! [`SpillPartitionWriter`] on disk. From that point the partition's rows live
//! on NVMe, never in RAM, so the full build/probe side is NEVER all resident.
//!
//! `finish_and_probe` processes each partition through a work queue, holding ONE
//! partition resident at a time — so its size bound is the FULL per-query
//! materialization budget (`materialize_cap`), NOT the push-time
//! `per_partition_budget` spill threshold. A partition whose build side FITS
//! `materialize_cap` is materialized (streamed back from its spill file, or taken
//! from RAM), indexed via [`HashIndex`], and probed. A partition whose build side
//! EXCEEDS `materialize_cap` (join-key skew) is
//! RE-PARTITIONED instead of materialized: its spill file is stream-read frame-
//! by-frame and re-hashed with a fresh seed into sub-partitions (see
//! [`super::grace_repartition`]), which are processed recursively. A genuinely
//! unsplittable partition (identical-key skew) hits a depth cap and returns a
//! deterministic [`crate::Error::MemoryExhausted`] — it NEVER materializes an
//! oversized partition, so the spill path can never OOM. Results are unioned and
//! the GLOBAL `limit` is applied ONCE at the end — identical to the in-memory
//! reference.
//!
//! ## Why partitioning is correct
//!
//! Identical reasoning to `grace_partitioner`: [`partition_hash`] hashes the
//! SAME extracted key value bytes that [`HashIndex`] memcmp-matches on, with a
//! fixed seed, so any pair that *could* match co-locates in one partition.
//! Cross / keyless joins are cartesian products and run as a single partition.
//!
//! ## Why the doc id is dropped
//!
//! Join output is produced by `merge_join_docs_binary`, which only ever reads
//! the VALUE bytes of each side — the `String` id is never emitted. Storing
//! `(String::new(), value)` therefore loses nothing and avoids framing the id
//! through every spill file.
//!
//! ## Platform fallback
//!
//! Spilling requires io_uring ([`SpillPartitionWriter::create`] returns `None`
//! off-Linux or when uring is unavailable). When `create` returns `None` the
//! partition simply stays in memory: correctness is preserved, only the
//! memory bound is lost. This is a platform capability gap, not an error.

use std::path::PathBuf;

use super::grace_partitioner::{GraceSpec, partition_hash};
use super::grace_repartition::{PartitionSource, repartition_side};
use super::hash::{HashIndex, ProbeParams, probe_hash_index};
use super::spill::SpillPartitionWriter;

/// Maximum recursive re-partition depth before a skewed partition is declared
/// unsplittable.
///
/// Each re-partition level re-hashes with a fresh seed into [`SUB_P`] sub-
/// partitions. For DISTINCT keys this shrinks the largest sub-partition
/// geometrically (≈ by `SUB_P` per level), so even a build side many thousands
/// of times the budget fits within a few levels — 4 levels gives an effective
/// `SUB_P^4 = 16^4 = 65536`-way fan-out under the top-level P. The cap only ever
/// bites IDENTICAL-key skew (all rows the same join key), which is genuinely
/// unsplittable: there, the cap converts a would-be OOM into a deterministic
/// [`crate::Error::MemoryExhausted`].
const MAX_DEPTH: u32 = 4;

/// Number of sub-partitions produced per re-partition level. 16 keeps the fan-
/// out aggressive enough that distinct-key skew collapses in a handful of levels
/// while bounding the open-file/handle count per level (16 build + 16 probe
/// spill writers).
const SUB_P: usize = 16;

/// One join side's streaming partition state: P in-memory buffers, their running
/// byte totals, and a lazily-created spill writer per partition.
///
/// A partition is "in-memory" while its `spiller` is `None` and "spilled" once
/// the writer is `Some` — after which `buffers[p]` is empty and all further rows
/// for that partition append straight to disk.
struct SideState {
    /// Per-partition in-memory rows: `(empty id, value bytes)`.
    buffers: Vec<Vec<(String, Vec<u8>)>>,
    /// Per-partition running in-memory byte total (value bytes only).
    bytes: Vec<usize>,
    /// Per-partition spill writer — `None` until the partition starts spilling.
    spillers: Vec<Option<SpillPartitionWriter>>,
}

impl SideState {
    fn new(partitions: usize) -> Self {
        Self {
            buffers: vec![vec![]; partitions],
            bytes: vec![0; partitions],
            // `SpillPartitionWriter` is not `Clone`, so `vec![None; n]` won't
            // compile here — build the Vec with an iterator instead.
            spillers: (0..partitions).map(|_| None).collect(),
        }
    }
}

/// Streaming grace-hash partition spiller.
///
/// `!Send` — holds [`SpillPartitionWriter`]s, which wrap `!Send` / TPC-owned
/// io_uring writers. Lives entirely inside one Data-Plane core.
// Consumed by `drive_grace_build` (build-side stream + over-budget spill).
pub(super) struct PartitionedSpiller {
    /// Number of partitions (P ≥ 1; forced to 1 for cross / keyless joins).
    partitions: usize,
    /// PUSH-TIME spill threshold: an in-memory partition buffer spills to disk
    /// once it exceeds this many bytes. `0` = never spill (pure in-memory path,
    /// e.g. for non-Linux or tiny inputs). `drive_grace_build` sets this to
    /// `budget / P` so the aggregate of ~P in-RAM buffers stays within the full
    /// query budget; the tests pass `1` to FORCE every partition to spill.
    per_partition_budget: usize,
    /// FINISH-TIME re-partition threshold: in `finish_and_probe` ONE partition
    /// is materialized at a time, so the materialization bound is the FULL
    /// per-query budget (≈ `max_scan_result_bytes`), NOT the per-partition spill
    /// threshold. A spilled partition whose materialized build side exceeds this
    /// is re-partitioned instead of materialized. `0` = unlimited (never
    /// re-partition), matching the `per_partition_budget == 0` path. Decoupling
    /// this from `per_partition_budget` is essential: with `per_partition_budget`
    /// at `budget / 64` (or `1` in tests) every partition would otherwise look
    /// oversized and re-partition to the depth cap even for well-distributed
    /// distinct keys that materialize fine.
    materialize_cap: usize,
    /// Directory spill files are written into.
    spill_dir: PathBuf,
    /// Build-side join-key fields (owned; passed directly to `partition_hash`).
    build_keys: Vec<String>,
    /// Probe-side join-key fields (owned; positionally aligned with build_keys).
    probe_keys: Vec<String>,
    /// Join type string (e.g. "inner", "left", "cross").
    join_type: String,
    /// Global output row limit; applied once after all partitions are probed.
    limit: usize,
    /// Collection/alias prefix for the probe (left) side columns.
    probe_collection: String,
    /// Collection/alias prefix for the index (right/build) side columns.
    index_collection: String,
    /// Whether unmatched build-side rows are emitted (right/full outer joins).
    emit_unmatched_right: bool,
    build: SideState,
    probe: SideState,
}

impl PartitionedSpiller {
    /// Create a spiller for one join.
    ///
    /// `partitions` is clamped to 1 when the join is a cartesian product
    /// (`spec.join_type == "cross"`) or keyless (either key list empty): hash-
    /// partitioning by key would break the cross product, so everything must
    /// share one partition.
    pub(super) fn new(
        spec: &GraceSpec,
        partitions: usize,
        per_partition_budget: usize,
        materialize_cap: usize,
        spill_dir: PathBuf,
    ) -> Self {
        let build_keys: Vec<String> = spec.build_keys.iter().map(|s| s.to_string()).collect();
        let probe_keys: Vec<String> = spec.probe_keys.iter().map(|s| s.to_string()).collect();

        let partitions = if spec.join_type == "cross"
            || build_keys.is_empty()
            || probe_keys.is_empty()
            || partitions == 0
        {
            1
        } else {
            partitions
        };

        Self {
            partitions,
            per_partition_budget,
            materialize_cap,
            spill_dir,
            build_keys,
            probe_keys,
            join_type: spec.join_type.to_string(),
            limit: spec.limit,
            probe_collection: spec.probe_collection.to_string(),
            index_collection: spec.index_collection.to_string(),
            emit_unmatched_right: spec.emit_unmatched_right,
            build: SideState::new(partitions),
            probe: SideState::new(partitions),
        }
    }

    /// Feed one build-side (RIGHT / index) row's raw msgpack value bytes.
    pub(super) fn push_build(&mut self, value: &[u8]) -> crate::Result<()> {
        let p = (partition_hash(value, &self.build_keys) % self.partitions as u64) as usize;
        push_row(
            &mut self.build,
            p,
            value,
            self.per_partition_budget,
            &self.spill_dir,
            "build",
        )
    }

    /// Feed one probe-side (LEFT) row's raw msgpack value bytes.
    pub(super) fn push_probe(&mut self, value: &[u8]) -> crate::Result<()> {
        let p = (partition_hash(value, &self.probe_keys) % self.partitions as u64) as usize;
        push_row(
            &mut self.probe,
            p,
            value,
            self.per_partition_budget,
            &self.spill_dir,
            "probe",
        )
    }

    /// Consume the spiller: process every partition through a work queue that
    /// RE-PARTITIONS any partition whose build side exceeds `materialize_cap`
    /// (the full per-query budget — one partition is resident at a time here, so
    /// `per_partition_budget`, the push-time spill threshold, would wrongly flag
    /// well-distributed partitions) instead of materializing it whole. This bounds peak
    /// memory under join-key skew: the only thing held resident is one fitting
    /// (sub-)partition's build index + one streaming row, never a whole oversized
    /// partition. A genuinely unsplittable partition (identical-key skew) hits
    /// the [`MAX_DEPTH`] cap and returns [`crate::Error::MemoryExhausted`] —
    /// never an OOM, never a panic.
    ///
    /// The per-(sub-)partition probe runs with `usize::MAX` — NEVER the real
    /// `limit` (else up to P×limit rows). The GLOBAL `limit` is applied ONCE,
    /// after the queue drains, via `results.truncate(limit)`. Same rule as
    /// `grace_join_in_memory`.
    ///
    /// ## emit_unmatched_right across sub-partitions
    ///
    /// Per-(sub-)partition unmatched-right tracking stays correct WITHOUT cross-
    /// partition aggregation, recursively: a build row can only be matched by
    /// probe rows that hash to the SAME (sub-)partition under the SAME seed
    /// (`partition_hash_seeded` is deterministic, and the build and probe sides
    /// are always re-partitioned with the SAME seed + their respective keys). So
    /// if a build row is unmatched within its terminal sub-partition it is
    /// globally unmatched, and the existing per-partition
    /// `probe_hash_index(emit_unmatched_right=..)` emits exactly the right set.
    pub(super) fn finish_and_probe(self) -> crate::Result<Vec<Vec<u8>>> {
        // Destructure self to move all fields out at once — no clones needed.
        let PartitionedSpiller {
            partitions,
            // `per_partition_budget` is the PUSH-TIME spill threshold; it is not
            // a materialization bound, so it is consumed only by `push_*` (which
            // already ran). The re-partition trigger below uses `materialize_cap`.
            per_partition_budget: _,
            materialize_cap,
            spill_dir,
            join_type,
            limit,
            probe_collection,
            index_collection,
            emit_unmatched_right,
            build_keys,
            probe_keys,
            build,
            probe,
        } = self;

        // Build &str views once (per call, not per-row).
        let build_key_refs: Vec<&str> = build_keys.iter().map(String::as_str).collect();
        let probe_key_refs: Vec<&str> = probe_keys.iter().map(String::as_str).collect();

        // Seed the work queue with the top-level partitions. A non-spilled
        // partition becomes an in-memory source (≤budget by construction); a
        // spilled partition is `finish()`ed to its on-disk path and becomes a
        // spilled source whose size will be checked below.
        let mut build_buffers = build.buffers;
        let mut build_spillers = build.spillers;
        let mut probe_buffers = probe.buffers;
        let mut probe_spillers = probe.spillers;

        let mut queue: Vec<WorkItem> = Vec::with_capacity(partitions);
        for i in 0..partitions {
            let build_src = side_source(
                build_spillers[i].take(),
                std::mem::take(&mut build_buffers[i]),
            )?;
            let probe_src = side_source(
                probe_spillers[i].take(),
                std::mem::take(&mut probe_buffers[i]),
            )?;
            queue.push(WorkItem {
                build: build_src,
                probe: probe_src,
                depth: 0,
            });
        }

        let mut results: Vec<Vec<u8>> = Vec::new();
        // `next_repartition_id` names the per-level sub-directory so re-partition
        // outputs from different oversized partitions never collide. All live
        // under `spill_dir`, so `drive_grace_build`'s recursive `remove_dir_all`
        // cleans them.
        let mut next_repartition_id: u64 = 0;

        while let Some(item) = queue.pop() {
            // Whether this source CAN be re-partitioned at all. Only a SPILLED
            // build side is a re-partition candidate: a spilled file proves a
            // `SpillPartitionWriter` was creatable (io_uring available), so the
            // sub-partition writers can be created too, and the spilled rows can
            // be streamed back frame-by-frame rather than held whole.
            //
            // An IN-MEMORY source is NEVER re-partitioned: it is already resident,
            // so re-partitioning it to disk and back gains nothing; and on a no-
            // io_uring platform (where nothing ever spills) it CANNOT be — that is
            // the documented platform memory-bound gap, not an error. In-memory
            // top-level partitions are also ≤budget by construction on the spill
            // path (they spill the instant they cross it).
            let is_spilled = matches!(item.build, PartitionSource::Spilled(_));
            let build_size = item.build.size_bytes()?;

            // Fits → materialize and probe directly. The bound is the FULL
            // per-query materialization budget (`materialize_cap`), NOT the
            // per-partition spill threshold: `finish_and_probe` holds ONE
            // partition resident at a time, so a partition is only too big to
            // materialize when it exceeds the whole-query budget. `materialize_cap
            // == 0` means "unlimited" (never re-partition), so everything fits.
            let fits = !is_spilled || materialize_cap == 0 || build_size <= materialize_cap;
            if fits {
                let build_docs = item.build.materialize()?;
                let probe_docs = item.probe.materialize()?;

                let index = HashIndex::build(&build_docs, &build_key_refs);
                let mut part = probe_hash_index(&ProbeParams {
                    probe_docs: &probe_docs,
                    index: &index,
                    index_docs: &build_docs,
                    probe_keys: &probe_key_refs,
                    join_type: &join_type,
                    limit: usize::MAX,
                    probe_collection: &probe_collection,
                    index_collection: &index_collection,
                    join_filters: &[],
                    emit_unmatched_right,
                });
                results.append(&mut part);
                continue;
            }

            // Oversized build side. If we cannot split further, the skew is
            // unsplittable (identical-key) — surface a deterministic error rather
            // than OOM by materializing it.
            if item.depth >= MAX_DEPTH {
                return Err(crate::Error::MemoryExhausted {
                    engine: "grace-join".into(),
                });
            }

            // RE-PARTITION: re-hash both sides with a FRESH seed into SUB_P sub-
            // partitions, streaming each spill file frame-by-frame (never whole-
            // file). The seed is derived from depth so build and probe use the
            // SAME seed (matching rows co-locate). Each level's outputs live in a
            // distinct sub-dir under `spill_dir`.
            let new_seed = new_seed_for(item.depth);
            let sub_dir = spill_dir.join(format!("rp-{next_repartition_id}-d{}", item.depth + 1));
            next_repartition_id += 1;

            let build_sub_paths = repartition_side(
                item.build,
                &build_key_refs,
                new_seed,
                SUB_P,
                &sub_dir,
                "build",
            )?;
            let probe_sub_paths = repartition_side(
                item.probe,
                &probe_key_refs,
                new_seed,
                SUB_P,
                &sub_dir,
                "probe",
            )?;

            // Enqueue SUB_P new items at depth+1. `repartition_side` always
            // returns exactly SUB_P paths each, positionally aligned by sub-
            // partition index, so the zip pairs build/probe sub-partitions that
            // share a seed bucket.
            for (bp, pp) in build_sub_paths.into_iter().zip(probe_sub_paths) {
                queue.push(WorkItem {
                    build: PartitionSource::Spilled(bp),
                    probe: PartitionSource::Spilled(pp),
                    depth: item.depth + 1,
                });
            }
        }

        results.truncate(limit);
        Ok(results)
    }
}

/// One unit of join work: a build + probe (sub-)partition pair sharing a hash
/// seed, plus the recursion depth that produced it.
struct WorkItem {
    build: PartitionSource,
    probe: PartitionSource,
    /// Re-partition depth: 0 for top-level partitions, +1 per re-partition.
    depth: u32,
}

/// Seed for re-partition `depth → depth+1`. A small deterministic sequence keyed
/// on depth: distinct from seed 0 (top-level) and distinct per level, so each
/// re-partition genuinely re-hashes distinct keys into new buckets.
fn new_seed_for(depth: u32) -> u64 {
    // `+1` so the first re-partition (depth 0 → 1) never reuses the top-level
    // seed 0. Odd multiplier spreads the per-depth seeds apart.
    (depth as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Convert a top-level partition's `(spiller, in_mem)` pair into a
/// [`PartitionSource`].
///
/// A spilled partition is `finish()`ed to its on-disk path (a spilled source);
/// a non-spilled partition's already-resident rows become an in-memory source
/// (≤budget by construction, since it never crossed the spill threshold).
fn side_source(
    spiller: Option<SpillPartitionWriter>,
    in_mem: Vec<(String, Vec<u8>)>,
) -> crate::Result<PartitionSource> {
    match spiller {
        Some(writer) => Ok(PartitionSource::Spilled(writer.finish()?)),
        None => Ok(PartitionSource::InMemory(in_mem)),
    }
}

/// Push one row into `side` partition `p`, spilling the partition if it grows
/// past `budget` (and a writer can be created).
fn push_row(
    side: &mut SideState,
    p: usize,
    value: &[u8],
    budget: usize,
    spill_dir: &std::path::Path,
    side_tag: &str,
) -> crate::Result<()> {
    // Already spilling → append straight to disk, no RAM growth.
    if let Some(w) = side.spillers[p].as_mut() {
        w.append_row(value)?;
        return Ok(());
    }

    // In-memory path.
    side.buffers[p].push((String::new(), value.to_vec()));
    side.bytes[p] += value.len();

    // Budget == 0 means "never spill" (pure in-memory).
    if budget == 0 || side.bytes[p] <= budget {
        return Ok(());
    }

    // Over budget — try to start spilling this partition.
    let path = spill_dir.join(format!("p{p}.{side_tag}.spill"));
    match SpillPartitionWriter::create(&path) {
        Some(mut w) => {
            // Drain everything currently buffered into the writer, then free RAM.
            for (_, row) in side.buffers[p].drain(..) {
                w.append_row(&row)?;
            }
            side.bytes[p] = 0;
            side.spillers[p] = Some(w);
        }
        None => {
            // io_uring unavailable (e.g. non-Linux): keep the partition in
            // memory. Correctness holds; only the memory bound is lost. This is
            // a platform capability gap, not an error — fall through silently.
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A side of a join: `(doc_id, raw msgpack bytes)` pairs.
    type DocSet = Vec<(String, Vec<u8>)>;

    /// Build a msgpack map fixture via the same helper the other join tests use
    /// (`nodedb_types::json_to_msgpack`), NOT serde_json directly.
    fn msgpack_row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).unwrap()
    }

    /// Sort a result set so it can be compared as a MULTISET (duplicates
    /// preserved, order irrelevant).
    fn as_multiset(mut rows: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        rows.sort();
        rows
    }

    /// Single-key fixtures: matches, non-matches, DUPLICATE keys (count must be
    /// preserved), and a row MISSING the key field on each side.
    fn single_key_fixtures() -> (DocSet, DocSet) {
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

    /// Drive a `PartitionedSpiller` end to end for one join, returning its
    /// output. Each side's rows are fed one at a time via push_*.
    fn run_spiller(
        build: &[(String, Vec<u8>)],
        probe: &[(String, Vec<u8>)],
        partitions: usize,
        per_partition_budget: usize,
        materialize_cap: usize,
        spill_dir: PathBuf,
        spec: &GraceSpec,
    ) -> Vec<Vec<u8>> {
        let mut spiller = PartitionedSpiller::new(
            spec,
            partitions,
            per_partition_budget,
            materialize_cap,
            spill_dir,
        );
        for (_, v) in build {
            spiller.push_build(v).unwrap();
        }
        for (_, v) in probe {
            spiller.push_probe(v).unwrap();
        }
        spiller.finish_and_probe().unwrap()
    }

    /// Like [`run_spiller`] but returns the raw `Result` so tests can assert the
    /// depth-cap error path without unwrapping.
    fn run_spiller_result(
        build: &[(String, Vec<u8>)],
        probe: &[(String, Vec<u8>)],
        partitions: usize,
        per_partition_budget: usize,
        materialize_cap: usize,
        spill_dir: PathBuf,
        spec: &GraceSpec,
    ) -> crate::Result<Vec<Vec<u8>>> {
        let mut spiller = PartitionedSpiller::new(
            spec,
            partitions,
            per_partition_budget,
            materialize_cap,
            spill_dir,
        );
        for (_, v) in build {
            spiller.push_build(v)?;
        }
        for (_, v) in probe {
            spiller.push_probe(v)?;
        }
        spiller.finish_and_probe()
    }

    // Round-trip + spill exercises io_uring → gate on Linux, mirroring spill.rs.
    #[cfg(target_os = "linux")]
    mod io_tests {
        use super::super::super::grace_partitioner::grace_join_in_memory;
        use super::*;

        /// budget = 1 forces EVERY partition to spill; assert the spilling path
        /// is multiset-equivalent to the in-memory reference for every join
        /// type and a couple of partition counts.
        #[test]
        fn spilling_matches_in_memory_reference_all_join_types() {
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
                // partitions=4 matches the reference's own clamp rules
                let want =
                    as_multiset(grace_join_in_memory(build.clone(), probe.clone(), 4, &spec));

                for p in [1usize, 4] {
                    let dir = tempfile::tempdir().unwrap();
                    let got = run_spiller(
                        &build,
                        &probe,
                        p,
                        /* per_partition_budget */ 1, // force spill to disk
                        // materialize_cap large → NO re-partition. This test is a
                        // spill→materialize→probe correctness oracle (== in-memory
                        // reference), not a re-partition test.
                        /* materialize_cap */
                        64 * 1024 * 1024,
                        dir.path().to_path_buf(),
                        &spec,
                    );
                    assert_eq!(
                        want,
                        as_multiset(got),
                        "SPILLING join_type={jt} partitions={p} multiset mismatch"
                    );
                }
            }
        }

        /// Composite-key spilling equivalence for inner + left.
        #[test]
        fn spilling_matches_reference_composite_key() {
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
                let want =
                    as_multiset(grace_join_in_memory(build.clone(), probe.clone(), 4, &spec));
                for p in [1usize, 4] {
                    let dir = tempfile::tempdir().unwrap();
                    // budget=1 forces spill; materialize_cap large → no re-partition.
                    // Spill→materialize→probe correctness oracle for composite keys.
                    let got = run_spiller(
                        &build,
                        &probe,
                        p,
                        1,
                        64 * 1024 * 1024,
                        dir.path().to_path_buf(),
                        &spec,
                    );
                    assert_eq!(
                        want,
                        as_multiset(got),
                        "SPILLING composite join_type={jt} partitions={p}"
                    );
                }
            }
        }

        /// Non-spilling path (huge budget) must also match the reference — this
        /// confirms the pure in-memory branch of the spiller is equivalent.
        #[test]
        fn non_spilling_matches_in_memory_reference() {
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
                let want =
                    as_multiset(grace_join_in_memory(build.clone(), probe.clone(), 4, &spec));
                for p in [1usize, 4] {
                    let dir = tempfile::tempdir().unwrap();
                    let got = run_spiller(
                        &build,
                        &probe,
                        p,
                        /* per_partition_budget */ 64 * 1024 * 1024, // never spill
                        /* materialize_cap */ 64 * 1024 * 1024, // never re-partition
                        dir.path().to_path_buf(),
                        &spec,
                    );
                    assert_eq!(
                        want,
                        as_multiset(got),
                        "NON-SPILLING join_type={jt} partitions={p} multiset mismatch"
                    );
                }
            }
        }

        /// SKEWED build with DISTINCT keys forced into ONE top-level partition
        /// (partitions=1) under a tiny per-partition budget so the partition
        /// spills and is oversized → the work queue RE-PARTITIONS it. With
        /// distinct keys the re-partition genuinely splits the data, so the join
        /// must COMPLETE with the full, correct result (multiset-equal to the
        /// in-memory reference). This is the regression guard against the old
        /// whole-partition materialization OOM.
        #[test]
        fn skewed_distinct_keys_completes_via_repartition() {
            // 200 distinct build keys, each matching one probe key → 200 inner
            // matches. partitions=1 funnels them all into one partition; a 1-byte
            // budget forces that partition to spill and be oversized, triggering
            // recursive re-partition.
            const N: i64 = 200;
            let build: Vec<(String, Vec<u8>)> = (0..N)
                .map(|k| {
                    (
                        format!("b{k}"),
                        msgpack_row(&[
                            ("k", serde_json::json!(k)),
                            ("rv", serde_json::json!(format!("r{k}"))),
                        ]),
                    )
                })
                .collect();
            let probe: Vec<(String, Vec<u8>)> = (0..N)
                .map(|k| {
                    (
                        format!("p{k}"),
                        msgpack_row(&[
                            ("k", serde_json::json!(k)),
                            ("lv", serde_json::json!(format!("l{k}"))),
                        ]),
                    )
                })
                .collect();

            let build_keys = ["k"];
            let probe_keys = ["k"];

            for jt in ["inner", "left", "right", "full"] {
                let spec = GraceSpec {
                    build_keys: &build_keys,
                    probe_keys: &probe_keys,
                    join_type: jt,
                    limit: usize::MAX,
                    probe_collection: "l",
                    index_collection: "r",
                    emit_unmatched_right: true,
                };
                let want =
                    as_multiset(grace_join_in_memory(build.clone(), probe.clone(), 1, &spec));
                let dir = tempfile::tempdir().unwrap();
                // materialize_cap sizing (the property under test is RE-PARTITION
                // that COMPLETES). Each build row is a small msgpack map
                // (`{k: int, rv: "rN"}`, ~15-20 bytes); on disk a Spilled source's
                // `size_bytes` is the file length = Σ (4-byte frame header + row
                // bytes). For N=200 that whole single partition is ≈ 200 × ~22 ≈
                // ~4.4 KiB. We pick 1024:
                //   (a) < ~4.4 KiB whole-partition bytes  → top-level partition is
                //       oversized → it RE-PARTITIONS its 200 DISTINCT keys; and
                //   (b) ≫ one sub-partition's bytes: SUB_P=16 spreads ~200 distinct
                //       keys to ~12-13 rows (~290 bytes) per sub-partition, well
                //       under 1024 → every sub-partition fits at depth 1, far below
                //       MAX_DEPTH → the join COMPLETES with the full correct result.
                let got = run_spiller(
                    &build,
                    &probe,
                    /* partitions */ 1,
                    /* per_partition_budget */ 1, // force spill + oversize
                    /* materialize_cap */ 1024,
                    dir.path().to_path_buf(),
                    &spec,
                );
                assert_eq!(
                    want.len(),
                    N as usize,
                    "fixture sanity: expected {N} matches for inner-style join_type={jt}"
                );
                assert_eq!(
                    want,
                    as_multiset(got),
                    "SKEWED-DISTINCT join_type={jt}: re-partition must produce the full result"
                );
            }
        }

        /// IDENTICAL-key skew: every build (and probe) row carries the SAME join
        /// key, so it is unsplittable at every seed/depth. Under a tiny budget the
        /// single partition spills, is oversized, and re-partition keeps routing
        /// every row into one sub-partition forever → the depth cap MUST surface
        /// `MemoryExhausted` rather than OOM or panic.
        #[test]
        fn identical_key_skew_hits_depth_cap_error() {
            // Many rows, all key=1. Enough that the partition stays oversized
            // past every re-partition level (each row > the 1-byte budget on its
            // own, so the sub-partition can never fit).
            const N: i64 = 500;
            let build: Vec<(String, Vec<u8>)> = (0..N)
                .map(|i| {
                    (
                        format!("b{i}"),
                        msgpack_row(&[
                            ("k", serde_json::json!(1)),
                            ("rv", serde_json::json!(format!("r{i}"))),
                        ]),
                    )
                })
                .collect();
            let probe: Vec<(String, Vec<u8>)> = (0..N)
                .map(|i| {
                    (
                        format!("p{i}"),
                        msgpack_row(&[
                            ("k", serde_json::json!(1)),
                            ("lv", serde_json::json!(format!("l{i}"))),
                        ]),
                    )
                })
                .collect();

            let build_keys = ["k"];
            let probe_keys = ["k"];
            let spec = GraceSpec {
                build_keys: &build_keys,
                probe_keys: &probe_keys,
                join_type: "inner",
                limit: usize::MAX,
                probe_collection: "l",
                index_collection: "r",
                emit_unmatched_right: true,
            };

            let dir = tempfile::tempdir().unwrap();
            // materialize_cap smaller than the whole identical-key partition
            // (N=500 rows × ~26 framed bytes ≈ ~13 KiB). 1024 keeps the partition
            // oversized; because every row carries the SAME key it is unsplittable
            // — re-partition routes all rows into one sub-partition at every seed,
            // so it stays > 1024 at every level → MAX_DEPTH → MemoryExhausted.
            let result = run_spiller_result(
                &build,
                &probe,
                /* partitions */ 1,
                /* per_partition_budget */ 1,
                /* materialize_cap */ 1024,
                dir.path().to_path_buf(),
                &spec,
            );
            match result {
                Err(crate::Error::MemoryExhausted { engine }) => {
                    assert_eq!(engine, "grace-join", "depth-cap error engine tag");
                }
                other => panic!(
                    "identical-key skew must hit the depth cap with MemoryExhausted, got {other:?}"
                ),
            }
        }
    }
}
