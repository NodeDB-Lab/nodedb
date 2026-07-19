// SPDX-License-Identifier: BUSL-1.1

//! Node-local consumer entry point for cross-node shuffle joins.
//!
//! Per the "receive-to-spill, then local grace join" design (D1), a distributed
//! shuffle join stages each side's rows to a LOCAL scratch file on the consumer
//! node, then runs the EXISTING grace-hash join over those local files. This
//! module is the node-local half: it takes two already-staged frame files (a
//! build/right file and a probe/left file), wraps each in a
//! [`RowSource::ShuffleStream`], and drives the SAME generalized grace machinery
//! the local-join path uses ([`CoreLoop::drive_grace_build`] +
//! [`CoreLoop::finish_grace_join`]). No build/probe logic is duplicated.
//!
//! The transport → staging wiring (writing those files from the shuffle inbox,
//! see `crate::control::server::shuffle`) and the cross-node end-to-end test
//! live in SEPARATE units. This module is validated node-locally by writing
//! staged files directly (see the in-file tests) — it never touches the shuffle
//! transport, the inbox, or `nodedb-cluster`.
//!
//! ## Staged-file format
//!
//! Each staged file is a sequence of `[u32 LE len][row-bytes]` frames, ONE join
//! row per frame, each row a single msgpack document with the SAME per-row byte
//! shape `scan_collection_for_each` yields. This is exactly the framing
//! [`super::spill::SpillPartitionWriter`] produces and
//! [`super::grace_repartition::FrameStreamReader`] consumes, so the staged-row
//! read path is byte-identical to the grace spill-read path.

use std::path::PathBuf;

use super::grace_drive::GraceSources;
use super::grace_partitioner::GraceSpec;
use super::params::JoinParams;
use super::row_source::RowSource;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;

/// Immutable inputs to a node-local shuffle-join completion: the two staged
/// frame files (build/right and probe/left) plus the collection/alias names used
/// to qualify emitted columns.
///
/// Bundled into one struct so [`CoreLoop::execute_shuffle_join`] stays within the
/// argument-count limit. The `*_qualifier` fields are the prefixes the grace
/// emission code uses to namespace columns from each side (the same role
/// `probe_collection` / `index_collection` play on the local path).
pub(in crate::data::executor) struct ShuffleJoinInputs {
    /// Staged file holding the BUILD (right) side rows.
    pub build_path: PathBuf,
    /// Staged file holding the PROBE (left) side rows.
    pub probe_path: PathBuf,
    /// Column qualifier for probe-side (left) columns.
    pub probe_qualifier: String,
    /// Column qualifier for build-side (right/index) columns.
    pub index_qualifier: String,
}

impl CoreLoop {
    /// Run a cross-node shuffle join's node-local half: drive the grace-hash
    /// join over two LOCAL staged frame files.
    ///
    /// Both sides are fed as [`RowSource::ShuffleStream`] over the staged files
    /// in `inputs`; the SAME [`CoreLoop::finish_grace_join`] tail the local path
    /// uses then drives the build + probe and produces the final response — so
    /// the output is byte-identical to the equivalent local grace join of the
    /// same rows.
    ///
    /// Returns the same `Response` shape every grace-join path returns:
    /// - the encoded join rows on success;
    /// - a deterministic `ResourcesExhausted` for an over-budget skew error or a
    ///   no-LIMIT output that fills the budget ceiling;
    /// - `Internal` for any other driver/encode error.
    ///
    /// Cross / keyless joins are NOT handled here (the same declared deferral the
    /// local path carries): a cartesian product cannot be hash-partitioned by
    /// key, and the streamed-probe path needs join keys to build an index. The
    /// caller MUST only route equi-joins through this entry point; a keyless
    /// `join.on` surfaces a deterministic `Internal` error rather than silently
    /// producing wrong results.
    pub(in crate::data::executor) fn execute_shuffle_join(
        &self,
        join: &JoinParams<'_>,
        inputs: ShuffleJoinInputs,
        budget: usize,
    ) -> Response {
        // Probe-side (left) and build-side (right) join-key field names. Same
        // extraction as the local path's `try_grace_hash_join`.
        let probe_keys: Vec<&str> = join.on.iter().map(|(l, _)| l.as_str()).collect();
        let build_keys: Vec<&str> = join.on.iter().map(|(_, r)| r.as_str()).collect();

        // Cross / keyless join: not handled here (declared deferral, identical to
        // the local path). The caller routes only equi-joins through this entry;
        // a keyless `on` is a routing bug, so surface it loudly rather than
        // hash-partitioning a cartesian product (which would be wrong).
        if join.join_type == "cross" || build_keys.is_empty() || probe_keys.is_empty() {
            return self.response_error(
                join.task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: "shuffle join requires equi-join keys; cross/keyless join must not be \
                             routed to the shuffle-join consumer"
                        .into(),
                },
            );
        }

        // Identical output-bound derivation to the local path: an explicit user
        // LIMIT is honored exactly (no budget check); a no-LIMIT join is bounded
        // by the per-query byte budget (or truly unbounded when 0).
        let (probe_limit, enforce_output_budget) = if join.limit != usize::MAX {
            (join.limit, false)
        } else if budget == 0 {
            (usize::MAX, false)
        } else {
            (
                crate::data::executor::handlers::scan_budget::fetch_limit_for(
                    usize::MAX,
                    0,
                    budget,
                ),
                true,
            )
        };

        let spec = GraceSpec {
            build_keys: &build_keys,
            probe_keys: &probe_keys,
            join_type: join.join_type,
            limit: probe_limit,
            probe_collection: &inputs.probe_qualifier,
            index_collection: &inputs.index_qualifier,
            // Matches the local path: emit unmatched build-side rows for
            // RIGHT/FULL (no broadcast de-duplication here).
            emit_unmatched_right: true,
        };

        let sources = GraceSources {
            build: RowSource::ShuffleStream {
                path: inputs.build_path,
            },
            probe: RowSource::ShuffleStream {
                path: inputs.probe_path,
            },
        };

        let unique_join_id = join.task.request_id().as_u64();
        self.finish_grace_join(
            join,
            sources,
            &spec,
            budget,
            unique_join_id,
            enforce_output_budget,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{Priority, Request, Status};
    use crate::data::executor::handlers::join::grace_partitioner::{
        GraceSpec, grace_join_in_memory,
    };
    use crate::data::executor::task::ExecutionTask;
    use crate::types::*;
    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use std::io::Write as _;
    use std::time::{Duration, Instant};

    /// Build a `CoreLoop` over a temp `data_dir` (mirrors the core_loop tests).
    fn make_core() -> (CoreLoop, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("open core");
        (core, dir)
    }

    /// A minimal `ExecutionTask`; `finish_grace_join` reads only `task.request`
    /// (database_id, request_id) and the `JoinParams` fields — never executes the
    /// embedded plan.
    fn make_task() -> ExecutionTask {
        let request = Request {
            request_id: RequestId::new(7),
            tenant_id: TenantId::new(0),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "t".into(),
                document_id: "d".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            deadline: Instant::now() + Duration::from_secs(30),
            priority: Priority::Normal,
            trace_id: TraceId::generate(),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        };
        ExecutionTask::new(request)
    }

    /// A msgpack map row, built via the same helper the join tests use.
    fn row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).expect("encode row")
    }

    /// Write `rows` as `[u32 LE len][row-bytes]` frames (one join row per frame)
    /// — the staged-file format the shuffle consumer reads.
    fn write_staged(path: &std::path::Path, rows: &[Vec<u8>]) {
        let mut f = std::fs::File::create(path).expect("create staged file");
        for r in rows {
            let len = u32::try_from(r.len()).expect("row fits u32");
            f.write_all(&len.to_le_bytes()).expect("write len");
            f.write_all(r).expect("write body");
        }
        f.flush().expect("flush");
    }

    /// Build/probe fixtures with a known match, a known non-match on each side,
    /// and a duplicate key (so multiset count matters).
    fn fixtures() -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        // BUILD (right) side.
        let build = vec![
            row(&[("k", serde_json::json!(1)), ("rv", serde_json::json!("r1"))]),
            row(&[
                ("k", serde_json::json!(1)),
                ("rv", serde_json::json!("r1b")),
            ]), // dup key
            row(&[("k", serde_json::json!(2)), ("rv", serde_json::json!("r2"))]),
            row(&[("k", serde_json::json!(9)), ("rv", serde_json::json!("r9"))]), // no probe match
        ];
        // PROBE (left) side.
        let probe = vec![
            row(&[("k", serde_json::json!(1)), ("lv", serde_json::json!("l1"))]), // matches r1,r1b
            row(&[("k", serde_json::json!(2)), ("lv", serde_json::json!("l2"))]), // matches r2
            row(&[("k", serde_json::json!(7)), ("lv", serde_json::json!("l7"))]), // no build match
        ];
        (build, probe)
    }

    fn spec<'a>(
        build_keys: &'a [&'a str],
        probe_keys: &'a [&'a str],
        join_type: &'a str,
    ) -> GraceSpec<'a> {
        GraceSpec {
            build_keys,
            probe_keys,
            join_type,
            limit: usize::MAX,
            probe_collection: "l",
            index_collection: "r",
            emit_unmatched_right: true,
        }
    }

    fn as_multiset(mut rows: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        rows.sort();
        rows
    }

    /// Generate `n` rows with key `i % key_mod` (so keys repeat, spanning many
    /// grace partitions) and a distinct value. Used to build a dataset large
    /// enough that a `build_bytes / 3` budget reliably forces the build side to
    /// spill while every partition still fits under `materialize_cap`.
    fn gen_rows(n: usize, key_mod: i64) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| {
                row(&[
                    ("k", serde_json::json!((i as i64) % key_mod)),
                    ("v", serde_json::json!(format!("val-{i}"))),
                ])
            })
            .collect()
    }

    fn total_bytes(rows: &[Vec<u8>]) -> usize {
        rows.iter().map(|r| r.len()).sum()
    }

    /// Driving the grace machinery over `ShuffleStream` sources produces the SAME
    /// (multiset) result as the in-memory reference join of the same rows — for
    /// inner/left/right/full — at both an ample budget (no spill) and a tiny
    /// budget (forces the build side to spill). This is the core equivalence
    /// `execute_shuffle_join` relies on.
    #[test]
    fn shuffle_grace_matches_reference_inmemory_join() {
        let (core, _dir) = make_core();
        let tmp = tempfile::tempdir().expect("staging dir");
        let build_path = tmp.path().join("build.frames");
        let probe_path = tmp.path().join("probe.frames");

        // A dataset large enough that `build_bytes / 3` forces the build side
        // to spill across grace partitions while every partition still fits
        // under `materialize_cap` (= budget), so the join COMPLETES via the
        // spill path. Keys repeat (`% 40`) so matches, duplicates, and
        // unmatched rows on both sides all occur.
        let build = gen_rows(120, 40);
        let probe = gen_rows(120, 40);
        write_staged(&build_path, &build);
        write_staged(&probe_path, &probe);

        let build_keys = ["k"];
        let probe_keys = ["k"];
        // Forces the build to spill (total > budget) while leaving each
        // partition comfortably under the cap.
        let spill_budget = (total_bytes(&build) / 3).max(1);

        for jt in ["inner", "left", "right", "full"] {
            let s = spec(&build_keys, &probe_keys, jt);

            // Reference: the owned-Vec in-memory grace oracle.
            let want = as_multiset(grace_join_in_memory(
                build.iter().map(|r| (String::new(), r.clone())).collect(),
                probe.iter().map(|r| (String::new(), r.clone())).collect(),
                64,
                &s,
            ));

            // budget 0 = unlimited (in-memory build); spill_budget = build
            // spills to disk + re-partitions, then completes. BOTH must equal
            // the in-memory reference (multiset).
            for budget in [0usize, spill_budget] {
                let sources = GraceSources {
                    build: RowSource::ShuffleStream {
                        path: build_path.clone(),
                    },
                    probe: RowSource::ShuffleStream {
                        path: probe_path.clone(),
                    },
                };
                let got = core
                    .drive_grace_build(sources, &s, budget, 100 + budget as u64)
                    .expect("shuffle grace join completes");
                assert_eq!(
                    want,
                    as_multiset(got),
                    "shuffle grace must equal reference: join_type={jt} budget={budget}"
                );
            }
        }
    }

    /// A budget below a single row's size is infeasible: a one-row grace
    /// partition cannot be re-partitioned below the cap, so after the depth cap
    /// the driver returns a DETERMINISTIC [`crate::Error::MemoryExhausted`] —
    /// never an OOM, never a silent truncation. This is the SAME guarantee the
    /// local grace path makes; the test proves it carries to the shuffle
    /// consumer's staged-file input.
    #[test]
    fn shuffle_grace_infeasible_budget_is_deterministic_error() {
        let (core, _dir) = make_core();
        let tmp = tempfile::tempdir().expect("staging dir");
        let build_path = tmp.path().join("b.frames");
        let probe_path = tmp.path().join("p.frames");
        let (build, probe) = fixtures();
        write_staged(&build_path, &build);
        write_staged(&probe_path, &probe);

        let build_keys = ["k"];
        let probe_keys = ["k"];
        let s = spec(&build_keys, &probe_keys, "inner");
        let sources = GraceSources {
            build: RowSource::ShuffleStream { path: build_path },
            probe: RowSource::ShuffleStream { path: probe_path },
        };
        // 4 bytes is below one msgpack row → cannot be satisfied.
        let got = core.drive_grace_build(sources, &s, 4, 999);
        assert!(
            matches!(got, Err(crate::Error::MemoryExhausted { .. })),
            "an infeasible (sub-row) budget must surface a deterministic \
             MemoryExhausted, got a different outcome"
        );
    }

    /// End-to-end: `execute_shuffle_join` returns a successful `Response` whose
    /// payload encodes the join rows. The array header's row count must equal the
    /// reference inner-join row count, and a known matched row's bytes must appear
    /// in the concatenated payload (a non-match's unique bytes must too, only for
    /// a join type that emits it).
    #[test]
    fn execute_shuffle_join_returns_ok_response_with_join_rows() {
        let (core, _dir) = make_core();
        let tmp = tempfile::tempdir().expect("staging dir");
        let build_path = tmp.path().join("b.frames");
        let probe_path = tmp.path().join("p.frames");
        let (build, probe) = fixtures();
        write_staged(&build_path, &build);
        write_staged(&probe_path, &probe);

        let build_keys = ["k"];
        let probe_keys = ["k"];
        let ref_spec = spec(&build_keys, &probe_keys, "inner");
        let reference = grace_join_in_memory(
            build.iter().map(|r| (String::new(), r.clone())).collect(),
            probe.iter().map(|r| (String::new(), r.clone())).collect(),
            64,
            &ref_spec,
        );

        let task = make_task();
        let join = JoinParams {
            task: &task,
            on: &[("k".to_string(), "k".to_string())],
            join_type: "inner",
            limit: usize::MAX,
            projection: &[],
            computed_projection_bytes: &[],
            join_filter_bytes: &[],
            post_filter_bytes: &[],
        };
        let inputs = ShuffleJoinInputs {
            build_path,
            probe_path,
            probe_qualifier: "l".into(),
            index_qualifier: "r".into(),
        };

        let resp = core.execute_shuffle_join(&join, inputs, 0);
        assert_eq!(resp.status, Status::Ok, "shuffle join must succeed");

        let payload = resp.payload.as_bytes();
        // First byte is a fixarray header (`0x90 | len`) for our small result.
        assert!(
            payload[0] & 0xf0 == 0x90,
            "payload must start with a fixarray header"
        );
        let row_count = (payload[0] & 0x0f) as usize;
        assert_eq!(
            row_count,
            reference.len(),
            "encoded row count must match the reference inner-join count"
        );

        // A known matched probe row's unique bytes appear in the concatenated
        // rows (the join emits the left columns for matched rows).
        let l1_marker = b"l1";
        assert!(
            payload.windows(l1_marker.len()).any(|w| w == l1_marker),
            "matched probe row bytes must be present in the join output"
        );
    }
}
