// SPDX-License-Identifier: BUSL-1.1

//! The streamed validate refuses exactly as the whole-record path does.
//!
//! Each case writes a record's content as chunk records into the core's WAL
//! and points a stream marker at them. The whole-record verdict runs the
//! checks the install runs over a decoded marker, in its order. The streamed
//! verdict runs at spill threshold 0, where every op spills, and at
//! `usize::MAX`, where nothing spills, each at a batch bound that puts about
//! one record in each batch and at the production bound. Every run must
//! answer the same `Ok`, or the same error code with the same text. Both
//! verdicts only read, so they run against one core.

use nodedb_physical::physical_plan::{
    BalancedDef, DocumentOp, MaterializedSumBinding, PhysicalPlan, RedoOrigin, RedoSumTargets,
    ResolvedSumTarget,
};
use nodedb_types::calvin::{EngineTag, ReadKeyIdent, VersionedReadEntry};
use nodedb_types::{
    DatabaseId, QualifiedCollection, RlsWriteCheck, StorageKey, Surrogate, TenantId,
};

use crate::bridge::envelope::{ErrorCode, Status};
use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::newest_ordinals::collection_vshard;
use crate::data::executor::core_loop::tests::{
    make_core_with_dir, make_default_task, task_with_vshard,
};
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::retention::judged_instant_ms;
use crate::data::executor::handlers::spill::cleanup::SpillKind;
use crate::data::executor::task::ExecutionTask;
use crate::data::io::uring_wal_cursor::UringWalCursor;
use crate::engine::document::store::{CollectionConfig, IndexPath};
use crate::types::{Lsn, TxnId, VShardId};
use crate::wal::manager::{RedoChunkPlacement, WalManager};
use crate::wal::redo::frame::{
    FrameBatcher, FramedRedoBuilder, RedoContentHeader, RedoFrameTag, push_frame,
};
use crate::wal::{
    RedoChunkHeader, RedoChunkRef, RedoRecord, RedoStreamId, RedoStreamReader, RedoStreamRef,
    RedoSubRecord, chunk_crc,
};

use super::super::clone_copy::clone_probe_unreadable;
use super::super::entry::CommittedRedo;
use super::super::install_refusal_tests as refusal;
use super::super::passes::{RedoTarget, final_refusal, redo_wal_record};
use super::super::streamed::unreadable;
use super::super::sub_ops::{document_ops, kv_ops};
use super::super::sync_refusal_tests as sync;
use super::super::test_commit::{doc_put_sub_record, failing_install_sub_record};
use super::super::unique_handover_tests as handover;
use super::super::validate::CommitBoundary;
use super::{StreamLimits, StreamValidateInput};

pub(in crate::data::executor::handlers::transaction::redo_apply) const TID: u64 = 1;
pub(in crate::data::executor::handlers::transaction::redo_apply) const LSN: u64 = 900;
/// Bytes of each chunk the tests write: chunks cut records apart.
const CHUNK_BYTES: usize = 97;
/// A batch bound that puts about one record in each batch.
const SMALL_BATCH_BYTES: usize = 64;

/// The batch a stream install holds by default.
fn default_batch_bytes() -> usize {
    nodedb_types::config::tuning::CalvinTuning::default().redo_stream_batch_bytes
}

/// The core's WAL, which the tests append chunk records to.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct StreamWal {
    wal: WalManager,
    next_stream: u64,
    next_key: u64,
}

impl StreamWal {
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn open(
        core: &CoreLoop,
    ) -> Self {
        Self {
            wal: WalManager::open_for_testing(&core.wal_source().dir).expect("open wal"),
            next_stream: 0,
            next_key: 0,
        }
    }

    /// The WAL the chunks go to.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn manager(
        &self,
    ) -> &WalManager {
        &self.wal
    }

    /// Append `content` as one stream's chunks, and return its reference.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn write(
        &mut self,
        content: &[u8],
    ) -> RedoStreamRef {
        self.next_stream += 1;
        let stream = RedoStreamId::Session {
            vshard: 0,
            idempotency_key: self.next_stream,
        };
        let placement = RedoChunkPlacement {
            tenant_id: TenantId::new(TID),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
        };
        let mut chunks = Vec::new();
        for (index, bytes) in (0u32..).zip(content.chunks(CHUNK_BYTES)) {
            self.next_key += 1;
            let header = RedoChunkHeader {
                stream,
                group_id: 1,
                term: 1,
                len: content.len() as u64,
                index,
            };
            let lsns = self
                .wal
                .append_redo_chunk(self.next_key, placement, header, bytes)
                .expect("append chunk");
            chunks.push(RedoChunkRef {
                index,
                first_lsn: lsns.first.as_u64(),
                last_lsn: lsns.last.as_u64(),
                len: bytes.len() as u64,
                crc: chunk_crc(bytes),
            });
        }
        self.wal.sync().expect("sync wal");
        RedoStreamRef {
            stream,
            len: content.len() as u64,
            chunks,
            publish_position: None,
            entry: None,
            stamp_floor: None,
        }
    }
}

pub(in crate::data::executor::handlers::transaction::redo_apply) fn record(
    ops: Vec<RedoSubRecord>,
) -> RedoRecord {
    RedoRecord {
        version: 1,
        ops,
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
        sum_targets: Vec::new(),
    }
}

/// `redo` as framed content: the header, the publishes and the reads,
/// then the row sources, the ops and the row changes.
pub(in crate::data::executor::handlers::transaction::redo_apply) fn framed(
    redo: &RedoRecord,
    sum_targets: &[RedoSumTargets],
    reads: &[VersionedReadEntry],
) -> Vec<u8> {
    let mut out = Vec::new();
    let header = RedoContentHeader {
        version: redo.version,
        calvin_stamp: redo.calvin_stamp.clone(),
        cross_shard_applied: redo.cross_shard_applied.clone(),
        user_roles: redo.user_roles.clone(),
        sum_targets: sum_targets.to_vec(),
    };
    push_frame(&mut out, RedoFrameTag::Header, &header).expect("header");
    for publish in &redo.publishes {
        push_frame(&mut out, RedoFrameTag::Publish, publish).expect("publish");
    }
    for read in reads {
        push_frame(&mut out, RedoFrameTag::Read, read).expect("read");
    }
    for source in &redo.row_sources {
        push_frame(&mut out, RedoFrameTag::RowSource, source).expect("row source");
    }
    for op in &redo.ops {
        push_frame(&mut out, RedoFrameTag::Op, op).expect("op");
    }
    for change in &redo.row_changes {
        push_frame(&mut out, RedoFrameTag::RowChange, change).expect("row change");
    }
    out
}

fn framed_ops(ops: Vec<RedoSubRecord>) -> Vec<u8> {
    framed(&record(ops), &[], &[])
}

pub(in crate::data::executor::handlers::transaction::redo_apply) fn default_task() -> ExecutionTask
{
    let mut task = make_default_task();
    task.wal_lsn = Some(Lsn::new(LSN));
    task
}

/// How one case validates.
struct Run {
    task: ExecutionTask,
    origin: RedoOrigin,
    snapshot: Option<i64>,
    stamp_floor: Option<i64>,
}

impl Run {
    fn commit() -> Self {
        Self {
            task: default_task(),
            origin: RedoOrigin::Commit,
            snapshot: None,
            stamp_floor: None,
        }
    }
}

/// The record the stream marker `marker` installs as, read whole back from
/// the core's WAL, and the sum targets and reads its content carries. A read
/// that fails is retryable. Content that does not decode refuses alike on
/// every replica.
fn decoded_content(
    core: &CoreLoop,
    marker: &RedoRecord,
    reference: &RedoStreamRef,
) -> Result<(RedoRecord, Vec<RedoSumTargets>, Vec<VersionedReadEntry>), ErrorCode> {
    let refuse = |error: crate::Error| final_refusal(error.into());
    let reader = RedoStreamReader::<UringWalCursor>::open(core.wal_source(), reference.clone())
        .map_err(|error| unreadable(reference, error))?;
    let mut batcher = FrameBatcher::new(reader, default_batch_bytes());
    let mut builder = FramedRedoBuilder::default();
    while let Some(batch) = batcher
        .next_batch()
        .map_err(|error| unreadable(reference, error))?
    {
        builder.push(&batch).map_err(refuse)?;
    }
    let mut framed = builder.finish().map_err(refuse)?;
    let sum_targets = std::mem::take(&mut framed.header.sum_targets);
    let reads = std::mem::take(&mut framed.reads);
    let redo = framed.into_installed(marker, reference).map_err(refuse)?;
    Ok((redo, sum_targets, reads))
}

/// The checks `install_redo_into` runs over a marker's decoded content
/// before it writes, in its order.
fn whole_verdict(
    core: &mut CoreLoop,
    run: &Run,
    marker: &RedoRecord,
    reference: &RedoStreamRef,
    judged_at_ms: i64,
) -> Result<(), ErrorCode> {
    let task = &run.task;
    let (redo, sum_targets, reads) = decoded_content(core, marker, reference)?;
    let bytes = redo
        .to_bytes()
        .map_err(|error| final_refusal(error.into()))?;
    let database_id = task.request.database_id.as_u64();
    let doc_ops = core
        .without_skipped_clone_copies(database_id, TID, document_ops(&redo.ops))
        .map_err(clone_probe_unreadable)?;
    let kv = kv_ops(&redo.ops);
    core.check_commit_boundary(&CommitBoundary {
        database_id,
        tid: TID,
        documents: &doc_ops,
        kv: &kv,
        sum_targets: &sum_targets,
        origin: run.origin,
        user_roles: &redo.user_roles,
        judged_at_ms,
    })
    .map_err(ErrorCode::from)?;
    let record = redo_wal_record(task, Lsn::new(LSN), bytes)?;
    let target = RedoTarget {
        record: &record,
        sub_records: redo.ops.len(),
        database_id,
        tid: TID,
        origin: run.origin,
    };
    core.validate_redo_pass(&target, &sum_targets)
        .map_err(|refusal| refusal.into_code())?;
    if let Some(ordinal) = run.snapshot {
        core.check_redo_snapshot(task, TID, &reads, &redo.ops, ordinal)?;
    }
    Ok(())
}

fn streamed_verdict(
    core: &mut CoreLoop,
    run: &Run,
    marker: &RedoRecord,
    reference: &RedoStreamRef,
    judged_at_ms: i64,
    limits: StreamLimits,
) -> Result<(), ErrorCode> {
    core.validate_redo_stream(&StreamValidateInput {
        task: &run.task,
        tid: TID,
        marker,
        reference,
        origin: run.origin,
        snapshot: run.snapshot,
        judged_at_ms,
        lsn: Lsn::new(LSN),
        limits,
    })
    .map(drop)
}

/// Write `content` as a stream, assert every streamed run answers as the
/// whole-record path, and return that answer.
fn parity(
    core: &mut CoreLoop,
    wal: &mut StreamWal,
    content: &[u8],
    run: &Run,
) -> Result<(), ErrorCode> {
    let mut reference = wal.write(content);
    reference.stamp_floor = run.stamp_floor;
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    let judged_at_ms = judged_instant_ms(None, run.task.request.system_ordinal);
    let whole = whole_verdict(core, run, &marker, &reference, judged_at_ms);
    for spill_threshold in [0, usize::MAX] {
        for batch_bytes in [SMALL_BATCH_BYTES, default_batch_bytes()] {
            let limits = StreamLimits {
                batch_bytes,
                spill_threshold,
            };
            let streamed = streamed_verdict(core, run, &marker, &reference, judged_at_ms, limits);
            assert_eq!(
                format!("{streamed:?}"),
                format!("{whole:?}"),
                "spill threshold {spill_threshold}, batch bytes {batch_bytes}"
            );
        }
    }
    whole
}

fn refusal_text(verdict: &Result<(), ErrorCode>) -> String {
    match verdict {
        Err(code) => format!("{code:?}"),
        Ok(()) => panic!("expected a refusal"),
    }
}

fn is_unique_violation(verdict: &Result<(), ErrorCode>) -> bool {
    matches!(
        verdict,
        Err(ErrorCode::RejectedConstraint { constraint, .. }) if constraint == "unique"
    )
}

/// A core with its bridge ends, data directory and WAL kept alive.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct Hold {
    pub(in crate::data::executor::handlers::transaction::redo_apply) core: CoreLoop,
    pub(in crate::data::executor::handlers::transaction::redo_apply) wal: StreamWal,
    _ends: Box<dyn std::any::Any>,
    _dir: tempfile::TempDir,
}

pub(in crate::data::executor::handlers::transaction::redo_apply) fn hold(
    setup: impl FnOnce(&std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>),
) -> Hold {
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, ends) = setup(dir.path());
    let wal = StreamWal::open(&core);
    Hold {
        core,
        wal,
        _ends: ends,
        _dir: dir,
    }
}

pub(in crate::data::executor::handlers::transaction::redo_apply) fn plain_core(
    dir: &std::path::Path,
) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (core, req, resp) = make_core_with_dir(dir);
    (core, Box::new((req, resp)))
}

fn seeded_unique_core(dir: &std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (core, req, resp) = handover::seeded(dir);
    (core, Box::new((req, resp)))
}

// ── install_refusal_tests ────────────────────────────────────────────────

/// Stage and resolve `plans` on `core`, and frame the record they resolve
/// to, with one sub-record more that fails at install when `refuse`.
fn plan_content(core: &mut CoreLoop, plans: &[PhysicalPlan], refuse: bool) -> Vec<u8> {
    let task = make_default_task();
    let bytes = match core.stage_and_resolve_for_test(&task, TID, plans, LSN) {
        Ok(bytes) => bytes,
        Err(response) => panic!("resolve: {:?}", response.error_code),
    };
    core.drop_overlay_entry(TxnId::new(LSN))
        .expect("drop overlay");
    let mut redo = RedoRecord::from_bytes(&bytes).expect("decode resolved redo");
    if refuse {
        redo.ops.push(failing_install_sub_record());
    }
    framed(&redo, &redo_sum_targets(plans), &[])
}

#[test]
fn every_install_refusal_record_validates_alike() {
    type Setup = fn(&mut CoreLoop);
    fn none(_: &mut CoreLoop) {}
    let cases: [(PhysicalPlan, Setup); 7] = [
        (refusal::document_insert(), none),
        (refusal::kv_predicate_delete(), refusal::seed_kv),
        (refusal::vector_direct_insert(), none),
        (refusal::crdt_upsert(), none),
        (refusal::spatial_insert(), none),
        (refusal::array_put(), refusal::open_array),
        (refusal::indexed_document_put(), refusal::declare_text),
    ];
    for (index, (plan, setup)) in cases.into_iter().enumerate() {
        for refuse in [false, true] {
            let mut held = hold(plain_core);
            setup(&mut held.core);
            let content = plan_content(&mut held.core, std::slice::from_ref(&plan), refuse);
            let verdict = parity(&mut held.core, &mut held.wal, &content, &Run::commit());
            assert!(
                verdict.is_ok(),
                "case {index}, refuse {refuse}: {verdict:?}"
            );
        }
    }
    for refuse in [false, true] {
        let mut held = hold(plain_core);
        refusal::declare_text(&mut held.core);
        let mut ops = vec![refusal::fts_index_sub_record()];
        if refuse {
            ops.push(failing_install_sub_record());
        }
        let verdict = parity(
            &mut held.core,
            &mut held.wal,
            &framed_ops(ops),
            &Run::commit(),
        );
        assert!(verdict.is_ok(), "{verdict:?}");
    }
    let mut held = hold(plain_core);
    let content = framed_ops(vec![refusal::raw_crdt_delta_sub_record()]);
    let verdict = parity(&mut held.core, &mut held.wal, &content, &Run::commit());
    assert!(
        refusal_text(&verdict).contains("raw CRDT delta"),
        "{verdict:?}"
    );
}

// ── unique_handover_tests ────────────────────────────────────────────────

#[test]
fn every_unique_handover_record_validates_alike() {
    let mut held = hold(seeded_unique_core);
    let ok = [
        vec![handover::put(1, "B"), handover::put(2, "A")],
        vec![handover::put(2, "A"), handover::put(1, "B")],
        vec![handover::put(3, "A"), handover::put(1, "X")],
    ];
    for ops in ok {
        let verdict = parity(
            &mut held.core,
            &mut held.wal,
            &framed_ops(ops),
            &Run::commit(),
        );
        assert!(verdict.is_ok(), "{verdict:?}");
    }
    let refused = [
        vec![handover::put(3, "Z"), handover::put(4, "Z")],
        vec![handover::put(3, "B")],
    ];
    for ops in refused {
        let verdict = parity(
            &mut held.core,
            &mut held.wal,
            &framed_ops(ops),
            &Run::commit(),
        );
        assert!(is_unique_violation(&verdict), "{verdict:?}");
    }
    // A staged delete releases `A` for the insert after it.
    let collection = QualifiedCollection::new(DatabaseId::DEFAULT, handover::COLL);
    let plans = [
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: collection.clone(),
            document_id: "r1".to_string(),
            surrogate: Some(Surrogate::new(1)),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        }),
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection,
            document_id: "r3".to_string(),
            value: handover::body("A"),
            if_absent: false,
            surrogate: Surrogate::new(3),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        }),
    ];
    let content = plan_content(&mut held.core, &plans, false);
    let verdict = parity(&mut held.core, &mut held.wal, &content, &Run::commit());
    assert!(verdict.is_ok(), "{verdict:?}");
}

/// Rows that claim nothing the test judges, to spread a record over
/// batches.
fn filler(from: u32, count: u32) -> Vec<RedoSubRecord> {
    (from..from + count)
        .map(|surrogate| handover::put(surrogate, &format!("filler-{surrogate}")))
        .collect()
}

#[test]
fn a_unique_swap_and_handover_across_batches_validate_alike() {
    let mut held = hold(seeded_unique_core);
    // A swap whose two halves sit batches apart.
    let mut swap = vec![handover::put(1, "B")];
    swap.extend(filler(100, 12));
    swap.push(handover::put(2, "A"));
    // A claim that a later batch's write releases.
    let mut handed = vec![handover::put(3, "A")];
    handed.extend(filler(200, 12));
    handed.push(handover::put(1, "X"));
    for ops in [swap, handed] {
        let run = Run {
            stamp_floor: Some(nodedb_types::OrdinalClock::new().next_ordinal()),
            ..Run::commit()
        };
        let verdict = parity(&mut held.core, &mut held.wal, &framed_ops(ops), &run);
        assert!(verdict.is_ok(), "{verdict:?}");
    }
    // Row 3's claim of `A` is dead once a later batch rewrites row 3, and
    // row 4's claim meets row 1, which still holds `A`.
    let mut dead = vec![handover::put(3, "A")];
    dead.extend(filler(300, 8));
    dead.push(handover::put(3, "C"));
    dead.extend(filler(400, 8));
    dead.push(handover::put(4, "A"));
    let verdict = parity(
        &mut held.core,
        &mut held.wal,
        &framed_ops(dead),
        &Run::commit(),
    );
    assert!(is_unique_violation(&verdict), "{verdict:?}");
    assert!(refusal_text(&verdict).contains("'A'"), "{verdict:?}");
}

// ── sync_refusal_tests ───────────────────────────────────────────────────

#[test]
fn every_sync_refusal_record_validates_alike() {
    let refused = [
        sync::text_record(Surrogate::ZERO, Some(sync::provenance())),
        sync::refused_vector(),
        sync::refused_spatial(false),
        sync::refused_spatial(true),
    ];
    for refused in refused {
        let mut held = hold(plain_core);
        let ops = vec![
            sync::text_record(Surrogate::new(91), None),
            sync::sibling_vector(),
            refused,
        ];
        // The refused sub-record fails at install, after the validate:
        // parity is the check here.
        let _verdict = parity(
            &mut held.core,
            &mut held.wal,
            &framed_ops(ops),
            &Run::commit(),
        );
    }
}

// ── Ranking ──────────────────────────────────────────────────────────────

const LEDGER: &str = "ledger";

/// [`seeded_unique_core`] with a `ledger` collection that declares
/// BALANCED over `j`, `t` and `a`.
fn ledger_core(dir: &std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (mut core, ends) = seeded_unique_core(dir);
    let mut config = CollectionConfig::new(LEDGER);
    config.enforcement.balanced = Some(BalancedDef {
        group_key_column: "j".into(),
        entry_type_column: "t".into(),
        debit_value: "D".into(),
        credit_value: "C".into(),
        amount_column: "a".into(),
    });
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(TID), LEDGER.to_string()),
        config,
    );
    (core, ends)
}

fn leg(surrogate: u32, group: &str, kind: &str, amount: i64) -> RedoSubRecord {
    let body = doc_format::encode_to_msgpack(&serde_json::json!({
        "j": group,
        "t": kind,
        "a": amount,
    }));
    doc_put_sub_record(LEDGER, &format!("l{surrogate}"), &body, surrogate)
}

#[test]
fn ranking_names_the_error_the_whole_record_names() {
    let mut held = hold(ledger_core);
    // `codes` sorts before `ledger`, and a collection check ranks before
    // the arms. Within `codes`, row 4 is the first second owner: of `Z`.
    let ops = vec![
        leg(50, "g1", "D", 10),
        handover::put(5, "Y"),
        handover::put(3, "Z"),
        refusal::raw_crdt_delta_sub_record(),
        handover::put(4, "Z"),
        handover::put(6, "Y"),
    ];
    let verdict = parity(
        &mut held.core,
        &mut held.wal,
        &framed_ops(ops),
        &Run::commit(),
    );
    assert!(is_unique_violation(&verdict), "{verdict:?}");
    assert!(refusal_text(&verdict).contains("'Z'"), "{verdict:?}");

    // Without it, the ledger's first unbalanced group in byte order.
    let ops = vec![
        leg(51, "g2", "D", 10),
        leg(52, "g10", "D", 5),
        refusal::raw_crdt_delta_sub_record(),
    ];
    let verdict = parity(
        &mut held.core,
        &mut held.wal,
        &framed_ops(ops),
        &Run::commit(),
    );
    assert!(refusal_text(&verdict).contains("g10"), "{verdict:?}");

    // With every collection sound, the arms' error.
    let ops = vec![
        leg(53, "g3", "D", 1),
        leg(54, "g3", "C", 1),
        refusal::raw_crdt_delta_sub_record(),
    ];
    let verdict = parity(
        &mut held.core,
        &mut held.wal,
        &framed_ops(ops),
        &Run::commit(),
    );
    assert!(
        refusal_text(&verdict).contains("raw CRDT delta"),
        "{verdict:?}"
    );

    // A restore runs UNIQUE only, so the unbalanced ledger passes.
    let restore = Run {
        origin: RedoOrigin::Restore,
        ..Run::commit()
    };
    let ops = vec![leg(55, "g4", "D", 7), handover::put(7, "R")];
    let verdict = parity(&mut held.core, &mut held.wal, &framed_ops(ops), &restore);
    assert!(!format!("{verdict:?}").contains("debits"), "{verdict:?}");
}

#[test]
fn balanced_sums_accumulate_across_batches() {
    let mut held = hold(ledger_core);
    let mut balanced = vec![leg(60, "g1", "D", 10)];
    balanced.extend(filler(500, 10));
    balanced.push(leg(61, "g1", "C", 4));
    balanced.extend(filler(520, 10));
    balanced.push(leg(62, "g1", "C", 6));
    let verdict = parity(
        &mut held.core,
        &mut held.wal,
        &framed_ops(balanced),
        &Run::commit(),
    );
    assert!(verdict.is_ok(), "{verdict:?}");

    let mut unbalanced = vec![leg(63, "g1", "D", 10)];
    unbalanced.extend(filler(540, 10));
    unbalanced.push(leg(64, "g1", "C", 4));
    let verdict = parity(
        &mut held.core,
        &mut held.wal,
        &framed_ops(unbalanced),
        &Run::commit(),
    );
    assert!(refusal_text(&verdict).contains("g1"), "{verdict:?}");
}

// ── Materialized-sum folds ───────────────────────────────────────────────

pub(in crate::data::executor::handlers::transaction::redo_apply) const CHARGES: &str = "charges";
pub(in crate::data::executor::handlers::transaction::redo_apply) const BALANCES: &str = "balances";
pub(in crate::data::executor::handlers::transaction::redo_apply) const A1: Surrogate =
    Surrogate(4242);
pub(in crate::data::executor::handlers::transaction::redo_apply) const A2: Surrogate =
    Surrogate(4243);

/// A core where `charges` sums `amount` into `balances.balance`, which is
/// UNIQUE. Account `a1` holds 10 and `a2` holds 20.
pub(in crate::data::executor::handlers::transaction::redo_apply) fn fold_core(
    dir: &std::path::Path,
) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (mut core, ends) = plain_core(dir);
    let mut target = CollectionConfig::new(BALANCES);
    target.index_paths.push(IndexPath {
        unique: true,
        ..IndexPath::new("balance")
    });
    core.doc_configs.insert(
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            BALANCES.to_string(),
        ),
        target,
    );
    let mut source = CollectionConfig::new(CHARGES);
    source.enforcement.materialized_sum_sources = vec![MaterializedSumBinding {
        target_collection: BALANCES.to_string(),
        target_column: "balance".to_string(),
        join_column: "account_id".to_string(),
        value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        declared_primary_key: None,
    }];
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(TID), CHARGES.to_string()),
        source,
    );
    for (surrogate, account, balance) in [(A1, "a1", "10"), (A2, "a2", "20")] {
        let doc = serde_json::json!({"id": account, "balance": balance});
        let ordinal = core.bitemporal_ordinal();
        core.sparse
            .put(
                0,
                TID,
                BALANCES,
                &StorageKey::for_surrogate(surrogate),
                &doc_format::encode_to_msgpack(&doc),
                ordinal,
            )
            .expect("seed target row");
    }
    (core, ends)
}

pub(in crate::data::executor::handlers::transaction::redo_apply) fn fold_targets()
-> Vec<RedoSumTargets> {
    vec![RedoSumTargets {
        collection: CHARGES.to_string(),
        resolved: vec![
            ResolvedSumTarget::new(BALANCES, "a1", A1),
            ResolvedSumTarget::new(BALANCES, "a2", A2),
        ],
    }]
}

pub(in crate::data::executor::handlers::transaction::redo_apply) fn charge(
    surrogate: u32,
    account: &str,
    amount: i64,
) -> RedoSubRecord {
    let body = doc_format::encode_to_msgpack(&serde_json::json!({
        "account_id": account,
        "amount": amount,
    }));
    doc_put_sub_record(CHARGES, &format!("c{surrogate}"), &body, surrogate)
}

#[test]
fn folds_accumulate_across_batches() {
    let mut held = hold(fold_core);
    // `a1` reaches 25: no other row holds it.
    let ops = vec![charge(1, "a1", 5), charge(2, "a1", 5), charge(3, "a1", 5)];
    let content = framed(&record(ops), &fold_targets(), &[]);
    let verdict = parity(&mut held.core, &mut held.wal, &content, &Run::commit());
    assert!(verdict.is_ok(), "{verdict:?}");

    // `a2` folds to 30 first. `a1` reaches 30 only if each fold reads the
    // balance the earlier folds left, batches apart.
    let ops = vec![
        charge(10, "a2", 10),
        charge(11, "a1", 5),
        charge(12, "a1", 5),
        charge(13, "a1", 5),
        charge(14, "a1", 5),
    ];
    let content = framed(&record(ops), &fold_targets(), &[]);
    let verdict = parity(&mut held.core, &mut held.wal, &content, &Run::commit());
    assert!(is_unique_violation(&verdict), "{verdict:?}");
    assert!(refusal_text(&verdict).contains("'30'"), "{verdict:?}");
}

// ── Snapshot ─────────────────────────────────────────────────────────────

fn orders_task(lsn: u64) -> ExecutionTask {
    let mut task =
        task_with_vshard(collection_vshard(DatabaseId::DEFAULT, "orders").expect("vshard"));
    task.wal_lsn = Some(Lsn::new(lsn));
    task
}

fn order(surrogate: u32) -> RedoSubRecord {
    doc_put_sub_record(
        "orders",
        &format!("o{surrogate}"),
        &crate::data::executor::core_loop::tests::doc_value("a", "1"),
        surrogate,
    )
}

#[test]
fn a_snapshot_commit_validates_alike() {
    let mut held = hold(plain_core);
    let seed = record(vec![order(7)]).to_bytes().expect("encode");
    let seeded = held.core.install_redo(
        &orders_task(10),
        TID,
        CommittedRedo {
            redo: &seed,
            collections: &["orders".to_string()],
            sum_targets: &[],
        },
        RedoOrigin::Commit,
    );
    assert_eq!(seeded.status, Status::Ok, "{:?}", seeded.error_code);
    let newest = held
        .core
        .sparse
        .versioned_latest_sys_from(
            0,
            TID,
            "orders",
            &StorageKey::for_surrogate(Surrogate::new(7)),
        )
        .expect("read row")
        .expect("row 7 holds a version");
    let predicate = VersionedReadEntry {
        engine: EngineTag::Document,
        collection: "orders".into(),
        key: ReadKeyIdent::Predicate,
        home_vshard: None,
    };
    let run = |snapshot: i64| Run {
        task: orders_task(LSN),
        origin: RedoOrigin::Commit,
        snapshot: Some(snapshot),
        stamp_floor: None,
    };
    // A stale write over row 7, after a fresh insert of row 8.
    let content = framed(&record(vec![order(8), order(7)]), &[], &[]);
    let verdict = parity(&mut held.core, &mut held.wal, &content, &run(newest - 1));
    assert!(refusal_text(&verdict).contains("writes"), "{verdict:?}");
    // A stale predicate read ranks before the write.
    let reads = vec![predicate.clone(); 3];
    let content = framed(&record(vec![order(7)]), &[], &reads);
    let verdict = parity(&mut held.core, &mut held.wal, &content, &run(newest - 1));
    assert!(refusal_text(&verdict).contains("read"), "{verdict:?}");
    // A covering snapshot commits.
    let verdict = parity(&mut held.core, &mut held.wal, &content, &run(newest));
    assert!(verdict.is_ok(), "{verdict:?}");
}

// ── Decode and local failures ────────────────────────────────────────────

#[test]
fn a_decode_error_in_the_last_batch_outranks_every_check() {
    let mut held = hold(seeded_unique_core);
    let mut ops = vec![handover::put(3, "Z")];
    ops.extend(filler(600, 10));
    ops.push(handover::put(4, "Z"));
    let mut content = framed_ops(ops);
    let sound = content.clone();
    // A row change whose body is no row change.
    push_frame(&mut content, RedoFrameTag::RowChange, &7u32).expect("frame");
    let verdict = parity(&mut held.core, &mut held.wal, &content, &Run::commit());
    assert!(
        verdict.is_err() && !is_unique_violation(&verdict),
        "{verdict:?}"
    );

    // Content that ends inside a record does not read back whole.
    let mut cut = sound.clone();
    cut.truncate(cut.len() - 3);
    let verdict = parity(&mut held.core, &mut held.wal, &cut, &Run::commit());
    assert!(
        matches!(verdict, Err(ErrorCode::RetryableRefusal { .. })),
        "{verdict:?}"
    );

    // Content with no header record.
    let mut headless = Vec::new();
    push_frame(&mut headless, RedoFrameTag::Op, &handover::put(3, "Z")).expect("frame");
    let verdict = parity(&mut held.core, &mut held.wal, &headless, &Run::commit());
    assert!(
        verdict.is_err() && !is_unique_violation(&verdict),
        "{verdict:?}"
    );

    // The sound content refuses on its UNIQUE violation.
    let verdict = parity(&mut held.core, &mut held.wal, &sound, &Run::commit());
    assert!(is_unique_violation(&verdict), "{verdict:?}");
}

#[test]
fn a_spill_failure_refuses_as_retryable() {
    let mut held = hold(seeded_unique_core);
    let dir = SpillKind::Txn
        .core_dir(&held.core.data_dir, held.core.core_id)
        .join(format!("redo-validate-{LSN}"));
    // A file where the spill directory goes: the first spill cannot create
    // it.
    std::fs::create_dir_all(dir.parent().expect("parent")).expect("create parent");
    std::fs::write(&dir, b"blocked").expect("block the spill directory");
    let run = Run::commit();
    let reference = held.wal.write(&framed_ops(vec![handover::put(3, "C")]));
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    let judged_at_ms = judged_instant_ms(None, run.task.request.system_ordinal);
    let spilling = StreamLimits {
        batch_bytes: SMALL_BATCH_BYTES,
        spill_threshold: 0,
    };
    let verdict = streamed_verdict(
        &mut held.core,
        &run,
        &marker,
        &reference,
        judged_at_ms,
        spilling,
    );
    assert!(
        matches!(&verdict, Err(ErrorCode::RetryableRefusal { reason }) if reason.contains("post-state")),
        "{verdict:?}"
    );
    std::fs::write(&dir, b"blocked").expect("block the spill directory");
    let in_memory = StreamLimits {
        spill_threshold: usize::MAX,
        ..spilling
    };
    let verdict = streamed_verdict(
        &mut held.core,
        &run,
        &marker,
        &reference,
        judged_at_ms,
        in_memory,
    );
    assert!(verdict.is_ok(), "{verdict:?}");
}

// ── Install ──────────────────────────────────────────────────────────────

#[test]
fn a_marker_install_answers_with_the_streamed_verdict() {
    let mut held = hold(seeded_unique_core);
    let install = |held: &mut Hold, ops: Vec<RedoSubRecord>| {
        let reference = held.wal.write(&framed_ops(ops));
        let marker = reference.marker(None, None, Vec::new()).expect("marker");
        let bytes = marker.to_bytes().expect("encode marker");
        held.core.execute_apply_transaction_redo(
            &default_task(),
            TID,
            CommittedRedo {
                redo: &bytes,
                collections: &[handover::COLL.to_string()],
                sum_targets: &[],
            },
        )
    };
    let refused = install(
        &mut held,
        vec![handover::put(3, "Z"), handover::put(4, "Z")],
    );
    assert_eq!(refused.status, Status::Error);
    assert!(
        matches!(
            refused.error_code.as_deref(),
            Some(ErrorCode::RejectedConstraint { constraint, .. }) if constraint == "unique"
        ),
        "{:?}",
        refused.error_code
    );
    let installed = install(
        &mut held,
        vec![handover::put(3, "C"), handover::put(1, "Q")],
    );
    assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
    let spill_dir = SpillKind::Txn
        .core_dir(&held.core.data_dir, held.core.core_id)
        .join(format!("redo-validate-{LSN}"));
    assert!(
        !spill_dir.exists(),
        "the validate's spill directory is gone"
    );
}
