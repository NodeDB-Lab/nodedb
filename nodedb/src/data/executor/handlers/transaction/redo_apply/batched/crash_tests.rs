// SPDX-License-Identifier: BUSL-1.1

//! A batched install cut by a crash completes forward at the next boot.
//!
//! The live core installs a stream marker under a root journal, batch by
//! batch, and each batch ends durable with its cursor. Each crash test
//! stops the install at a fail point, cuts the core's stores at that
//! instant the way a power loss does, and boots from the cut. Every source
//! row folds `amount` 1 into one account that holds 100.

use std::path::Path;

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan, RedoOrigin};

use crate::bridge::dispatch::JournalGroup;
use crate::bridge::envelope::Response;
use crate::control::server::wal_dispatch::{WalAppendRequest, append_group_origin};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::retention::judged_instant_ms;
use crate::data::executor::task::ExecutionTask;
use crate::engine::sparse::btree::SparseEngine;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::NO_APPLY_KEY;
use crate::wal::redo::install_cursor::{CursorState, InstallCursor};
use crate::wal::{RedoRecord, RedoSubRecord};

use super::super::calvin_fold_tests::{ACCOUNT, SOURCE, TID, balance, core_with_sum, sum_targets};
use super::super::replay_fold::tests::{boot, crash_image, install_rows, settle};
use super::super::stream_validate::tests::{StreamWal, framed, record};
use super::super::stream_validate::{StreamLimits, StreamValidateInput};
use super::super::test_commit::doc_put_sub_record;
use super::StreamInstallInput;

/// A batch bound that puts a few source rows in each batch.
const BATCH_BYTES: usize = 256;
/// Source rows the record writes, `amount` 1 each.
const CHARGES: u32 = 12;
/// The account's balance after one fold of every row.
const SINGLE_FOLD: &str = "112";

/// The record's source rows.
fn charges() -> Vec<RedoSubRecord> {
    (0..CHARGES)
        .map(|index| {
            let body = doc_format::encode_to_msgpack(&serde_json::json!({
                "account_id": ACCOUNT,
                "amount": 1,
            }));
            doc_put_sub_record(SOURCE, &format!("c{index}"), &body, 100 + index)
        })
        .collect()
}

/// The live node: a core whose source sums into the account, and its WAL.
struct Live {
    core: CoreLoop,
    wal: StreamWal,
    _ends: Box<dyn std::any::Any>,
    dir: tempfile::TempDir,
}

fn live() -> Live {
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, ends) = core_with_sum(dir.path());
    let wal = StreamWal::open(&core);
    Live {
        core,
        wal,
        _ends: ends,
        dir,
    }
}

/// A stream marker of `redo` in the live WAL, appended as a grouped commit's
/// origin: its plan, and its LSN.
fn append_marker(live: &mut Live, redo: &RedoRecord) -> (PhysicalPlan, Lsn) {
    let reference = live.wal.write(&framed(redo, &sum_targets(), &[]));
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    let plan = PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: marker.to_bytes().expect("encode marker"),
        collections: vec![SOURCE.to_string()],
        sum_targets: sum_targets(),
        origin: RedoOrigin::Commit,
        calvin: None,
        reads: Vec::new(),
        snapshot_ordinal: None,
    });
    let origin = append_group_origin(WalAppendRequest {
        wal: live.wal.manager().appender(NO_APPLY_KEY),
        event_source: crate::event::EventSource::User,
        tenant_id: TenantId::new(TID),
        vshard_id: VShardId::new(0),
        database_id: DatabaseId::DEFAULT,
        plan: &plan,
        credentials: None,
        now_override: None,
    })
    .expect("origin")
    .origin;
    // The funnel makes a marker durable before its install starts.
    live.wal.manager().sync().expect("sync");
    (plan, origin.lsn)
}

/// Install the marker `plan` names at `lsn` batch by batch under a root
/// journal, then end the journal as the core's tick does.
fn install(live: &mut Live, plan: PhysicalPlan, lsn: Lsn) -> Response {
    let (task, mut response) = install_unfinished(live, plan, lsn);
    if let Err(error) = live.core.finish_journalled_task(&task, &mut response) {
        panic!("finish journal: {error}");
    }
    response
}

/// [`install`] up to the install's answer, before the journal ends.
fn install_unfinished(live: &mut Live, plan: PhysicalPlan, lsn: Lsn) -> (ExecutionTask, Response) {
    let PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { redo, .. }) = &plan else {
        panic!("a redo install");
    };
    let marker = RedoRecord::from_bytes(redo).expect("decode marker");
    let reference = crate::wal::RedoStreamRef::of_marker(&marker)
        .expect("ref")
        .expect("a marker");
    let mut request = make_default_task().request;
    request.tenant_id = TenantId::new(TID);
    request.wal_lsn = Some(lsn);
    request.plan = plan;
    let task = ExecutionTask::new(request);
    let group = JournalGroup {
        origin: lsn,
        collection: SOURCE.into(),
        apply_key: NO_APPLY_KEY,
        commit_hlc: None,
        change_position: None,
    };
    let core = &mut live.core;
    let permit = core.begin_task_journal(&task, group).expect("root permit");
    let limits = StreamLimits {
        batch_bytes: BATCH_BYTES,
        spill_threshold: usize::MAX,
    };
    let validated = core
        .validate_redo_stream(&StreamValidateInput {
            task: &task,
            tid: TID,
            marker: &marker,
            reference: &reference,
            origin: RedoOrigin::Commit,
            snapshot: None,
            judged_at_ms: judged_instant_ms(None, task.request.system_ordinal),
            lsn,
            limits,
        })
        .unwrap_or_else(|code| panic!("validate: {code:?}"));
    let collections = [SOURCE.to_string()];
    let response = core.install_redo_stream(
        StreamInstallInput {
            task: &task,
            tid: TID,
            marker: &marker,
            reference: &reference,
            origin: RedoOrigin::Commit,
            lsn,
            limits,
            permit: Some(permit),
            collections: &collections,
        },
        validated,
        &mut Vec::new(),
    );
    (task, response)
}

/// The install cursors the stores at `stores` hold.
fn cursors(stores: &Path) -> Vec<InstallCursor> {
    let sparse = SparseEngine::open(&stores.join("sparse/core-0.redb")).expect("open sparse");
    sparse
        .load_install_cursors()
        .expect("cursors")
        .into_iter()
        .map(|(lsn, bytes)| InstallCursor::from_bytes(lsn, &bytes).expect("decode"))
        .collect()
}

mod cut {
    use nodedb_types::fail_point::{FailAction, FailGuard};

    use super::*;
    use crate::bootstrap::write_group_settle::settle::close_replayed_groups;

    /// The live node cut at `point`, with `failing` armed to fail besides:
    /// its WAL, the stores the cut left, and the record's LSN.
    struct Cut {
        live: Live,
        stores: tempfile::TempDir,
        lsn: u64,
    }

    fn cut_at(point: &str, failing: Option<&str>) -> Cut {
        cut_record_at(&record(charges()), point, failing)
    }

    /// [`cut_at`] for the install of `redo`.
    fn cut_record_at(redo: &RedoRecord, point: &str, failing: Option<&str>) -> Cut {
        let mut live = live();
        let (plan, lsn) = append_marker(&mut live, redo);
        let stores = {
            let _failing = failing.map(|name| FailGuard::fail(name, "a late batch fails"));
            let _cut = FailGuard::install(point, FailAction::Panic);
            let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                install(&mut live, plan, lsn)
            }));
            assert!(ran.is_err(), "the install stops at {point}");
            crash_image(live.dir.path())
        };
        Cut {
            live,
            stores,
            lsn: lsn.as_u64(),
        }
    }

    /// A collection of a database the install's fence never holds.
    const OTHER_DB: DatabaseId = DatabaseId::new(7);
    const OTHER_SURROGATE: u32 = 77;

    /// A put of one row into [`OTHER_DB`], with no journal.
    fn foreign_put() -> crate::bridge::envelope::Request {
        let mut request = make_default_task().request;
        request.request_id = crate::types::RequestId::new(2);
        request.tenant_id = TenantId::new(TID);
        request.database_id = OTHER_DB;
        // Its events carry another vShard than the install's, so the fence
        // lets it run while the install is parked.
        request.vshard_id = VShardId::new(1);
        request.plan =
            PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(OTHER_DB, "other"),
                document_id: "o1".into(),
                value: doc_format::encode_to_msgpack(&serde_json::json!({"value": 1})),
                surrogate: nodedb_types::Surrogate::new(OTHER_SURROGATE),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            });
        request
    }

    /// The live node cut at `point`, a fail point the install reaches while
    /// it is parked. The install runs through the core's request ring and
    /// `tick()`, with `foreign` queued behind it. The panic there stops the
    /// core, and the stores hold what a crash at the point leaves.
    fn cut_parked_at(point: &str, foreign: Option<crate::bridge::envelope::Request>) -> Cut {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse, WriteSetJournal};
        use nodedb_bridge::buffer::{Consumer, Producer};

        let mut live = live();
        live.core.redo_apply.stream_batch_bytes = BATCH_BYTES;
        let (plan, lsn) = append_marker(&mut live, &record(charges()));
        let mut request = make_default_task().request;
        request.tenant_id = TenantId::new(TID);
        request.wal_lsn = Some(lsn);
        request.plan = plan;
        let group = JournalGroup {
            origin: lsn,
            collection: SOURCE.into(),
            apply_key: NO_APPLY_KEY,
            commit_hlc: None,
            change_position: None,
        };
        let (requests, _responses) = live
            ._ends
            .downcast_mut::<(Producer<BridgeRequest>, Consumer<BridgeResponse>)>()
            .expect("ring ends");
        requests
            .try_push(BridgeRequest {
                inner: request,
                outcome_floor: Lsn::ZERO,
                journal: WriteSetJournal {
                    group: Some(group),
                    settled: Vec::new(),
                },
            })
            .expect("queue install");
        if let Some(foreign) = foreign {
            requests
                .try_push(BridgeRequest::unfloored(foreign))
                .expect("queue foreign write");
        }
        let stores = {
            let _cut = FailGuard::install(point, FailAction::Panic);
            for _ in 0..1_000 {
                if live.core.is_fail_stopped() {
                    break;
                }
                live.core.tick();
            }
            assert!(
                live.core.is_fail_stopped(),
                "the parked install stops at {point}"
            );
            crash_image(live.dir.path())
        };
        Cut {
            live,
            stores,
            lsn: lsn.as_u64(),
        }
    }

    /// Whether the stores at `stores` hold the foreign row.
    fn holds_foreign(stores: &Path) -> bool {
        let sparse = SparseEngine::open(&stores.join("sparse/core-0.redb")).expect("open sparse");
        sparse
            .get(
                OTHER_DB.as_u64(),
                TID,
                nodedb_types::QualifiedCollection::new(OTHER_DB, "other").as_str(),
                &nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(
                    OTHER_SURROGATE,
                )),
            )
            .expect("read foreign row")
            .is_some()
    }

    /// Settle and boot the cut stores. Returns the balance and the write
    /// sets replay stored.
    fn restart(cut: &Cut) -> (Option<String>, Vec<crate::wal::WriteSetCapture>) {
        let (outcome, records) = settle(cut.live.wal.manager(), cut.stores.path());
        boot(cut.stores.path(), &outcome, &records)
    }

    /// The cut leaves the first two batches durable: their rows, their folds
    /// and the fold ledger, with a cursor at the second batch. Boot folds
    /// only the ops above the ledger, and its write set commit drops every
    /// install row.
    #[test]
    fn a_crash_after_batch_k_folds_each_source_once() {
        let cut = cut_at(
            "redo_apply::batched::after_batch_durable::Document::1",
            None,
        );
        let left = cursors(cut.stores.path());
        assert_eq!(left.len(), 1, "the cut leaves the install's cursor");
        assert!(matches!(
            left[0].state,
            CursorState::Installing { batch: 1, .. }
        ));
        let (ledgers, targets) = install_rows(cut.stores.path(), cut.lsn);
        assert!(
            matches!(ledgers.as_slice(), [(lsn, folded)] if *lsn == cut.lsn && *folded > 0),
            "the durable batches' fold ledger is on disk: {ledgers:?}"
        );
        assert!(targets > 0, "the durable batches' target rows are on disk");

        let (balance, captures) = restart(&cut);
        assert_eq!(
            balance.as_deref(),
            Some(SINGLE_FOLD),
            "one fold of every row"
        );
        assert_eq!(captures.len(), 1);
        assert_eq!(
            captures[0].write_set().expect("write set").len(),
            CHARGES as usize,
            "the write set holds one target write per row"
        );
        assert!(
            cursors(cut.stores.path()).is_empty(),
            "boot drops the cursor"
        );
        assert_eq!(install_rows(cut.stores.path(), cut.lsn), (Vec::new(), 0));
    }

    /// Boot journals the replay's fold rows. The next boot replays the
    /// record plainly and keeps the single fold.
    #[test]
    fn a_second_restart_keeps_the_single_fold() {
        let cut = cut_at(
            "redo_apply::batched::after_batch_durable::Document::2",
            None,
        );
        let (balance, captures) = restart(&cut);
        assert_eq!(balance.as_deref(), Some(SINGLE_FOLD));
        assert!(close_replayed_groups(cut.live.wal.manager(), &captures).expect("close"));

        let (balance, captures) = restart(&cut);
        assert_eq!(
            balance.as_deref(),
            Some(SINGLE_FOLD),
            "a second restart folds nothing again"
        );
        assert!(captures.is_empty());
    }

    /// The install parked after its second batch, with its state out of
    /// the core. Boot completes the record forward and folds each source
    /// once.
    #[test]
    fn a_crash_while_parked_completes_forward_once() {
        let cut = cut_parked_at("redo_apply::batched::parked::Document::1", None);
        let left = cursors(cut.stores.path());
        assert!(
            matches!(left.as_slice(), [cursor] if matches!(cursor.state, CursorState::Installing { batch: 1, .. })),
            "the cut leaves the cursor of the parked batch: {left:?}"
        );
        let (balance, captures) = restart(&cut);
        assert_eq!(
            balance.as_deref(),
            Some(SINGLE_FOLD),
            "one fold of every row"
        );
        assert_eq!(captures.len(), 1);
        assert!(
            cursors(cut.stores.path()).is_empty(),
            "boot drops the cursor"
        );
    }

    /// A write of another database ran while the install was parked. The
    /// crash keeps it, and boot completes the install forward beside it.
    #[test]
    fn a_crash_while_parked_after_a_foreign_write_keeps_the_write() {
        let cut = cut_parked_at(
            "redo_apply::batched::parked::Document::1",
            Some(foreign_put()),
        );
        assert!(
            holds_foreign(cut.stores.path()),
            "the foreign write is durable at the cut"
        );
        let (balance, _) = restart(&cut);
        assert_eq!(balance.as_deref(), Some(SINGLE_FOLD));
        assert!(
            holds_foreign(cut.stores.path()),
            "boot keeps the foreign write"
        );
    }

    /// Every batch is durable and the write set is not stored: boot folds
    /// nothing the ledger names.
    #[test]
    fn a_crash_before_the_final_capture_folds_each_source_once() {
        let cut = cut_at("redo_apply::batched::before_final_capture", None);
        assert_eq!(cursors(cut.stores.path()).len(), 1);
        let (balance, captures) = restart(&cut);
        assert_eq!(balance.as_deref(), Some(SINGLE_FOLD));
        assert_eq!(captures.len(), 1);
    }

    /// The last batch fails once it wrote, and the rollback stops after its
    /// first durable checkpoint: the stores hold the forward install through
    /// the batches before the last. Boot completes the record forward once.
    #[test]
    fn a_crash_mid_rollback_installs_forward_once() {
        let cut = cut_at(
            "redo_apply::batched::rollback_after_checkpoint",
            Some("redo_apply::batched::install_batch_fails::Document"),
        );
        let left = cursors(cut.stores.path());
        assert!(
            matches!(left.as_slice(), [cursor] if matches!(cursor.state, CursorState::RollingBack { .. })),
            "the checkpoint stores a rolling-back cursor: {left:?}"
        );
        let (balance, captures) = restart(&cut);
        assert_eq!(
            balance.as_deref(),
            Some(SINGLE_FOLD),
            "one fold of every row"
        );
        assert_eq!(captures.len(), 1);
        assert!(cursors(cut.stores.path()).is_empty());
    }

    /// The rollback reversed every entry and stops before its durable end:
    /// the stores hold its last checkpoint, and boot completes the record
    /// forward once.
    #[test]
    fn a_crash_before_the_rollback_end_installs_forward_once() {
        let cut = cut_at(
            "redo_apply::batched::before_rollback_end",
            Some("redo_apply::batched::install_batch_fails::Document"),
        );
        assert_eq!(cursors(cut.stores.path()).len(), 1);
        let (balance, _) = restart(&cut);
        assert_eq!(balance.as_deref(), Some(SINGLE_FOLD));
    }

    /// A refused install's rollback ends durable before it answers, ahead of
    /// the journal's own commit: no install row stays on disk, and the
    /// account holds its seed.
    #[test]
    fn rollback_ends_durable_before_answering() {
        let mut live = live();
        let (plan, lsn) = append_marker(&mut live, &record(charges()));
        let (_task, response) = {
            let _failing = FailGuard::fail(
                "redo_apply::batched::install_batch_fails::Document",
                "a late batch fails",
            );
            install_unfinished(&mut live, plan, lsn)
        };
        assert!(response.error_code.is_some(), "{response:?}");
        let stores = crash_image(live.dir.path());
        assert!(cursors(stores.path()).is_empty());
        assert_eq!(install_rows(stores.path(), lsn.as_u64()), (Vec::new(), 0));
        let (seeded, _req, _resp) = make_core_with_dir(stores.path());
        assert_eq!(
            balance(&seeded).as_deref(),
            Some("100"),
            "the reversed folds are durable"
        );
    }

    /// A record of node-label sets alone, one node per op, each at ordinal
    /// `100 + n`.
    fn label_sets() -> RedoRecord {
        let ops = (0..CHARGES)
            .map(|node| RedoSubRecord {
                record_type: nodedb_wal::record::RecordType::GraphNodeLabelSet as u32,
                payload: crate::wal::NodeLabelRedo {
                    node_id: format!("n{node}"),
                    labels: vec!["L".into()],
                    system_from: Some(100 + i64::from(node)),
                    exact: false,
                }
                .encode()
                .expect("encode label op"),
            })
            .collect();
        record(ops)
    }

    /// A label-only install cut after a later batch's cursor is durable and
    /// before its edge store persist: the earlier batches' versions and the
    /// record's LSN mark are durable, the cut batch's are not. The cursor
    /// makes boot version every op of the record again.
    #[test]
    fn a_label_stream_cut_between_cursor_and_edges_versions_every_batch() {
        let cut = cut_record_at(
            &label_sets(),
            "redo_apply::batched::between_cursor_and_edges::GraphNodeLabelRedo::2",
            None,
        );
        assert_eq!(cursors(cut.stores.path()).len(), 1, "the cursor is durable");

        let (outcome, records) = settle(cut.live.wal.manager(), cut.stores.path());
        let (mut core, _req, _resp) = make_core_with_dir(cut.stores.path());
        core.set_never_durable_redos(&outcome.never_durable)
            .expect("fold ledger");
        core.replay_all_wal(&records, 1, &nodedb_wal::TombstoneSet::new())
            .expect("replay");
        for node in 0..CHARGES {
            let newest = core
                .edge_store
                .newest_node_label_ordinal(
                    DatabaseId::DEFAULT,
                    TenantId::new(TID),
                    &format!("n{node}"),
                )
                .expect("read label versions");
            assert_eq!(
                newest,
                Some(100 + i64::from(node)),
                "node n{node} is versioned at its op's ordinal"
            );
        }
        drop(core);
        assert!(
            cursors(cut.stores.path()).is_empty(),
            "boot drops the cursor"
        );
    }
}
