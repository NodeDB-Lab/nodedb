// SPDX-License-Identifier: BUSL-1.1

//! Boot refuses a batched install cursor it cannot settle: one whose
//! marker's chunks are gone, and one whose marker the WAL does not hold.

use std::path::Path;

use nodedb_wal::{TombstoneSet, WalRecord};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::RedoStreamId;
use crate::wal::manager::NO_APPLY_KEY;
use crate::wal::redo::install_cursor::{CursorState, InstallCursor};
use crate::wal::redo::replay_arm::RedoArm;

use super::super::stream_validate::tests::{StreamWal, TID, framed, plain_core, record};
use super::equivalence_tests::fill;

/// Write `cursor` into the store under `dir`.
fn store_cursor(dir: &Path, cursor: &InstallCursor) {
    let (core, _req, _resp) = make_core_with_dir(dir);
    let txn = core.sparse.begin_durable_write().expect("txn");
    core.sparse
        .put_install_cursor_in_txn(&txn, cursor.lsn, &cursor.to_bytes().expect("encode"))
        .expect("cursor");
    txn.commit().expect("commit");
}

fn installing(lsn: u64, stream: RedoStreamId) -> InstallCursor {
    InstallCursor {
        lsn,
        stream,
        state: CursorState::Installing {
            arm: RedoArm::Document,
            batch: 0,
            ops_through: 3,
        },
    }
}

/// A cursor's marker whose chunks a truncation removed: replay cannot
/// complete the record, so boot refuses.
#[test]
fn a_cursor_with_missing_chunks_refuses_to_boot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (stream, lsn, records) = {
        let live = live_in(dir.path());
        let mut stream_wal = live.wal;
        let reference = stream_wal.write(&framed(&record(fill(9000, 6)), &[], &[]));
        let wal = stream_wal.manager();
        wal.seal_active_segment().expect("seal the chunk segment");
        let marker = reference.marker(None, None, Vec::new()).expect("marker");
        let lsn = wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_transaction_redo(
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &marker,
            )
            .expect("marker record");
        wal.sync().expect("sync");
        assert!(wal.truncate_before(lsn).expect("truncate").segments_deleted > 0);
        (
            reference.stream,
            lsn.as_u64(),
            wal.replay().expect("replay"),
        )
    };
    store_cursor(dir.path(), &installing(lsn, stream));

    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let error = core
        .replay_all_wal(&records, 1, &TombstoneSet::new())
        .expect_err("a cursor marker without its chunks refuses boot");
    assert!(error.to_string().contains(&format!("lsn {lsn}")), "{error}");
    assert!(core.is_fail_stopped(), "the core serves no state");
}

/// A cursor whose marker the WAL does not hold fail-stops the core, with
/// an empty WAL and with one that holds other records.
#[test]
fn a_cursor_without_its_record_fail_stops() {
    let stream = RedoStreamId::Session {
        vshard: 0,
        idempotency_key: 77,
    };
    let other = kv_record(5);
    for records in [Vec::new(), vec![other]] {
        let dir = tempfile::tempdir().expect("tempdir");
        store_cursor(dir.path(), &installing(777, stream));
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let error = core
            .replay_all_wal(&records, 1, &TombstoneSet::new())
            .expect_err("an orphaned cursor refuses boot");
        assert!(error.to_string().contains("lsn 777"), "{error}");
        assert!(core.is_fail_stopped());
    }
}

/// The live node over `dir`: its stores and WAL are dropped with it.
fn live_in(dir: &Path) -> LiveIn {
    let (core, ends) = plain_core(dir);
    let wal = StreamWal::open(&core);
    LiveIn {
        _core: core,
        wal,
        _ends: ends,
    }
}

struct LiveIn {
    _core: CoreLoop,
    wal: StreamWal,
    _ends: Box<dyn std::any::Any>,
}

/// A KV put at `lsn`, a record no cursor names.
fn kv_record(lsn: u64) -> WalRecord {
    WalRecord::new(nodedb_wal::record::WalRecordArgs {
        record_type: nodedb_wal::record::RecordType::Put as u32,
        lsn,
        tenant_id: TID,
        vshard_id: 0,
        database_id: 0,
        payload: zerompk::to_msgpack_vec(&(
            "kv_put",
            "cache",
            b"k".to_vec(),
            b"v".to_vec(),
            0u64,
            None::<u64>,
            1u32,
            Vec::<nodedb_physical::physical_plan::DeclaredColumn>::new(),
        ))
        .expect("encode kv put"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("kv record")
}

/// A redo record at `lsn` that sets `labels` on `node` at `ordinal`.
fn label_record(lsn: u64, node: &str, labels: &[&str], ordinal: i64) -> WalRecord {
    let op = crate::wal::NodeLabelRedo {
        node_id: node.to_string(),
        labels: labels.iter().map(|label| label.to_string()).collect(),
        system_from: Some(ordinal),
        exact: false,
    };
    let redo = record(vec![crate::wal::RedoSubRecord {
        record_type: nodedb_wal::record::RecordType::GraphNodeLabelSet as u32,
        payload: op.encode().expect("encode label op"),
    }]);
    WalRecord::new(nodedb_wal::record::WalRecordArgs {
        record_type: nodedb_wal::record::RecordType::TransactionRedo as u32,
        lsn,
        tenant_id: TID,
        vshard_id: 0,
        database_id: 0,
        payload: redo.to_bytes().expect("encode redo"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("redo record")
}

/// The ordinal of `node`'s newest label version after a boot over `dir`
/// read its cursors and replayed `replayed`. The record is no stream
/// marker, so the test stops before boot settles the cursors.
fn newest_label_after_boot(dir: &Path, replayed: &WalRecord, node: &str) -> Option<i64> {
    let (mut core, _req, _resp) = make_core_with_dir(dir);
    core.load_install_cursors().expect("cursors");
    core.replay_engines_in_lsn_order(std::slice::from_ref(replayed), 1, &TombstoneSet::new())
        .expect("replay");
    core.edge_store
        .newest_node_label_ordinal(DatabaseId::DEFAULT, TenantId::new(TID), node)
        .expect("read label versions")
}

/// A batch of a record committed its label versions with the record's LSN
/// mark, and the crash cut the next batch. Boot versions the cut batch's
/// ops: the cursor makes the record due though the mark names it. With no
/// cursor the mark says the record is versioned, and boot writes none.
#[test]
fn a_cursor_record_is_versioned_again_past_the_label_mark() {
    const LSN: u64 = 40;
    for with_cursor in [true, false] {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let (core, _req, _resp) = make_core_with_dir(dir.path());
            let durable = crate::engine::graph::edge_store::NodeLabelVersion {
                db: DatabaseId::DEFAULT,
                tid: TenantId::new(TID),
                node: "n1".into(),
                ordinal: 10,
                labels: vec!["A".into()],
            };
            core.edge_store
                .put_node_label_versions(&[durable], Some(LSN))
                .expect("the durable batch's versions");
        }
        if with_cursor {
            let stream = RedoStreamId::Session {
                vshard: 0,
                idempotency_key: 3,
            };
            store_cursor(dir.path(), &installing(LSN, stream));
        }
        let cut_batch = label_record(LSN, "n2", &["B"], 20);
        let newest = newest_label_after_boot(dir.path(), &cut_batch, "n2");
        if with_cursor {
            assert_eq!(newest, Some(20), "boot versions the cut batch's op");
        } else {
            assert_eq!(newest, None, "the mark names the record versioned");
        }
    }
}
