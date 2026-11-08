// SPDX-License-Identifier: BUSL-1.1

//! Refused sync inputs retain their consumed frontier across WAL recovery.

use super::*;
use crate::bridge::envelope::{Admission, Status};
use crate::control::server::wal_dispatch::encode_text_op_record;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::types::{TenantId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};
use nodedb_types::QualifiedCollection;
use nodedb_types::sync::wire::{AckStatus, SyncAckResult, SyncOutcome, SyncProvenance};
use nodedb_wal::TombstoneSet;

const COLLECTION: &str = "articles";
const PRODUCER: u64 = 11;
const STREAM: u64 = 7;

fn provenance(seq: u64) -> SyncProvenance {
    SyncProvenance {
        producer_id: PRODUCER,
        epoch: 1,
        stream_id: STREAM,
        seq,
    }
}

fn index_op(surrogate: Surrogate, provenance: Option<SyncProvenance>) -> TextOp {
    TextOp::FtsIndexDoc {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        surrogate,
        fields: vec![("body".to_owned(), "searchable words".to_owned())],
        provenance,
    }
}

fn append(wal: &WalManager, op: &TextOp) -> crate::types::Lsn {
    let (kind, payload) = encode_text_op_record(op).unwrap().unwrap();
    assert_eq!(kind, RecordType::FtsIndex);
    wal.appender(NO_APPLY_KEY)
        .with_event_source(crate::event::EventSource::CrdtSync)
        .append_fts_index(
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &payload,
        )
        .unwrap()
}

fn count(core: &CoreLoop) -> u32 {
    core.inverted
        .corpus_stats(0, TenantId::new(1), COLLECTION)
        .unwrap()
        .0
}

#[test]
fn an_uncancelled_sync_refusal_and_next_index_survive_two_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let wal_path = directory.path().join("fts.wal");
    let core_path = directory.path().join("core");
    {
        let wal = WalManager::open_for_testing(&wal_path).unwrap();
        append(&wal, &index_op(Surrogate::ZERO, Some(provenance(1))));
        wal.sync().unwrap();
    }
    {
        let wal = WalManager::open_for_testing(&wal_path).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1, "crash precedes abort and HWM markers");
        let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
        core.replay_all_wal(&records, 1, &TombstoneSet::new())
            .unwrap();
        assert_eq!(core.sync_hwm_value(PRODUCER, STREAM), 1);
        assert_eq!(count(&core), 0);
        let op = index_op(Surrogate::new(9), Some(provenance(2)));
        let lsn = append(&wal, &op);
        let mut request = make_default_task().request;
        request.plan = PhysicalPlan::Text(op);
        request.admission = Admission::Admitted;
        request.event_source = crate::event::EventSource::CrdtSync;
        let response = core.execute(&ExecutionTask::with_wal_lsn(request, Some(lsn)));
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).unwrap();
        assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Applied));
        assert_eq!(ack.applied_seq, 2);
        assert_eq!(count(&core), 1);
        wal.sync().unwrap();
    }
    {
        let wal = WalManager::open_for_testing(&wal_path).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 2);
        let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
        core.replay_all_wal(&records, 1, &TombstoneSet::new())
            .unwrap();
        assert_eq!(core.sync_hwm_value(PRODUCER, STREAM), 2);
        assert_eq!(count(&core), 1);
    }
}

#[test]
fn an_unbound_local_index_remains_corrupt_on_reopen() {
    for provenance in [None, Some(SyncProvenance::default())] {
        let directory = tempfile::tempdir().unwrap();
        let wal_path = directory.path().join("fts.wal");
        {
            let wal = WalManager::open_for_testing(&wal_path).unwrap();
            append(&wal, &index_op(Surrogate::ZERO, provenance));
            wal.sync().unwrap();
        }
        let wal = WalManager::open_for_testing(&wal_path).unwrap();
        let records = wal.replay().unwrap();
        let (mut core, _requests, _responses) = make_core_with_dir(&directory.path().join("core"));
        assert!(
            core.replay_all_wal(&records, 1, &TombstoneSet::new())
                .is_err()
        );
        assert_eq!(count(&core), 0);
    }
}
