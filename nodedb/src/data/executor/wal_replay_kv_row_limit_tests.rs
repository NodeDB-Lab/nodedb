// SPDX-License-Identifier: BUSL-1.1

//! A KV record over the row image limit in the WAL: the node crashed
//! between the append and the Data Plane's refusal. Replay refuses the
//! record the same way on every replica, and the core boots.

use std::collections::HashMap;

use nodedb_physical::physical_plan::KvOp;
use nodedb_types::{MAX_ROW_IMAGE_BYTES, QualifiedCollection, Surrogate, Value};
use nodedb_wal::{TombstoneSet, WalRecord};

use crate::bridge::envelope::ErrorCode;
use crate::control::server::wal_dispatch_kv::encode::{encode_kv_field_set, encode_kv_put};
use crate::control::server::wal_dispatch_kv::wal_append_kv_op;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalAppender, WalManager};

const TID: u64 = 1;
const COLLECTION: &str = "cache";

struct Journal {
    wal: WalManager,
    _dir: tempfile::TempDir,
}

impl Journal {
    fn open() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("kv.wal")).expect("open wal");
        Self { wal, _dir: dir }
    }

    fn appender(&self) -> WalAppender<'_> {
        self.wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
    }

    /// Append `payload` as the control plane journals a KV write, with no
    /// refusal after it.
    fn append(&self, payload: &[u8]) {
        self.appender()
            .append_put(
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                payload,
            )
            .expect("append");
    }

    fn records(&self) -> Vec<WalRecord> {
        self.wal.sync().expect("sync wal");
        self.wal.replay().expect("read wal")
    }
}

/// Replay `records` onto a fresh core as a booting node does, and return
/// the value it stores for `k`.
fn replay(records: &[WalRecord]) -> Option<Vec<u8>> {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    core.replay_all_wal(records, 1, &TombstoneSet::new())
        .expect("replay boots");
    assert!(!core.is_fail_stopped(), "a size-refused record never halts");
    core.kv_engine.get(
        DatabaseId::DEFAULT.as_u64(),
        TID,
        COLLECTION,
        b"k",
        crate::engine::kv::current_ms(),
    )
}

fn text(len: usize) -> Vec<u8> {
    nodedb_types::value_to_msgpack(&Value::String("x".repeat(len))).expect("encode")
}

#[test]
fn a_put_journaled_before_its_refusal_replays_and_boots() {
    let journal = Journal::open();
    // One key byte makes the row one byte over the limit.
    let over = vec![0u8; MAX_ROW_IMAGE_BYTES];
    journal.append(&encode_kv_put(COLLECTION, b"k", &over, 0, None, 7).expect("encode"));
    assert_eq!(replay(&journal.records()), None);
}

/// A FieldSet whose merge grows the row past the limit refuses on replay
/// against the same pre-state on every replica, so all replicas store the
/// same value: the row before the FieldSet.
#[test]
fn a_size_refused_field_set_replays_the_same_on_every_replica() {
    let journal = Journal::open();
    let half = MAX_ROW_IMAGE_BYTES / 2;
    let row = nodedb_types::value_to_msgpack(&Value::Object(HashMap::from([(
        "a".to_string(),
        Value::String("x".repeat(half)),
    )])))
    .expect("encode");
    journal.append(&encode_kv_put(COLLECTION, b"k", &row, 0, None, 7).expect("encode"));
    journal.append(
        &encode_kv_field_set(COLLECTION, b"k", &[("b".to_string(), text(half))], 7, true)
            .expect("encode"),
    );
    let records = journal.records();
    let first = replay(&records);
    let second = replay(&records);
    assert_eq!(first.as_deref(), Some(row.as_slice()));
    assert_eq!(first, second);
}

/// The control plane refuses a put over the limit before its record is
/// appended, so a crash cannot leave it in the WAL.
#[test]
fn the_control_plane_refuses_a_put_over_the_limit_before_the_append() {
    let journal = Journal::open();
    let op = KvOp::Put {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        key: b"k".to_vec(),
        value: vec![0u8; MAX_ROW_IMAGE_BYTES],
        ttl_ms: 0,
        surrogate: Surrogate::new(7),
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    };
    let refused = wal_append_kv_op(
        journal.appender(),
        TenantId::new(TID),
        VShardId::new(0),
        DatabaseId::DEFAULT,
        &op,
        None,
    )
    .expect_err("refused");
    assert!(
        matches!(
            refused,
            crate::Error::DataPlane(ErrorCode::ProgramLimitExceeded { .. })
        ),
        "{refused:?}"
    );
    assert!(journal.records().is_empty(), "nothing is appended");
}
