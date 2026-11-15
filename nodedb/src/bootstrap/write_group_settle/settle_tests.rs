// SPDX-License-Identifier: BUSL-1.1

//! Boot settles a stored write set whose row is at the row image limit.

use std::sync::Arc;

use nodedb_physical::physical_plan::{DocumentOp, StorageMode, UpdateValue};
use nodedb_types::{
    MAX_ROW_IMAGE_BYTES, QualifiedCollection, RlsWriteCheck, RowIdentity, StorageKey, Surrogate,
    Value,
};
use nodedb_wal::TombstoneSet;
use nodedb_wal::record::RecordType;

use super::settle::settle_against;
use crate::bridge::envelope::{PhysicalPlan, RowVersion, WriteSetEntry};
use crate::control::server::wal_dispatch::{WalAppendRequest, append_group_origin};
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::event::EventSource;
use crate::types::document_event::DocumentPutEvent;
use crate::types::installed_document::InstalledDocument;
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};
use crate::wal::{CapturedEntry, GroupMembership, OriginAppend, WriteGroupRecord, WriteSetCapture};

const ORDERS: &str = "orders";

/// A plain MessagePack document `{"note": ...}` of exactly `len` bytes.
fn body_of_len(len: usize) -> Vec<u8> {
    // A one-entry map header, the `fixstr` key `note` and a `str 32` header.
    let note_len = len - (1 + 5 + 5);
    let mut body = Vec::with_capacity(len);
    body.push(0x81);
    body.push(0xa4);
    body.extend_from_slice(b"note");
    body.push(0xdb);
    body.extend_from_slice(&u32::try_from(note_len).expect("note length").to_be_bytes());
    body.resize(len, b'n');
    assert_eq!(body.len(), len, "the body is exactly {len} bytes");
    body
}

fn update_plan() -> PhysicalPlan {
    let literal = nodedb_types::value_to_msgpack(&Value::String("n".into())).expect("literal");
    PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, ORDERS),
        document_id: "o1".to_string(),
        surrogate: Some(Surrogate::new(1)),
        pk_bytes: b"o1".to_vec(),
        updates: vec![("note".to_string(), UpdateValue::Literal(literal))],
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
    })
}

/// The write set of the update, stored before a crash cut its parts: one
/// row whose bodies and event images are each at the limit.
fn stored_write_set(plan: &PhysicalPlan, origin: u64, body: &[u8]) -> WriteSetCapture {
    let entry = WriteSetEntry::installed_put(
        1,
        RowIdentity::from_user_key("o1"),
        InstalledDocument {
            canonical: body.to_vec(),
            stored: body.to_vec(),
            version: RowVersion::open(1),
            schema: Arc::new(StorageMode::Schemaless),
            chain_head: None,
        },
        DocumentPutEvent {
            new_value: body.to_vec(),
            old_value: Some(body.to_vec()),
        },
    );
    let mut schemas = Vec::new();
    let captured = CapturedEntry::from_entry(&entry, &mut schemas).expect("capture entry");
    let capture = WriteSetCapture {
        tenant_id: 1,
        vshard_id: 0,
        database_id: DatabaseId::DEFAULT.as_u64(),
        collection: ORDERS.to_string(),
        origin,
        entries: vec![captured],
        schemas,
        origin_append: OriginAppend {
            plan: zerompk::to_msgpack_vec(plan).expect("encode plan"),
            apply_key: NO_APPLY_KEY,
            event_source: EventSource::User.wal_code(),
            commit_hlc: None,
            resolved_now_ms: None,
            change_position: None,
        },
    };
    // Boot reads the write set back from its stored encoding.
    WriteSetCapture::from_bytes(&capture.to_bytes().expect("encode capture"))
        .expect("decode capture")
}

fn membership(records: &[nodedb_wal::WalRecord]) -> GroupMembership {
    let mut groups = GroupMembership::default();
    for record in records {
        if RecordType::from_raw(record.logical_record_type()) == Some(RecordType::WriteGroup) {
            let group = WriteGroupRecord::from_bytes(&record.payload)
                .expect("decode group record")
                .group;
            groups.observe(record.header.lsn, group);
        }
    }
    groups
}

/// Boot journals the missing part of a write set at the limit, the group
/// is whole, a second boot appends nothing, and replay installs the row.
#[test]
fn a_stored_write_set_at_the_row_limit_completes_and_boot_proceeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
    let plan = update_plan();
    let origin = append_group_origin(WalAppendRequest {
        wal: wal.appender(NO_APPLY_KEY),
        event_source: EventSource::User,
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        database_id: DatabaseId::DEFAULT,
        plan: &plan,
        credentials: None,
        now_override: None,
    })
    .expect("append origin")
    .origin;
    wal.sync().expect("sync");

    let body = body_of_len(MAX_ROW_IMAGE_BYTES);
    let stored = stored_write_set(&plan, origin.lsn.as_u64(), &body);
    let records = wal.replay().expect("replay");
    assert!(
        !membership(&records).broken_through(u64::MAX).is_empty(),
        "the crash left the group broken"
    );
    assert!(
        settle_against(&wal, &records, &[stored])
            .expect("settle")
            .appended,
        "boot journals the missing part"
    );
    wal.sync().expect("sync");

    let records = wal.replay().expect("replay");
    assert!(
        membership(&records).broken_through(u64::MAX).is_empty(),
        "the group is whole"
    );
    assert!(
        records
            .iter()
            .any(|record| record.payload.len() > 4 * MAX_ROW_IMAGE_BYTES),
        "the part journals every image of the row"
    );
    assert!(
        !settle_against(&wal, &records, &[])
            .expect("settle again")
            .appended,
        "a settled WAL needs nothing at the next boot"
    );

    let base = tempfile::tempdir().expect("tempdir");
    let (mut core, _requests, _responses) = make_core_with_dir(base.path());
    core.replay_all_wal(&records, 1, &TombstoneSet::new())
        .expect("replay proceeds");
    let row = core
        .sparse
        .get(
            DatabaseId::DEFAULT.as_u64(),
            1,
            ORDERS,
            &StorageKey::for_surrogate(Surrogate::new(1)),
        )
        .expect("read row");
    assert_eq!(
        row.as_deref(),
        Some(body.as_slice()),
        "replay installs the row"
    );
}
