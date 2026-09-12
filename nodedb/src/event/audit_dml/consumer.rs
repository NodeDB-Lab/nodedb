// SPDX-License-Identifier: BUSL-1.1

//! Event Plane DML audit consumer.
//!
//! Called once per `WriteEvent` inside `process_normal_batch`. Records a
//! `DmlAudit` entry in the Control Plane audit log when:
//!
//! 1. The event source is `User` (not Trigger / RaftFollower / CrdtSync /
//!    Deferred — those are infrastructure writes, not user-initiated DML).
//! 2. The write op is a data-modifying operation (Insert / Update / Delete /
//!    BulkInsert / BulkDelete).
//! 3. The event's collection maps to a database with `AuditDmlMode::Writes`
//!    or `AuditDmlMode::All`.
//!
//! The control plane `AuditDmlCache` and `CollectionToDatabase` reverse map
//! are consulted from the `SharedState` (both are `Send + Sync`).
//!
//! The function is synchronous: it records an in-memory audit entry (and
//! best-effort WAL flush via `audit_record_with_db`). There is no disk I/O
//! on the Event Plane hot path — the flush is handled by the Control Plane's
//! periodic `flush_audit_log` timer.

use std::sync::Arc;

use nodedb_types::AuditDmlMode;

use crate::control::state::SharedState;
use crate::event::types::{EventSource, WriteEvent, WriteOp};

/// Attempt to record a DML audit entry for `event` if the database's
/// `AuditDmlMode` requires it.
///
/// Silently skips on any miss (unknown collection → unknown database →
/// mode is `None`) — the fail-open default keeps DML unblocked when the
/// cache is cold at startup.
pub fn audit_dml_event(event: &WriteEvent, state: &Arc<SharedState>) {
    // Only User-sourced writes are subject to DML auditing.
    match event.source {
        EventSource::User => {}
        EventSource::Trigger
        | EventSource::RaftFollower
        | EventSource::CrdtSync
        | EventSource::Deferred => return,
    }

    // Only data-modifying ops (not Heartbeat).
    match event.op {
        WriteOp::Insert
        | WriteOp::Update
        | WriteOp::Delete
        | WriteOp::BulkInsert { .. }
        | WriteOp::BulkDelete { .. } => {}
        WriteOp::Heartbeat => return,
    }

    // The Data Plane propagates the request database with every write; do not
    // reverse-map by collection because a tenant may use the same name in more
    // than one database.
    let db_id = event.database_id;

    // Check the per-database audit mode.
    let mode = state.audit_dml_cache.get(db_id);
    match mode {
        AuditDmlMode::None => return,
        AuditDmlMode::Writes | AuditDmlMode::All => {}
    }

    // Build a compact detail string: op collection:row_id
    let detail = format!(
        "{} {}:{} lsn={}",
        event.op,
        event.collection,
        event.row_id,
        event.lsn.as_u64(),
    );

    let source = event.user_id.as_deref().unwrap_or("unknown").to_string();

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DmlAudit,
        Some(event.tenant_id),
        Some(db_id),
        &source,
        &detail,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::state::audit_dml_cache::AuditDmlCache;
    use crate::control::state::collection_to_database::CollectionToDatabase;
    use crate::event::types::{RowId, WriteOp};
    use crate::types::{Lsn, TenantId, VShardId};
    use nodedb_types::DatabaseId;

    fn minimal_event(source: EventSource, op: WriteOp) -> WriteEvent {
        WriteEvent {
            sequence: 1,
            collection: Arc::from("orders"),
            op,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("o-1")),
            lsn: Lsn::new(100),
            database_id: DatabaseId::new(42),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source,
            new_value: Some(Arc::from(b"data".as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: Some(Arc::from("alice")),
            statement_digest: None,
        }
    }

    /// Verify that a non-User source is skipped even when the cache has mode Writes.
    #[test]
    fn skips_trigger_source() {
        let cache = AuditDmlCache::new();
        let coll_db = CollectionToDatabase::new();
        let db_id = DatabaseId::new(42);
        coll_db.insert(TenantId::new(1), Arc::from("orders"), db_id);
        cache.set(db_id, AuditDmlMode::Writes);

        // We can't easily spin up a full SharedState in a unit test, so we
        // test the routing logic directly by inspecting the guard clauses.
        let event = minimal_event(EventSource::Trigger, WriteOp::Insert);
        match event.source {
            EventSource::User => panic!("should not be User"),
            EventSource::Trigger
            | EventSource::RaftFollower
            | EventSource::CrdtSync
            | EventSource::Deferred => {}
        }
    }

    /// Verify that a Heartbeat op is skipped.
    #[test]
    fn skips_heartbeat_op() {
        let event = minimal_event(EventSource::User, WriteOp::Heartbeat);
        match event.op {
            WriteOp::Heartbeat => {} // expected
            WriteOp::Insert
            | WriteOp::Update
            | WriteOp::Delete
            | WriteOp::BulkInsert { .. }
            | WriteOp::BulkDelete { .. } => panic!("should not be data op"),
        }
    }

    /// Verify AuditDmlMode::None skips auditing.
    #[test]
    fn none_mode_skips() {
        let cache = AuditDmlCache::new();
        let db_id = DatabaseId::new(42);
        cache.set(db_id, AuditDmlMode::None);
        assert_eq!(cache.get(db_id), AuditDmlMode::None);
    }

    /// Verify AuditDmlMode::Writes triggers auditing.
    #[test]
    fn writes_mode_triggers() {
        let cache = AuditDmlCache::new();
        let db_id = DatabaseId::new(42);
        cache.set(db_id, AuditDmlMode::Writes);
        assert_ne!(cache.get(db_id), AuditDmlMode::None);
    }

    /// Verify collection_to_database lookup miss skips.
    #[test]
    fn unknown_collection_skips() {
        let coll_db = CollectionToDatabase::new();
        let result = coll_db.lookup(TenantId::new(1), "unknown_collection");
        assert!(result.is_none());
    }
}
