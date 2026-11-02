// SPDX-License-Identifier: BUSL-1.1

//! `_system.move_tenant_journal` table definition and persisted record types.
//!
//! Key: `tenant_id (u64)`.
//! Value: MessagePack-serialized [`MoveTenantJournalEntry`].
//!
//! The journal makes `MOVE TENANT` crash-safe: on startup, the recovery path
//! scans for in-progress entries and either completes or compensates each one.
//!
//! These live with the catalog rather than with the `MOVE TENANT` workflow that
//! drives them, because the catalog is what owns the redb table: it creates the
//! table at bootstrap and it is the only module that can reach
//! `SystemCatalog::db` to read and write rows. A workflow that reaches the
//! other way — a catalog table defined above the catalog — would make the
//! lower layer depend on the higher one.

use redb::TableDefinition;

pub const MOVE_TENANT_JOURNAL: TableDefinition<u64, &[u8]> =
    TableDefinition::new("_system.move_tenant_journal");

/// Phase of the in-progress move at the time the journal entry was last written.
///
/// Every `match` on this enum must be exhaustive — no `_ =>` arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[repr(u8)]
pub enum MovePhase {
    /// Pre-flight verified; drain about to start.
    Preflight = 1,
    /// Drain issued; waiting for sessions to wind down.
    Drain = 2,
    /// Drain complete; snapshot in progress.
    Snapshot = 3,
    /// Snapshot complete; cutover Raft proposal in progress.
    Cutover = 4,
    /// Cutover succeeded; tenant is in the target database.
    Resumed = 5,
}

/// Persisted state for a single in-progress `MOVE TENANT` operation.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct MoveTenantJournalEntry {
    pub tenant_id: u64,
    pub tenant_name: String,
    pub source_db_id: u64,
    pub source_db_name: String,
    pub target_db_id: u64,
    pub target_db_name: String,
    pub phase: MovePhase,
    /// WAL LSN at the time this entry was last written.
    pub last_durable_lsn: u64,
    /// Key under which the in-cluster temporary snapshot was stored, if any.
    pub temp_snapshot_key: Option<String>,
}

impl MoveTenantJournalEntry {
    /// Return a clone of this entry with the given phase.
    pub fn with_phase(self, phase: MovePhase) -> Self {
        Self { phase, ..self }
    }

    /// Return a clone of this entry with a temp snapshot key set.
    pub fn with_temp_snapshot_key(self, key: String) -> Self {
        Self {
            temp_snapshot_key: Some(key),
            ..self
        }
    }
}
