// SPDX-License-Identifier: BUSL-1.1

//! Sequence audit log: records every GAP_FREE reservation and its outcome.
//!
//! Persisted to the `_system.sequence_log` redb table. Each entry records:
//! - sequence name, counter value, status (committed/rolled_back/crash_recycled)
//! - timestamp, user, tenant

use serde::{Deserialize, Serialize};

/// Status of a sequence reservation.
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum ReservationStatus {
    Committed = 0,
    RolledBack = 1,
    CrashRecycled = 2,
}

impl std::fmt::Display for ReservationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Committed => write!(f, "committed"),
            Self::RolledBack => write!(f, "rolled_back"),
            Self::CrashRecycled => write!(f, "crash_recycled"),
        }
    }
}

/// A single sequence log entry.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct SequenceLogEntry {
    pub sequence_name: String,
    pub value: i64,
    pub status: ReservationStatus,
    pub timestamp_ms: u64,
    pub user: String,
    pub database_id: u64,
    pub tenant_id: u64,
}

/// Append a sequence log entry to the catalog.
///
/// The entry rides the `_system.sequence_state` table as a `SequenceState`
/// row. Its name segment starts with `log:`, which no SQL identifier can
/// produce, so a log row never collides with a real counter. The row carries
/// the entry's own database, so the key is
/// `"{database_id}:{tenant_id}:log:{sequence}:{timestamp_ms}:{status}"`.
pub fn log_reservation(
    catalog: &crate::control::security::catalog::types::SystemCatalog,
    entry: &SequenceLogEntry,
) {
    let synthetic_state = crate::control::security::catalog::sequence_types::SequenceState {
        database_id: entry.database_id,
        tenant_id: entry.tenant_id,
        name: format!(
            "log:{}:{}:{}",
            entry.sequence_name, entry.timestamp_ms, entry.status
        ),
        current_value: entry.value,
        is_called: true,
        epoch: 0,
        period_key: entry.user.clone(),
        reservation: None,
    };
    if let Err(e) = catalog.put_sequence_state(&synthetic_state) {
        tracing::warn!(
            sequence = %entry.sequence_name,
            error = %e,
            "failed to persist sequence log entry"
        );
    }
}

/// Create a log entry for a committed reservation.
pub fn committed(
    sequence_name: &str,
    value: i64,
    user: &str,
    database_id: u64,
    tenant_id: u64,
) -> SequenceLogEntry {
    SequenceLogEntry {
        sequence_name: sequence_name.to_string(),
        value,
        status: ReservationStatus::Committed,
        timestamp_ms: now_ms(),
        user: user.to_string(),
        database_id,
        tenant_id,
    }
}

/// Create a log entry for a rolled-back reservation.
pub fn rolled_back(
    sequence_name: &str,
    value: i64,
    user: &str,
    database_id: u64,
    tenant_id: u64,
) -> SequenceLogEntry {
    SequenceLogEntry {
        sequence_name: sequence_name.to_string(),
        value,
        status: ReservationStatus::RolledBack,
        timestamp_ms: now_ms(),
        user: user.to_string(),
        database_id,
        tenant_id,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_serialization_roundtrip() {
        let entry = committed("invoice_seq", 42, "admin", 4, 1);
        let bytes = zerompk::to_msgpack_vec(&entry).unwrap();
        let decoded: SequenceLogEntry = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.sequence_name, "invoice_seq");
        assert_eq!(decoded.value, 42);
        assert_eq!(decoded.status, ReservationStatus::Committed);
        assert_eq!(decoded.database_id, 4);
        assert_eq!(decoded.tenant_id, 1);
    }

    #[test]
    fn status_display() {
        assert_eq!(ReservationStatus::Committed.to_string(), "committed");
        assert_eq!(ReservationStatus::RolledBack.to_string(), "rolled_back");
        assert_eq!(
            ReservationStatus::CrashRecycled.to_string(),
            "crash_recycled"
        );
    }
}
