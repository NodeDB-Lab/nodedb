// SPDX-License-Identifier: BUSL-1.1

//! Persistent lockout-state operations for the system catalog.
//!
//! The `_system.lockout_state` table stores one row per username, keyed by the
//! username string and encoded as MessagePack. The in-memory
//! `LoginAttemptTracker` cache is rebuilt from this table on startup and kept
//! in sync on every failure and success event.

use super::types::{LOCKOUT_STATE, SystemCatalog, catalog_err};
use redb::{ReadableDatabase, ReadableTable};

/// Re-export the table definition so `system_catalog.rs` can open it during
/// catalog initialization without reaching into `types` directly.
pub(super) use super::types::LOCKOUT_STATE as LOCKOUT_STATE_TABLE;

/// Persisted form of a user's lockout state.
///
/// Timestamps are milliseconds since the Unix epoch. `last_failure_ip` is
/// stored as the canonical text form of the IP address so it is human-readable
/// in diagnostic queries. A `None` means no IP was available (e.g. in-process
/// auth paths).
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct StoredLockoutRecord {
    /// Number of consecutive failed login attempts since the last success.
    pub failed_count: u32,
    /// Epoch millisecond at which the lockout expires. `0` means not locked.
    pub locked_until_ms: u64,
    /// Epoch millisecond of the most recent failure.
    pub last_failure_ms: u64,
    /// Canonical text form of the IP address of the most recent failure.
    pub last_failure_ip: Option<String>,
}

impl SystemCatalog {
    /// Upsert a lockout record for `username`.
    ///
    /// Replaces any existing row. Called on every failure increment and on
    /// success reset.
    pub fn put_lockout_record(
        &self,
        username: &str,
        record: &StoredLockoutRecord,
    ) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(record).map_err(|e| catalog_err("serialize lockout", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("lockout write txn", e))?;
        {
            let mut table = write_txn
                .open_table(LOCKOUT_STATE)
                .map_err(|e| catalog_err("open lockout_state", e))?;
            table
                .insert(username, bytes.as_slice())
                .map_err(|e| catalog_err("put lockout", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("lockout commit", e))?;
        Ok(())
    }

    /// Remove the lockout record for `username`.
    ///
    /// Called when `record_login_success` clears a user whose state no longer
    /// needs tracking (failed_count = 0, not locked).
    pub fn delete_lockout_record(&self, username: &str) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("lockout write txn", e))?;
        {
            let mut table = write_txn
                .open_table(LOCKOUT_STATE)
                .map_err(|e| catalog_err("open lockout_state", e))?;
            table
                .remove(username)
                .map_err(|e| catalog_err("delete lockout", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("lockout commit", e))?;
        Ok(())
    }

    /// Load all lockout records. Used to rebuild the in-memory cache on startup.
    pub fn load_all_lockout_records(&self) -> crate::Result<Vec<(String, StoredLockoutRecord)>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("lockout read txn", e))?;
        let table = read_txn
            .open_table(LOCKOUT_STATE)
            .map_err(|e| catalog_err("open lockout_state", e))?;

        let mut out = Vec::new();
        for entry in table
            .range(..)
            .map_err(|e| catalog_err("range lockout_state", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read lockout", e))?;
            let record: StoredLockoutRecord = match zerompk::from_msgpack(value.value()) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        username = key.value(),
                        error = %e,
                        "skipping unparseable lockout record"
                    );
                    continue;
                }
            };
            out.push((key.value().to_string(), record));
        }
        Ok(out)
    }

    /// Remove all lockout records whose `failed_count == 0` and
    /// `locked_until_ms` is at or before `cutoff_ms`.
    ///
    /// "Expired and cleared" means the lockout window has passed and there are
    /// no pending failures to remember.  Active lockouts are always preserved.
    pub fn gc_lockout_records(&self, cutoff_ms: u64) -> crate::Result<u64> {
        // Phase 1: identify keys to remove.
        let to_delete = {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("lockout gc read txn", e))?;
            let table = read_txn
                .open_table(LOCKOUT_STATE)
                .map_err(|e| catalog_err("open lockout_state gc", e))?;

            let mut keys = Vec::new();
            for entry in table
                .range(..)
                .map_err(|e| catalog_err("range lockout_state gc", e))?
            {
                let (key, value) = entry.map_err(|e| catalog_err("read lockout gc", e))?;
                let record: StoredLockoutRecord = match zerompk::from_msgpack(value.value()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                // Prune only if cleared (no pending failures) and lock window expired.
                if record.failed_count == 0 && record.locked_until_ms <= cutoff_ms {
                    keys.push(key.value().to_string());
                }
            }
            keys
        };

        if to_delete.is_empty() {
            return Ok(0);
        }

        // Phase 2: delete.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("lockout gc write txn", e))?;
        {
            let mut table = write_txn
                .open_table(LOCKOUT_STATE)
                .map_err(|e| catalog_err("open lockout_state gc write", e))?;
            for key in &to_delete {
                table
                    .remove(key.as_str())
                    .map_err(|e| catalog_err("gc lockout remove", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("lockout gc commit", e))?;
        Ok(to_delete.len() as u64)
    }
}
