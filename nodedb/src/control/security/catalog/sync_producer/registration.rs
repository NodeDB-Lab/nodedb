// SPDX-License-Identifier: BUSL-1.1

//! `_system.sync_producers` — one row per registered Lite client, keyed by the
//! opaque `lite_id` from the handshake.
//!
//! All writes go through single-statement redb write transactions for
//! crash-safety, matching the pattern used by every other `_system.*` table.

use crate::control::security::catalog::types::{SystemCatalog, catalog_err};
use redb::{ReadableDatabase, ReadableTable};

/// Per-Lite-client producer registration rows.
///
/// Key:   `lite_id` (opaque string from the Lite handshake; typically a
///         UUID or device fingerprint).
/// Value: MessagePack-serialized [`StoredProducerRegistration`].
pub const SYNC_PRODUCERS: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("_system.sync_producers");

/// Persisted state for a single Lite client's sync producer.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, PartialEq)]
#[msgpack(map)]
pub struct StoredProducerRegistration {
    /// Stable, monotonic, per-database u64 identity for this Lite client's
    /// write stream.  Allocated from `_system.sync_producer_hwm` and never
    /// reused.
    pub producer_id: u64,

    /// Fencing epoch, advanced by `fence()` calls.  Any token issued with a
    /// lower epoch is considered stale and must be rejected.  Starts at 0 on
    /// first registration.
    pub current_epoch: u64,

    /// Internal tenant that owns this registration.
    pub tenant_id: u64,

    /// Immutable internal user that owns this registration.
    pub user_id: u64,

    /// Unix-millisecond timestamp when this registration was first created.
    pub created_ms: i64,
}

impl SystemCatalog {
    /// Persist a producer registration row, creating or overwriting it.
    ///
    /// Idempotent: re-inserting the same `lite_id` with the same record
    /// overwrites the existing row on disk (no-op at the application layer).
    pub fn put_producer_registration(
        &self,
        lite_id: &str,
        reg: &StoredProducerRegistration,
    ) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(reg)
            .map_err(|e| catalog_err("serialize producer_registration", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("sync_producers write txn", e))?;
        {
            let mut table = txn
                .open_table(SYNC_PRODUCERS)
                .map_err(|e| catalog_err("open sync_producers", e))?;
            table
                .insert(lite_id, bytes.as_slice())
                .map_err(|e| catalog_err("insert sync_producers", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("sync_producers commit", e))
    }

    /// Load the registration row for `lite_id`, or `None` if not found.
    pub fn get_producer_registration(
        &self,
        lite_id: &str,
    ) -> crate::Result<Option<StoredProducerRegistration>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("sync_producers read txn", e))?;
        let table = txn
            .open_table(SYNC_PRODUCERS)
            .map_err(|e| catalog_err("open sync_producers", e))?;
        match table
            .get(lite_id)
            .map_err(|e| catalog_err("get sync_producers", e))?
        {
            None => Ok(None),
            Some(v) => {
                let reg: StoredProducerRegistration = zerompk::from_msgpack(v.value())
                    .map_err(|e| catalog_err("deserialize producer_registration", e))?;
                Ok(Some(reg))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    fn reg(producer_id: u64, epoch: u64, tenant_id: u64) -> StoredProducerRegistration {
        StoredProducerRegistration {
            producer_id,
            current_epoch: epoch,
            tenant_id,
            user_id: 7,
            created_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn put_then_get_roundtrip() {
        let (_dir, cat) = open();
        let r = reg(1, 0, 99);
        cat.put_producer_registration("device-abc", &r).unwrap();
        let got = cat
            .get_producer_registration("device-abc")
            .unwrap()
            .unwrap();
        assert_eq!(got, r);
    }

    #[test]
    fn missing_lite_id_returns_none() {
        let (_dir, cat) = open();
        assert!(cat.get_producer_registration("nobody").unwrap().is_none());
    }

    #[test]
    fn put_is_idempotent_overwrite() {
        let (_dir, cat) = open();
        cat.put_producer_registration("dev", &reg(1, 0, 1)).unwrap();
        cat.put_producer_registration("dev", &reg(1, 1, 1)).unwrap();
        let got = cat.get_producer_registration("dev").unwrap().unwrap();
        assert_eq!(got.current_epoch, 1);
    }

    #[test]
    fn registrations_persist_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.put_producer_registration("dev-1", &reg(10, 3, 5))
                .unwrap();
        }
        let cat = SystemCatalog::open(&path).unwrap();
        let got = cat.get_producer_registration("dev-1").unwrap().unwrap();
        assert_eq!(got.producer_id, 10);
        assert_eq!(got.current_epoch, 3);
        assert_eq!(got.tenant_id, 5);
    }
}
