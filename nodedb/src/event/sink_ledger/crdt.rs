// SPDX-License-Identifier: BUSL-1.1

//! The durable ledger of CRDT sync packaging.
//!
//! Packaging an event assigns it the next sequence of its collection and
//! hands the delta to the connected Lite sessions. The ledger records the
//! event's key and the collection's sequence in one redb transaction before
//! the delta is handed on. An event the ledger already holds is not packaged
//! again, and the sequence continues across a restart instead of starting
//! over at one.
//!
//! Keys at or below a core's persisted watermark are pruned: the consumer
//! never delivers them again.

use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};

use super::key::SinkEventKey;

/// Packaged event keys.
const APPLIED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("crdt_packaged");
/// Last sequence assigned per collection.
const SEQUENCES: TableDefinition<&str, u64> = TableDefinition::new("crdt_sequences");

fn storage(detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "event_plane".into(),
        detail: format!("crdt ledger: {detail}"),
    }
}

/// Durable record of packaged events and per-collection sequences.
pub struct CrdtLedger {
    db: Database,
}

impl CrdtLedger {
    /// Open or create the ledger at `{dir}/crdt_ledger.redb`.
    pub fn open(dir: &Path) -> crate::Result<Self> {
        std::fs::create_dir_all(dir)
            .map_err(|e| storage(format!("create {}: {e}", dir.display())))?;
        let path = dir.join("crdt_ledger.redb");
        let db = Database::create(&path)
            .map_err(|e| storage(format!("open {}: {e}", path.display())))?;
        let txn = db.begin_write().map_err(storage)?;
        txn.open_table(APPLIED).map_err(storage)?;
        txn.open_table(SEQUENCES).map_err(storage)?;
        txn.commit().map_err(storage)?;
        Ok(Self { db })
    }

    /// Claim the next sequence of `collection`. With a `key`, the claim is
    /// made only when the key was never packaged, and `None` means it was.
    pub fn claim(
        &self,
        key: Option<&SinkEventKey>,
        collection: &str,
    ) -> crate::Result<Option<u64>> {
        let txn = self.db.begin_write().map_err(storage)?;
        let sequence = {
            let mut applied = txn.open_table(APPLIED).map_err(storage)?;
            if let Some(key) = key {
                let bytes = key.to_bytes();
                if applied.get(bytes.as_slice()).map_err(storage)?.is_some() {
                    return Ok(None);
                }
                applied
                    .insert(bytes.as_slice(), [].as_slice())
                    .map_err(storage)?;
            }
            let mut sequences = txn.open_table(SEQUENCES).map_err(storage)?;
            let next = sequences
                .get(collection)
                .map_err(storage)?
                .map_or(0, |guard| guard.value())
                .saturating_add(1);
            sequences.insert(collection, next).map_err(storage)?;
            next
        };
        txn.commit().map_err(storage)?;
        Ok(Some(sequence))
    }

    /// Drop the keys of `core` at or below `through`.
    pub fn prune(&self, core: u32, through: u64) -> crate::Result<()> {
        let from = SinkEventKey::floor_bytes(core, 0);
        let to = SinkEventKey::floor_bytes(core, through.saturating_add(1));
        let txn = self.db.begin_write().map_err(storage)?;
        {
            let mut applied = txn.open_table(APPLIED).map_err(storage)?;
            applied
                .retain_in(from.as_slice()..to.as_slice(), |_, _| false)
                .map_err(storage)?;
        }
        txn.commit().map_err(storage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(lsn: u64) -> SinkEventKey {
        SinkEventKey {
            core: 0,
            lsn,
            occurrence: 0,
            delete: false,
            collection: "orders".into(),
            row_kind: 0,
            row: format!("o-{lsn}"),
        }
    }

    #[test]
    fn a_key_is_packaged_once_and_sequences_survive_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let ledger = CrdtLedger::open(dir.path()).expect("open");
            assert_eq!(
                ledger.claim(Some(&key(1)), "orders").expect("claim"),
                Some(1)
            );
            assert_eq!(ledger.claim(Some(&key(1)), "orders").expect("claim"), None);
            assert_eq!(ledger.claim(None, "orders").expect("claim"), Some(2));
        }
        let ledger = CrdtLedger::open(dir.path()).expect("reopen");
        assert_eq!(ledger.claim(Some(&key(1)), "orders").expect("claim"), None);
        assert_eq!(
            ledger.claim(Some(&key(2)), "orders").expect("claim"),
            Some(3)
        );

        ledger.prune(0, 1).expect("prune");
        // A pruned key is below the watermark and never delivered again; the
        // ledger no longer holds it.
        assert_eq!(
            ledger.claim(Some(&key(1)), "orders").expect("claim"),
            Some(4)
        );
        assert_eq!(ledger.claim(Some(&key(2)), "orders").expect("claim"), None);
    }
}
