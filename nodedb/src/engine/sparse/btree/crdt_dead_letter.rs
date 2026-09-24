// SPDX-License-Identifier: BUSL-1.1

//! Durable dead-letter entries for rejected CRDT deltas.
//!
//! A rejected delta changes no CRDT state, and its record carries an abort
//! marker, so restart replay never reaches it. The entry it left is state of
//! its own. It is stored here when the rejection happens and read back when
//! the tenant's CRDT engine is created.
//!
//! Storage: the `crdt_dead_letters` redb table, keyed
//! `"{database_id}:{tenant_id}:{source_lsn:020}"`. The key names the record
//! that produced the entry, so storing an entry for a record twice keeps one.
//! The value is the entry as MessagePack.

use nodedb_crdt::DeadLetter;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::engine::SparseEngine;
use super::tables::redb_err;

/// Table definition for stored dead-letter entries.
pub(super) const CRDT_DEAD_LETTERS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("crdt_dead_letters");

fn dead_letter_prefix(database_id: u64, tenant_id: u64) -> String {
    format!("{database_id}:{tenant_id}:")
}

fn entry_key(database_id: u64, tenant_id: u64, source_lsn: u64) -> String {
    format!("{database_id}:{tenant_id}:{source_lsn:020}")
}

/// The tenant a key belongs to.
fn key_tenant(key: &str) -> crate::Result<u64> {
    key.split(':')
        .nth(1)
        .and_then(|tenant| tenant.parse::<u64>().ok())
        .ok_or_else(|| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("malformed crdt dead-letter key: {key}"),
        })
}

fn decode(key: &str, bytes: &[u8]) -> crate::Result<DeadLetter> {
    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("decode crdt dead-letter {key}: {e}"),
    })
}

impl SparseEngine {
    /// Store `entry`, produced by the record at `source_lsn`.
    pub fn put_crdt_dead_letter(
        &self,
        database_id: u64,
        tenant_id: u64,
        source_lsn: u64,
        entry: &DeadLetter,
    ) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(entry).map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("encode crdt dead-letter: {e}"),
        })?;
        let key = entry_key(database_id, tenant_id, source_lsn);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(CRDT_DEAD_LETTERS)
                .map_err(|e| redb_err("open crdt dead letters", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| redb_err("insert crdt dead letter", e))?;
        }
        txn.commit()
            .map_err(|e| redb_err("commit crdt dead letter", e))
    }

    /// Every stored entry of one tenant in one database, in record order.
    pub fn load_crdt_dead_letters(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<DeadLetter>> {
        let prefix = dead_letter_prefix(database_id, tenant_id);
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(CRDT_DEAD_LETTERS)
            .map_err(|e| redb_err("open crdt dead letters", e))?;
        let mut out = Vec::new();
        for entry in table
            .range(prefix.as_str()..)
            .map_err(|e| redb_err("range crdt dead letters", e))?
        {
            let (k, v) = entry.map_err(|e| redb_err("scan crdt dead letter", e))?;
            let key = k.value();
            if !key.starts_with(prefix.as_str()) {
                break;
            }
            out.push(decode(key, v.value())?);
        }
        Ok(out)
    }

    /// Remove every stored entry of one collection. Returns how many.
    pub fn delete_crdt_dead_letters_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<usize> {
        let prefix = dead_letter_prefix(database_id, tenant_id);
        self.delete_crdt_dead_letters_where(|key, entry| {
            Ok(key.starts_with(prefix.as_str()) && entry.collection == collection)
        })
    }

    /// Remove every stored entry of `tenant_id`, in every database. Returns
    /// how many.
    pub fn delete_crdt_dead_letters_for_tenant(&self, tenant_id: u64) -> crate::Result<usize> {
        self.delete_crdt_dead_letters_where(|key, _| Ok(key_tenant(key)? == tenant_id))
    }

    fn delete_crdt_dead_letters_where(
        &self,
        doomed: impl Fn(&str, &DeadLetter) -> crate::Result<bool>,
    ) -> crate::Result<usize> {
        let keys: Vec<String> = {
            let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
            let table = read_txn
                .open_table(CRDT_DEAD_LETTERS)
                .map_err(|e| redb_err("open crdt dead letters", e))?;
            let mut out = Vec::new();
            for entry in table
                .iter()
                .map_err(|e| redb_err("iter crdt dead letters", e))?
            {
                let (k, v) = entry.map_err(|e| redb_err("scan crdt dead letter", e))?;
                let key = k.value();
                if doomed(key, &decode(key, v.value())?)? {
                    out.push(key.to_string());
                }
            }
            out
        };
        if keys.is_empty() {
            return Ok(0);
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(CRDT_DEAD_LETTERS)
                .map_err(|e| redb_err("open crdt dead letters", e))?;
            for key in &keys {
                table
                    .remove(key.as_str())
                    .map_err(|e| redb_err("remove crdt dead letter", e))?;
            }
        }
        txn.commit()
            .map_err(|e| redb_err("commit crdt dead letter purge", e))?;
        Ok(keys.len())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_crdt::{
        CompensationHint, Constraint, ConstraintKind, DeadLetterQueue, EnqueueDeadLetterArgs,
    };

    use super::*;

    fn entry(collection: &str, source_lsn: u64) -> DeadLetter {
        let mut dlq = DeadLetterQueue::new(4);
        let id = dlq
            .enqueue(EnqueueDeadLetterArgs {
                peer_id: 1,
                user_id: 0,
                tenant_id: 3,
                delta: b"delta".to_vec(),
                constraint: &Constraint {
                    name: "email_unique".into(),
                    collection: collection.into(),
                    field: "email".into(),
                    kind: ConstraintKind::Unique,
                },
                reason: "duplicate".into(),
                hint: CompensationHint::ManualIntervention {
                    reason: "duplicate".into(),
                },
            })
            .expect("enqueue");
        dlq.bind_source(id, source_lsn).cloned().expect("bound")
    }

    fn open() -> (tempfile::TempDir, SparseEngine) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).expect("open");
        (dir, engine)
    }

    #[test]
    fn stored_entries_survive_a_reopen_once_per_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("sparse.redb");
        let first = entry("users", 5);
        {
            let engine = SparseEngine::open(&path).expect("open");
            engine.put_crdt_dead_letter(1, 3, 5, &first).expect("put");
            engine
                .put_crdt_dead_letter(1, 3, 5, &first)
                .expect("put again");
            engine
                .put_crdt_dead_letter(1, 4, 6, &entry("users", 6))
                .expect("put other tenant");
        }
        let engine = SparseEngine::open(&path).expect("reopen");
        assert_eq!(
            engine.load_crdt_dead_letters(1, 3).expect("load"),
            vec![first]
        );
    }

    #[test]
    fn a_purged_collection_or_tenant_leaves_no_entry() {
        let (_dir, engine) = open();
        engine
            .put_crdt_dead_letter(1, 3, 5, &entry("users", 5))
            .expect("put");
        engine
            .put_crdt_dead_letter(1, 3, 6, &entry("orders", 6))
            .expect("put");
        engine
            .put_crdt_dead_letter(2, 3, 7, &entry("users", 7))
            .expect("put");

        assert_eq!(
            engine
                .delete_crdt_dead_letters_for_collection(1, 3, "users")
                .expect("purge collection"),
            1
        );
        assert_eq!(engine.load_crdt_dead_letters(1, 3).expect("load").len(), 1);
        assert_eq!(
            engine
                .delete_crdt_dead_letters_for_tenant(3)
                .expect("purge tenant"),
            2
        );
        assert!(
            engine
                .load_crdt_dead_letters(1, 3)
                .expect("load")
                .is_empty()
        );
        assert!(
            engine
                .load_crdt_dead_letters(2, 3)
                .expect("load")
                .is_empty()
        );
    }
}
