// SPDX-License-Identifier: BUSL-1.1

//! Durable per-collection hash-chain heads for `HASH_CHAIN` collections.
//!
//! The head is the SHA-256 link the next INSERT chains from. It is durable
//! state, not a cache: without it every restart would restart every chain at
//! `GENESIS_HASH`, and `VERIFY_HASH_CHAIN` would report the first row written
//! after a restart as broken even though nothing was tampered with.
//!
//! The head is NOT rebuilt by rescanning the collection at boot, and must not
//! be "simplified" into one. `verify_chain` walks entries in INSERTION order,
//! while a storage scan yields them in surrogate (key) order. Those two orders
//! coincide only if surrogates are handed out strictly monotonically per
//! collection, and nothing guarantees that: in cluster mode each node carves a
//! disjoint HiLo batch of surrogates from the global watermark (see
//! `control::surrogate::registry`) and hands them out locally, so a row
//! inserted later can carry a lower surrogate than one inserted earlier. A
//! rescan would therefore recompute a chain that is not the one that was
//! written.
//!
//! Storage: its own `chain_heads` redb table, keyed
//! `"{database_id}:{tenant_id}:{collection}"`. A separate table is what makes
//! collision with a document row structurally impossible — document rows live
//! in `DOCUMENTS` under `"{database_id}:{tenant_id}:{collection}:{document_id}"`
//! where `document_id` is the 8-hex surrogate rendered via `StorageKey`. Storing
//! the head as a sentinel row inside `DOCUMENTS` would be worse than a
//! collision risk: every document scan is a prefix range over that table and
//! would return the head as if it were a row.

use std::collections::HashMap;

use redb::{ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use super::engine::SparseEngine;
use super::tables::redb_err;

/// Table definition for persisted hash-chain heads.
/// Key: `"{database_id}:{tenant_id}:{collection}"` → Value: 64-char hex hash.
pub(super) const CHAIN_HEADS: TableDefinition<&str, &str> = TableDefinition::new("chain_heads");

/// Build the chain-head key for one collection.
fn chain_head_key(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection}")
}

/// Split a chain-head key back into its parts.
///
/// `splitn(3, ':')` so a collection name containing `:` round-trips intact.
fn parse_chain_head_key(key: &str) -> crate::Result<(u64, u64, String)> {
    let mut parts = key.splitn(3, ':');
    let malformed = || crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("malformed chain head key: {key}"),
    };
    let database_id = parts
        .next()
        .ok_or_else(malformed)?
        .parse::<u64>()
        .map_err(|_| malformed())?;
    let tenant_id = parts
        .next()
        .ok_or_else(malformed)?
        .parse::<u64>()
        .map_err(|_| malformed())?;
    let collection = parts.next().ok_or_else(malformed)?.to_string();
    Ok((database_id, tenant_id, collection))
}

impl SparseEngine {
    /// Read one collection's persisted chain head.
    pub fn get_chain_head(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Option<String>> {
        let key = chain_head_key(database_id, tenant_id, collection);
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(CHAIN_HEADS)
            .map_err(|e| redb_err("open chain heads", e))?;
        match table.get(key.as_str()) {
            Ok(Some(v)) => Ok(Some(v.value().to_string())),
            Ok(None) => Ok(None),
            Err(e) => Err(redb_err("get chain head", e)),
        }
    }

    /// Write one collection's chain head inside an externally-owned write
    /// transaction.
    ///
    /// This is the form the write path uses: the head must land in the SAME
    /// transaction as the row whose hash it is, so head and row commit or roll
    /// back as one unit. A head that can advance without its row (or a row that
    /// lands without its head) breaks the chain exactly like the missing
    /// rehydration this table exists to fix.
    pub fn put_chain_head_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        head: &str,
    ) -> crate::Result<()> {
        let key = chain_head_key(database_id, tenant_id, collection);
        let mut table = txn
            .open_table(CHAIN_HEADS)
            .map_err(|e| redb_err("open chain heads", e))?;
        table
            .insert(key.as_str(), head)
            .map_err(|e| redb_err("insert chain head", e))?;
        Ok(())
    }

    /// Remove one collection's chain head inside an externally-owned write
    /// transaction.
    pub fn delete_chain_head_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<()> {
        let key = chain_head_key(database_id, tenant_id, collection);
        let mut table = txn
            .open_table(CHAIN_HEADS)
            .map_err(|e| redb_err("open chain heads", e))?;
        table
            .remove(key.as_str())
            .map_err(|e| redb_err("remove chain head", e))?;
        Ok(())
    }

    /// Write one collection's chain head in its own transaction.
    ///
    /// Used by transaction rollback, which restores the pre-image after the
    /// forward transaction already committed.
    pub fn put_chain_head(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        head: &str,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        self.put_chain_head_in_txn(&txn, database_id, tenant_id, collection, head)?;
        txn.commit().map_err(|e| redb_err("commit chain head", e))?;
        Ok(())
    }

    /// Remove one collection's chain head in its own transaction.
    ///
    /// Called from every lifecycle path that drops the in-memory head, so a
    /// collection that is dropped and recreated under the same name starts a
    /// fresh chain at genesis instead of resuming the dead one.
    pub fn delete_chain_head(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        self.delete_chain_head_in_txn(&txn, database_id, tenant_id, collection)?;
        txn.commit().map_err(|e| redb_err("commit chain head", e))?;
        Ok(())
    }

    /// Remove every chain head belonging to `tenant_id`, in every database.
    ///
    /// Mirrors the tenant-wide in-memory sweep in the purge handler: the
    /// persisted heads and the in-memory map must drop the same keys, or a
    /// recreated tenant resumes a chain whose rows are gone.
    ///
    /// Returns the number of heads removed.
    pub fn delete_chain_heads_for_tenant(&self, tenant_id: u64) -> crate::Result<usize> {
        let doomed: Vec<String> = {
            let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
            let table = read_txn
                .open_table(CHAIN_HEADS)
                .map_err(|e| redb_err("open chain heads", e))?;
            let mut out = Vec::new();
            for entry in table.iter().map_err(|e| redb_err("iter chain heads", e))? {
                let (k, _) = entry.map_err(|e| redb_err("scan chain head", e))?;
                let key = k.value().to_string();
                let (_, tid, _) = parse_chain_head_key(&key)?;
                if tid == tenant_id {
                    out.push(key);
                }
            }
            out
        };
        if doomed.is_empty() {
            return Ok(0);
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(CHAIN_HEADS)
                .map_err(|e| redb_err("open chain heads", e))?;
            for key in &doomed {
                table
                    .remove(key.as_str())
                    .map_err(|e| redb_err("remove chain head", e))?;
            }
        }
        txn.commit()
            .map_err(|e| redb_err("commit chain head purge", e))?;
        Ok(doomed.len())
    }

    /// Load every persisted chain head, keyed for the core's in-memory map.
    ///
    /// Called once per core at open so the first INSERT after a restart chains
    /// from the row that preceded it rather than from `GENESIS_HASH`.
    pub fn load_chain_heads(
        &self,
    ) -> crate::Result<HashMap<(nodedb_types::DatabaseId, nodedb_types::TenantId, String), String>>
    {
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(CHAIN_HEADS)
            .map_err(|e| redb_err("open chain heads", e))?;
        let mut out = HashMap::new();
        for entry in table.iter().map_err(|e| redb_err("iter chain heads", e))? {
            let (k, v) = entry.map_err(|e| redb_err("scan chain head", e))?;
            let (database_id, tenant_id, collection) = parse_chain_head_key(k.value())?;
            out.insert(
                (
                    nodedb_types::DatabaseId::new(database_id),
                    nodedb_types::TenantId::new(tenant_id),
                    collection,
                ),
                v.value().to_string(),
            );
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
        (engine, dir)
    }

    /// A chain head written inside a caller-owned transaction must be readable
    /// after that transaction commits, and must survive reopening the database —
    /// the whole point of persisting it.
    #[test]
    fn chain_heads_persist_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse.redb");
        {
            let engine = SparseEngine::open(&path).unwrap();
            let txn = engine.begin_write().unwrap();
            engine
                .put_chain_head_in_txn(&txn, 0, 1, "ledger", "abc123")
                .unwrap();
            txn.commit().unwrap();
        }
        let engine = SparseEngine::open(&path).unwrap();
        assert_eq!(
            engine.get_chain_head(0, 1, "ledger").unwrap(),
            Some("abc123".to_string())
        );
        let heads = engine.load_chain_heads().unwrap();
        assert_eq!(
            heads.get(&(
                nodedb_types::DatabaseId::new(0),
                nodedb_types::TenantId::new(1),
                "ledger".to_string()
            )),
            Some(&"abc123".to_string())
        );
    }

    /// A dropped head must not be resurrected by the tenant sweep or by a reopen.
    #[test]
    fn chain_heads_are_removable_per_collection_and_per_tenant() {
        let (engine, _dir) = open_temp();
        engine.put_chain_head(0, 1, "ledger", "h1").unwrap();
        engine.put_chain_head(0, 1, "audit", "h2").unwrap();
        engine.put_chain_head(0, 2, "ledger", "h3").unwrap();

        engine.delete_chain_head(0, 1, "ledger").unwrap();
        assert_eq!(engine.get_chain_head(0, 1, "ledger").unwrap(), None);

        assert_eq!(engine.delete_chain_heads_for_tenant(1).unwrap(), 1);
        assert_eq!(engine.get_chain_head(0, 1, "audit").unwrap(), None);
        assert_eq!(
            engine.get_chain_head(0, 2, "ledger").unwrap(),
            Some("h3".to_string())
        );
    }
}
