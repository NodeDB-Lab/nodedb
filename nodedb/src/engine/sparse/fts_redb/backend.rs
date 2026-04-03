//! Redb-backed FTS backend for Origin deployment.
//!
//! Implements `nodedb_fts::FtsBackend` over redb tables, providing
//! persistent full-text search storage for the Origin Data Plane.

use std::sync::Arc;

use redb::{Database, ReadableTable};

use nodedb_fts::backend::FtsBackend;
use nodedb_fts::posting::Posting;

use super::tables::{DOC_LENGTHS, INDEX_META, POSTINGS};

fn redb_err(ctx: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "inverted".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// Redb-backed FTS backend.
pub struct RedbFtsBackend {
    db: Arc<Database>,
}

impl RedbFtsBackend {
    /// Open or create redb tables for FTS.
    pub fn open(db: Arc<Database>) -> crate::Result<Self> {
        let write_txn = db.begin_write().map_err(|e| redb_err("init tables", e))?;
        {
            write_txn
                .open_table(POSTINGS)
                .map_err(|e| redb_err("create postings table", e))?;
            write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| redb_err("create doc_lengths table", e))?;
            write_txn
                .open_table(INDEX_META)
                .map_err(|e| redb_err("create index_meta table", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit init", e))?;

        Ok(Self { db })
    }

    /// Access the underlying database.
    pub fn db(&self) -> &Database {
        &self.db
    }
}

impl FtsBackend for RedbFtsBackend {
    type Error = crate::Error;

    fn read_postings(&self, collection: &str, term: &str) -> crate::Result<Vec<Posting>> {
        let key = format!("{collection}:{term}");
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(POSTINGS)
            .map_err(|e| redb_err("open postings", e))?;
        match table.get(key.as_str()) {
            Ok(Some(val)) => {
                let list: Vec<Posting> = rmp_serde::from_slice(val.value())
                    .map_err(|e| redb_err("deserialize postings", e))?;
                Ok(list)
            }
            Ok(None) => Ok(Vec::new()),
            Err(e) => Err(redb_err("get postings", e)),
        }
    }

    fn write_postings(
        &self,
        collection: &str,
        term: &str,
        postings: &[Posting],
    ) -> crate::Result<()> {
        let key = format!("{collection}:{term}");
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(POSTINGS)
                .map_err(|e| redb_err("open postings", e))?;
            if postings.is_empty() {
                let _ = table.remove(key.as_str());
            } else {
                let bytes = rmp_serde::to_vec_named(postings)
                    .map_err(|e| redb_err("serialize postings", e))?;
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(|e| redb_err("insert posting", e))?;
            }
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    fn remove_postings(&self, collection: &str, term: &str) -> crate::Result<()> {
        let key = format!("{collection}:{term}");
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(POSTINGS)
                .map_err(|e| redb_err("open postings", e))?;
            let _ = table.remove(key.as_str());
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    fn read_doc_length(&self, collection: &str, doc_id: &str) -> crate::Result<Option<u32>> {
        let key = format!("{collection}:{doc_id}");
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| redb_err("open doc_lengths", e))?;
        match table.get(key.as_str()) {
            Ok(Some(val)) => {
                let len: u32 = rmp_serde::from_slice(val.value())
                    .map_err(|e| redb_err("deserialize doc_length", e))?;
                Ok(Some(len))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(redb_err("get doc_length", e)),
        }
    }

    fn write_doc_length(&self, collection: &str, doc_id: &str, length: u32) -> crate::Result<()> {
        let key = format!("{collection}:{doc_id}");
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| redb_err("open doc_lengths", e))?;
            let bytes =
                rmp_serde::to_vec_named(&length).map_err(|e| redb_err("serialize doc_len", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| redb_err("insert doc_len", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    fn remove_doc_length(&self, collection: &str, doc_id: &str) -> crate::Result<()> {
        let key = format!("{collection}:{doc_id}");
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| redb_err("open doc_lengths", e))?;
            let _ = table.remove(key.as_str());
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    fn collection_terms(&self, collection: &str) -> crate::Result<Vec<String>> {
        let prefix = format!("{collection}:");
        let end = format!("{collection}:\u{ffff}");
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(POSTINGS)
            .map_err(|e| redb_err("open postings", e))?;

        let terms: Vec<String> = table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| redb_err("range", e))?
            .filter_map(|r| {
                r.ok()
                    .and_then(|(k, _)| k.value().strip_prefix(&prefix).map(String::from))
            })
            .collect();
        Ok(terms)
    }

    fn collection_stats(&self, collection: &str) -> crate::Result<(u32, u64)> {
        let meta_key = format!("stats:{collection}");
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEX_META)
            .map_err(|e| redb_err("open index_meta", e))?;
        match table.get(meta_key.as_str()) {
            Ok(Some(val)) => {
                let stats: (u32, u64) = rmp_serde::from_slice(val.value())
                    .map_err(|e| redb_err("deserialize stats", e))?;
                Ok(stats)
            }
            Ok(None) => Ok((0, 0)),
            Err(e) => Err(redb_err("get stats", e)),
        }
    }

    fn increment_stats(&self, collection: &str, doc_len: u32) -> crate::Result<()> {
        let meta_key = format!("stats:{collection}");
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(INDEX_META)
                .map_err(|e| redb_err("open index_meta", e))?;
            let (mut count, mut total) = table
                .get(meta_key.as_str())
                .ok()
                .flatten()
                .and_then(|v| rmp_serde::from_slice::<(u32, u64)>(v.value()).ok())
                .unwrap_or((0, 0));
            count += 1;
            total += doc_len as u64;
            let bytes = rmp_serde::to_vec_named(&(count, total))
                .map_err(|e| redb_err("serialize stats", e))?;
            table
                .insert(meta_key.as_str(), bytes.as_slice())
                .map_err(|e| redb_err("insert stats", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    fn decrement_stats(&self, collection: &str, doc_len: u32) -> crate::Result<()> {
        let meta_key = format!("stats:{collection}");
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(INDEX_META)
                .map_err(|e| redb_err("open index_meta", e))?;
            let (mut count, mut total) = table
                .get(meta_key.as_str())
                .ok()
                .flatten()
                .and_then(|v| rmp_serde::from_slice::<(u32, u64)>(v.value()).ok())
                .unwrap_or((0, 0));
            count = count.saturating_sub(1);
            total = total.saturating_sub(doc_len as u64);
            let bytes = rmp_serde::to_vec_named(&(count, total))
                .map_err(|e| redb_err("serialize stats", e))?;
            table
                .insert(meta_key.as_str(), bytes.as_slice())
                .map_err(|e| redb_err("insert stats", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    fn purge_collection(&self, collection: &str) -> crate::Result<usize> {
        let prefix = format!("{collection}:");
        let end = format!("{collection}:\u{ffff}");

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("purge write txn", e))?;
        let mut removed = 0;

        {
            let mut postings = write_txn
                .open_table(POSTINGS)
                .map_err(|e| redb_err("open postings", e))?;
            let keys: Vec<String> = postings
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| redb_err("postings range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            removed += keys.len();
            for key in &keys {
                let _ = postings.remove(key.as_str());
            }
        }

        {
            let mut doc_lengths = write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| redb_err("open doc_lengths", e))?;
            let keys: Vec<String> = doc_lengths
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| redb_err("doc_lengths range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            removed += keys.len();
            for key in &keys {
                let _ = doc_lengths.remove(key.as_str());
            }
        }

        // Clear incremental stats for this collection.
        {
            let mut meta = write_txn
                .open_table(INDEX_META)
                .map_err(|e| redb_err("open index_meta", e))?;
            let stats_key = format!("stats:{collection}");
            let _ = meta.remove(stats_key.as_str());
        }

        write_txn
            .commit()
            .map_err(|e| redb_err("commit purge", e))?;
        Ok(removed)
    }
}
