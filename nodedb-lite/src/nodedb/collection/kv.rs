//! KV collection operations for Lite.
//!
//! Two modes based on `sync_enabled`:
//!
//! - **sync off**: direct redb B-tree via `Namespace::Kv`. No Loro, no CRDT,
//!   no delta tracking. Same performance class as SQLite.
//!
//! - **sync on**: writes go to redb (source of truth) AND Loro CRDT (for
//!   delta tracking). Reads always come from redb. Sync log entries are
//!   generated for LWW replication to Origin.
//!
//! Writes are buffered in memory and flushed as a single redb transaction
//! on `kv_flush()` or when the buffer exceeds `KV_FLUSH_THRESHOLD`. An
//! in-memory overlay lets reads see uncommitted writes without hitting redb.

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::{StorageEngine, StorageEngineSync, WriteOp};

/// Prefix for KV collection names in the CRDT namespace.
const KV_CRDT_PREFIX: &str = "_kv_";

/// Flush the write buffer when it reaches this many operations.
const KV_FLUSH_THRESHOLD: usize = 1024;

/// Build the redb composite key: `{collection}\0{key}`.
fn redb_key(collection: &str, key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(collection.len() + 1 + key.len());
    k.extend_from_slice(collection.as_bytes());
    k.push(0);
    k.extend_from_slice(key.as_bytes());
    k
}

/// Extract `(collection, key)` from a redb composite key.
fn split_redb_key(composite: &[u8]) -> Option<(&str, &str)> {
    let sep = composite.iter().position(|&b| b == 0)?;
    let coll = std::str::from_utf8(&composite[..sep]).ok()?;
    let key = std::str::from_utf8(&composite[sep + 1..]).ok()?;
    Some((coll, key))
}

impl<S: StorageEngine + StorageEngineSync> NodeDbLite<S> {
    /// KV PUT: store a key-value pair.
    ///
    /// Buffered in memory — call `kv_flush()` to commit to redb, or let
    /// the auto-flush threshold handle it.
    pub fn kv_put(&self, collection: &str, key: &str, value: &[u8]) -> NodeDbResult<()> {
        let rkey = redb_key(collection, key);

        let mut buf = self.kv_write_buf.lock_or_recover();
        buf.overlay.insert(rkey.clone(), Some(value.to_vec()));
        buf.ops.push(WriteOp::Put {
            ns: Namespace::Kv,
            key: rkey,
            value: value.to_vec(),
        });
        let should_flush = buf.ops.len() >= KV_FLUSH_THRESHOLD;
        drop(buf);

        if should_flush {
            self.kv_flush_inner()?;
        }

        // Sync path: also update Loro for delta generation.
        if self.sync_enabled {
            let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
            let mut crdt = self.crdt.lock_or_recover();
            let fields: Vec<(&str, loro::LoroValue)> =
                vec![("value", loro::LoroValue::Binary(value.to_vec().into()))];
            crdt.upsert_deferred(&crdt_collection, key, &fields)
                .map_err(NodeDbError::storage)?;
        }

        Ok(())
    }

    /// KV GET: retrieve a value by key.
    ///
    /// Checks the in-memory write buffer first (for uncommitted writes),
    /// then falls through to redb.
    pub fn kv_get(&self, collection: &str, key: &str) -> NodeDbResult<Option<Vec<u8>>> {
        let rkey = redb_key(collection, key);

        // Check write buffer overlay first.
        let buf = self.kv_write_buf.lock_or_recover();
        if let Some(entry) = buf.overlay.get(&rkey) {
            return Ok(entry.clone());
        }
        drop(buf);

        // Fall through to redb.
        self.storage
            .get_sync(Namespace::Kv, &rkey)
            .map_err(NodeDbError::storage)
    }

    /// KV DELETE: remove a key.
    pub fn kv_delete(&self, collection: &str, key: &str) -> NodeDbResult<bool> {
        let rkey = redb_key(collection, key);

        let mut buf = self.kv_write_buf.lock_or_recover();
        buf.overlay.insert(rkey.clone(), None);
        buf.ops.push(WriteOp::Delete {
            ns: Namespace::Kv,
            key: rkey,
        });
        let should_flush = buf.ops.len() >= KV_FLUSH_THRESHOLD;
        drop(buf);

        if should_flush {
            self.kv_flush_inner()?;
        }

        if self.sync_enabled {
            let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
            let mut crdt = self.crdt.lock_or_recover();
            crdt.delete_deferred(&crdt_collection, key)
                .map_err(NodeDbError::storage)?;
        }

        Ok(true)
    }

    /// KV SCAN: iterate keys in sorted order starting from `cursor`.
    ///
    /// Returns up to `count` key-value pairs where key >= cursor (inclusive).
    /// Pass an empty cursor to start from the beginning of the collection.
    ///
    /// Flushes the write buffer first to ensure redb has all data, then
    /// uses redb's native B-tree range scan — O(log N + count).
    pub fn kv_scan(
        &self,
        collection: &str,
        cursor: &str,
        count: usize,
    ) -> NodeDbResult<Vec<(String, Vec<u8>)>> {
        // Flush pending writes so redb is up to date.
        self.kv_flush_inner()?;

        let start = redb_key(collection, cursor);
        let entries = self
            .storage
            .scan_range_sync(Namespace::Kv, &start, count)
            .map_err(NodeDbError::storage)?;

        let mut results = Vec::with_capacity(entries.len());
        for (composite_key, value) in entries {
            if let Some((coll, key)) = split_redb_key(&composite_key) {
                if coll != collection {
                    break;
                }
                results.push((key.to_string(), value));
            }
        }

        Ok(results)
    }

    /// Flush buffered KV writes to redb as a single transaction.
    ///
    /// Also flushes deferred CRDT deltas when sync is enabled.
    pub fn kv_flush(&self) -> NodeDbResult<usize> {
        let count = self.kv_flush_inner()?;

        if self.sync_enabled {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.flush_deltas().map_err(NodeDbError::storage)?;
        }

        Ok(count)
    }

    /// Internal: flush write buffer to redb without touching CRDT.
    fn kv_flush_inner(&self) -> NodeDbResult<usize> {
        let mut buf = self.kv_write_buf.lock_or_recover();
        if buf.ops.is_empty() {
            return Ok(0);
        }

        let ops = std::mem::take(&mut buf.ops);
        buf.overlay.clear();
        drop(buf);

        let count = ops.len();
        self.storage
            .batch_write_sync(&ops)
            .map_err(NodeDbError::storage)?;

        Ok(count)
    }

    /// List all keys in a KV collection.
    pub fn kv_keys(&self, collection: &str) -> NodeDbResult<Vec<String>> {
        // Flush pending writes first.
        self.kv_flush_inner()?;

        let prefix = redb_key(collection, "");
        let entries = self
            .storage
            .scan_range_sync(Namespace::Kv, &prefix, usize::MAX)
            .map_err(NodeDbError::storage)?;

        let mut keys = Vec::with_capacity(entries.len());
        for (composite_key, _) in entries {
            if let Some((coll, key)) = split_redb_key(&composite_key) {
                if coll != collection {
                    break;
                }
                keys.push(key.to_string());
            }
        }
        Ok(keys)
    }

    /// KV INCREMENT: atomic counter increment via CRDT counter semantics.
    ///
    /// Always uses Loro (counters need CRDT merge for correctness).
    pub fn kv_increment(&self, collection: &str, key: &str, delta: i64) -> NodeDbResult<i64> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let mut crdt = self.crdt.lock_or_recover();

        let current = match crdt.read(&crdt_collection, key) {
            Some(loro::LoroValue::Map(map)) => {
                if let Some(loro::LoroValue::I64(v)) = map.get("counter") {
                    *v
                } else {
                    0
                }
            }
            _ => 0,
        };

        let new_value = current + delta;
        let fields: Vec<(&str, loro::LoroValue)> =
            vec![("counter", loro::LoroValue::I64(new_value))];
        crdt.upsert(&crdt_collection, key, &fields)
            .map_err(NodeDbError::storage)?;

        Ok(new_value)
    }

    /// Set conflict policy for a KV collection.
    pub fn kv_set_conflict_policy(
        &self,
        collection: &str,
        policy: nodedb_crdt::CollectionPolicy,
    ) -> NodeDbResult<()> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let mut crdt = self.crdt.lock_or_recover();
        crdt.set_policy(&crdt_collection, policy);
        Ok(())
    }

    /// Subscribe to a subset of KV keys matching a pattern.
    pub fn kv_subscribe_shape(
        &self,
        collection: &str,
        key_pattern: &str,
    ) -> NodeDbResult<Vec<String>> {
        let all_keys = self.kv_keys(collection)?;
        let matched: Vec<String> = all_keys
            .into_iter()
            .filter(|k| glob_matches(key_pattern, k))
            .collect();
        Ok(matched)
    }
}

/// Simple glob matching for shape subscriptions.
fn glob_matches(pattern: &str, input: &str) -> bool {
    let pat = pattern.as_bytes();
    let inp = input.as_bytes();
    let mut pi = 0;
    let mut ii = 0;
    let mut star_pi = usize::MAX;
    let mut star_ii = 0;

    while ii < inp.len() {
        if pi < pat.len() && (pat[pi] == b'?' || pat[pi] == inp[ii]) {
            pi += 1;
            ii += 1;
        } else if pi < pat.len() && pat[pi] == b'*' {
            star_pi = pi;
            star_ii = ii;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ii += 1;
            ii = star_ii;
        } else {
            return false;
        }
    }

    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }

    pi == pat.len()
}
