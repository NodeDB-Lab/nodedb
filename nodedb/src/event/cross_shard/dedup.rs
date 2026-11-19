// SPDX-License-Identifier: BUSL-1.1

//! Exact-key dedup for cross-shard trigger writes.
//!
//! Every request carries a stable key: the source write's
//! `(source_vshard, source_lsn, source_sequence)` plus the `origin` tag of
//! the body that emitted it. The receiver records a key once its write
//! applies and drops any later request with that key. Keys are exact, so
//! requests can arrive in any order: a retried request is never hidden
//! behind a newer one, and two bodies fired by one source write never
//! collide.
//!
//! The key is written in the receiver's commit, inside the redo record that
//! carries the request's writes (`RedoRecord::cross_shard_applied`). Every
//! replica records it here as that record applies, and a restart restores
//! every key its WAL still holds ([`CrossShardDedup::restore_from_wal`]). So
//! the key is durable exactly when the write is: no crash leaves one without
//! the other. Keys also survive in redb past the WAL's truncation. A block
//! that wrote nothing commits no record, and the receiver records its key
//! after the commit.
//!
//! A key is pruned only after [`KEY_RETENTION_MS`]: longer than any request
//! with that key can still be sent. A request is sent automatically for at
//! most [`SEND_WINDOW_MS`] (trigger-action retries, then cross-shard
//! dispatcher retries). After that it sits in a dead-letter queue, and a
//! dead-letter entry that re-sends cross-node requests is refused requeue
//! once it is older than [`DLQ_REPLAY_WINDOW_MS`]. Bounding the DLQ replay
//! window keeps the key set bounded; keeping keys forever does not.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use nodedb_wal::record::RecordType;

use super::types::CrossShardWriteRequest;
use crate::wal::{CrossShardAppliedKey, RedoRecord, WalManager};

/// Applied keys: `(source_vshard, source_lsn, source_sequence, origin)` →
/// applied-at milliseconds.
const APPLIED: TableDefinition<(u32, u64, u64, &str), u64> =
    TableDefinition::new("cross_shard_applied");

/// The same keys ordered by applied-at, for pruning:
/// `(applied_at_ms, source_vshard, source_lsn, source_sequence, origin)`.
const APPLIED_BY_TIME: TableDefinition<(u64, u32, u64, u64, &str), ()> =
    TableDefinition::new("cross_shard_applied_by_time");

/// Longest a request is re-sent automatically: trigger-action retries (five
/// attempts, backoff under two seconds) and cross-shard dispatcher retries
/// (five attempts, backoff at most ten seconds each) fit well inside it.
pub const SEND_WINDOW_MS: u64 = 60 * 60 * 1000;

/// Longest a dead-lettered action that re-sends cross-node requests can be
/// requeued: seven days.
pub const DLQ_REPLAY_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// How long an applied key is kept. A lost acknowledgement can leave an
/// applied request in automatic retries, then the DLQ, then a requeue that
/// sends it again, so the key outlives all three.
pub const KEY_RETENTION_MS: u64 = DLQ_REPLAY_WINDOW_MS + 2 * SEND_WINDOW_MS;

/// Expired keys removed per recorded write. Bounds the write transaction.
const PRUNE_BATCH: usize = 256;

/// The dedup key of `request`, stable across every re-send.
pub fn applied_key_of(request: &CrossShardWriteRequest) -> CrossShardAppliedKey {
    CrossShardAppliedKey {
        source_vshard: request.source_vshard,
        source_lsn: request.source_lsn,
        source_sequence: request.source_sequence,
        origin: request.origin.clone(),
    }
}

fn key_tuple(key: &CrossShardAppliedKey) -> (u32, u64, u64, &str) {
    (
        key.source_vshard,
        key.source_lsn,
        key.source_sequence,
        key.origin.as_str(),
    )
}

/// Durable set of applied request keys.
///
/// Updated by the committed-redo apply, never by the receiver directly.
pub struct CrossShardDedup {
    db: Database,
    /// Keys whose redb write failed. The WAL still holds them, so a restart
    /// restores them; until then this set answers for them.
    unpersisted: std::sync::Mutex<std::collections::HashSet<CrossShardAppliedKey>>,
}

fn storage_error(what: &str, error: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "event_plane".into(),
        detail: format!("cross-shard dedup {what}: {error}"),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

impl crate::storage::RedbBacked for CrossShardDedup {
    fn redb_database(&self) -> &redb::Database {
        &self.db
    }
}

impl CrossShardDedup {
    /// Open or create the store under `data_dir/event_plane`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir)
            .map_err(|e| storage_error(&format!("create dir {}", dir.display()), e))?;

        let path = dir.join("cross_shard_dedup.redb");
        let db = Database::create(&path)
            .map_err(|e| storage_error(&format!("open {}", path.display()), e))?;

        let txn = db
            .begin_write()
            .map_err(|e| storage_error("begin_write", e))?;
        txn.open_table(APPLIED)
            .map_err(|e| storage_error("open applied table", e))?;
        txn.open_table(APPLIED_BY_TIME)
            .map_err(|e| storage_error("open applied-by-time table", e))?;
        txn.commit().map_err(|e| storage_error("commit", e))?;

        Ok(Self {
            db,
            unpersisted: std::sync::Mutex::new(std::collections::HashSet::new()),
        })
    }

    /// Whether a request with `key` already applied here.
    pub fn is_applied(&self, key: &CrossShardAppliedKey) -> crate::Result<bool> {
        if self
            .unpersisted
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(key)
        {
            return Ok(true);
        }
        let txn = self
            .db
            .begin_read()
            .map_err(|e| storage_error("begin_read", e))?;
        let table = txn
            .open_table(APPLIED)
            .map_err(|e| storage_error("open applied table", e))?;
        let found = table
            .get(key_tuple(key))
            .map_err(|e| storage_error("get", e))?;
        Ok(found.is_some())
    }

    /// Record every cross-shard key a committed redo record in `wal` carries.
    /// Run at startup, before the receiver serves: a crash between the apply
    /// and its record here leaves the key only in the WAL. Aborted records are
    /// already dropped by the WAL replay. Returns how many keys were restored.
    pub fn restore_from_wal(&self, wal: &WalManager) -> crate::Result<usize> {
        let mut restored = 0;
        for record in wal.replay()? {
            if !matches!(
                RecordType::from_raw(record.logical_record_type()),
                Some(RecordType::TransactionRedo)
            ) {
                continue;
            }
            let Some(key) = RedoRecord::from_bytes(&record.payload)?.cross_shard_applied else {
                continue;
            };
            if !self.is_applied(&key)? {
                self.record_applied(&key)?;
                restored += 1;
            }
        }
        Ok(restored)
    }

    /// Record that the request with `key` applied, and prune keys older than
    /// [`KEY_RETENTION_MS`].
    ///
    /// A failed redb write still answers for the key in memory, and returns
    /// the error for the caller to report.
    pub fn record_applied(&self, key: &CrossShardAppliedKey) -> crate::Result<()> {
        self.record_applied_at(key, now_ms()).inspect_err(|_| {
            self.unpersisted
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(key.clone());
        })
    }

    fn record_applied_at(&self, key: &CrossShardAppliedKey, now_ms: u64) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| storage_error("begin_write", e))?;
        {
            let mut applied = txn
                .open_table(APPLIED)
                .map_err(|e| storage_error("open applied table", e))?;
            let mut by_time = txn
                .open_table(APPLIED_BY_TIME)
                .map_err(|e| storage_error("open applied-by-time table", e))?;

            let (vshard, lsn, sequence, origin) = key_tuple(key);
            let previous = applied
                .insert(key_tuple(key), now_ms)
                .map_err(|e| storage_error("insert", e))?
                .map(|at| at.value());
            if let Some(previous) = previous {
                by_time
                    .remove((previous, vshard, lsn, sequence, origin))
                    .map_err(|e| storage_error("remove", e))?;
            }
            by_time
                .insert((now_ms, vshard, lsn, sequence, origin), ())
                .map_err(|e| storage_error("insert", e))?;

            let cutoff = now_ms.saturating_sub(KEY_RETENTION_MS);
            let mut expired: Vec<(u64, u32, u64, u64, String)> = Vec::new();
            for entry in by_time
                .range(..(cutoff, 0u32, 0u64, 0u64, ""))
                .map_err(|e| storage_error("range", e))?
                .take(PRUNE_BATCH)
            {
                let (guard, _) = entry.map_err(|e| storage_error("entry", e))?;
                let (at, vshard, lsn, sequence, origin) = guard.value();
                expired.push((at, vshard, lsn, sequence, origin.to_owned()));
            }
            for (at, vshard, lsn, sequence, origin) in &expired {
                by_time
                    .remove((*at, *vshard, *lsn, *sequence, origin.as_str()))
                    .map_err(|e| storage_error("remove", e))?;
                applied
                    .remove((*vshard, *lsn, *sequence, origin.as_str()))
                    .map_err(|e| storage_error("remove", e))?;
            }
        }
        txn.commit().map_err(|e| storage_error("commit", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(vshard: u32, lsn: u64, sequence: u64, origin: &str) -> CrossShardAppliedKey {
        CrossShardAppliedKey {
            source_vshard: vshard,
            source_lsn: lsn,
            source_sequence: sequence,
            origin: origin.into(),
        }
    }

    #[test]
    fn applied_key_is_a_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let store = CrossShardDedup::open(dir.path()).unwrap();
        let k = key(3, 100, 1, "trigger/1/audit");

        assert!(!store.is_applied(&k).unwrap());
        store.record_applied(&k).unwrap();
        assert!(store.is_applied(&k).unwrap());
    }

    #[test]
    fn an_older_request_is_not_hidden_by_a_newer_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = CrossShardDedup::open(dir.path()).unwrap();

        store
            .record_applied(&key(3, 200, 1, "trigger/1/a"))
            .unwrap();
        assert!(!store.is_applied(&key(3, 100, 1, "trigger/1/a")).unwrap());
    }

    #[test]
    fn bodies_of_one_source_write_do_not_collide() {
        let dir = tempfile::tempdir().unwrap();
        let store = CrossShardDedup::open(dir.path()).unwrap();

        store
            .record_applied(&key(3, 100, 1, "trigger/1/a"))
            .unwrap();
        assert!(!store.is_applied(&key(3, 100, 1, "trigger/1/b")).unwrap());
        assert!(!store.is_applied(&key(3, 100, 2, "trigger/1/a")).unwrap());
        assert!(!store.is_applied(&key(4, 100, 1, "trigger/1/a")).unwrap());
    }

    #[test]
    fn keys_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let k = key(5, 500, 9, "trigger/1/audit");
        {
            let store = CrossShardDedup::open(dir.path()).unwrap();
            store.record_applied(&k).unwrap();
        }
        let store = CrossShardDedup::open(dir.path()).unwrap();
        assert!(store.is_applied(&k).unwrap());
    }

    #[test]
    fn expired_keys_are_pruned() {
        let dir = tempfile::tempdir().unwrap();
        let store = CrossShardDedup::open(dir.path()).unwrap();
        let old = key(1, 1, 1, "trigger/1/old");
        let fresh = key(1, 2, 1, "trigger/1/fresh");

        store.record_applied_at(&old, 1_000).unwrap();
        store
            .record_applied_at(&fresh, 1_000 + KEY_RETENTION_MS + 1)
            .unwrap();

        assert!(!store.is_applied(&old).unwrap());
        assert!(store.is_applied(&fresh).unwrap());
    }

    /// A crash after the receiver's commit is durable but before its key
    /// reaches this store: the restart restores the key from the WAL, so a
    /// retried request is a duplicate and never applies again.
    #[test]
    fn a_key_committed_in_the_wal_survives_a_crash_before_its_record() {
        use crate::types::{DatabaseId, TenantId, VShardId};
        use crate::wal::RedoSubRecord;

        let dir = tempfile::tempdir().unwrap();
        let wal = WalManager::open_for_testing(&dir.path().join("dedup.wal")).unwrap();
        let committed = key(3, 100, 7, "trigger/1/audit");
        let record = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: vec![1, 2, 3],
            }],
            calvin_stamp: None,
            cross_shard_applied: Some(committed.clone()),
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
            pins: Default::default(),
        };
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::Trigger)
            .append_transaction_redo(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &record,
            )
            .unwrap();
        wal.sync().unwrap();

        // The store never saw the key: the crash hit before it was recorded.
        let store = CrossShardDedup::open(dir.path()).unwrap();
        assert!(!store.is_applied(&committed).unwrap());

        assert_eq!(store.restore_from_wal(&wal).unwrap(), 1);
        assert!(store.is_applied(&committed).unwrap());
        assert_eq!(
            store.restore_from_wal(&wal).unwrap(),
            0,
            "restore is idempotent"
        );
    }
}
