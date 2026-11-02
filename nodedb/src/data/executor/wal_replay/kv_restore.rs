// SPDX-License-Identifier: BUSL-1.1

//! Replay arm for a restored KV key (`crate::wal::KvRestoreRedo`).
//!
//! A RESTORE re-issues each KV key with every version its backup holds. The
//! live row installs at the ordinal its value was set at, with its absolute
//! expiry. Each ended version installs with its own interval. The ordinals
//! come from the record, never from the apply, so every replica and every
//! replay installs the same versions, and a read at any cut answers as it
//! answered on the source.
//!
//! A record with `reassert_at` sets the backup's current state again at that
//! ordinal: the live row again, or a delete of the destination's live row.
//!
//! The arm is idempotent: the live row at its own ordinal replaces itself in
//! place, and an ended version the key already holds installs once.
//!
//! A clone copy (`clone_copy`) installs only where the key holds no version,
//! live or ended. The check reads the state the record applies over, so
//! every replica and every replay decides alike.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_note::KeyRepr;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::engine::kv::history::HistoryVersion;
use crate::wal::{KvRestoreLive, KvRestoreRedo};
use nodedb_types::Surrogate;

use super::kv_put::KvReplayRecord;

impl CoreLoop {
    /// Replay a restored KV key. `None` when the payload is not one.
    /// `Some(0)` when the record was recognized and not applied.
    pub(in crate::data::executor) fn try_replay_kv_restore(
        &mut self,
        rec: &KvReplayRecord<'_>,
        tombstones: &nodedb_wal::DatabaseTombstones<'_>,
    ) -> Option<usize> {
        let KvReplayRecord {
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
        } = *rec;
        let KvRestoreRedo {
            collection,
            key,
            surrogate,
            live,
            history,
            reassert_at,
            clone_copy,
        } = KvRestoreRedo::decode(payload)?;

        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        // A restored key names a bound row. One under `ZERO` is refused.
        let surrogate = Surrogate::new(surrogate);
        if surrogate == Surrogate::ZERO {
            self.replay_record_unapplied(
                "kv",
                "restore_identity",
                record_lsn,
                &format!("restored KV key of '{collection}' carries no surrogate"),
            );
            return Some(0);
        }
        let versions: Vec<HistoryVersion> = history
            .into_iter()
            .map(|v| HistoryVersion {
                sys_from: v.sys_from,
                sys_to: v.sys_to,
                value: v.value,
                expire_at_ms: v.expire_at_ms,
                surrogate,
            })
            .collect();
        // The ended versions merge only where they overlap nothing the key
        // holds. The live row's install truncates no interval a restored
        // version reaches, since every one ends at or below the live row's
        // start, so this check before any write gives the install's verdict.
        if let Err(overlap) = self.kv_engine.merge_key_history(
            database_id,
            tenant_id,
            &collection,
            &key,
            versions.clone(),
        ) {
            self.replay_record_unapplied("kv", "restore_overlap", record_lsn, &overlap.to_string());
            return Some(0);
        }
        if self.claim_for_validation() {
            return Some(0);
        }
        // A write into the clone wins over a copy of its source key.
        if clone_copy && self.kv_key_holds_a_version(database_id, tenant_id, &collection, &key) {
            return Some(0);
        }
        if self.recording_redo_undo() {
            // Rollback runs the log in reverse: the live row comes back
            // first, then the history it found.
            let prior_history =
                self.kv_engine
                    .key_history(database_id, tenant_id, &collection, &key);
            let prior = self
                .kv_engine
                .stored_image(database_id, tenant_id, &collection, &key);
            self.record_redo_undo([
                UndoEntry::KvHistory {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior: prior_history,
                },
                UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior,
                },
            ]);
        }

        let target = RestoredKey {
            database_id,
            tenant_id,
            collection: &collection,
            key: &key,
            now_ms,
            surrogate,
        };
        if let Some(live) = &live {
            let restored =
                self.kv_engine
                    .restore_live_row(target.put(live), live.expire_at_ms, live.sys_from);
            if let Err(e) = restored {
                self.replay_record_unapplied("kv", "restore_identity", record_lsn, &e.to_string());
                return Some(0);
            }
        }
        if let Err(overlap) =
            self.kv_engine
                .restore_key_history(database_id, tenant_id, &collection, &key, versions)
        {
            self.replay_record_unapplied("kv", "restore_overlap", record_lsn, &overlap.to_string());
            return Some(0);
        }
        // The destination held the key past the backup's newest version: the
        // backup's current state is set again above it.
        if let Some(ordinal) = reassert_at {
            match &live {
                Some(live) => {
                    let set_again = self.kv_engine.restore_live_row(
                        target.put(live),
                        live.expire_at_ms,
                        ordinal,
                    );
                    if let Err(e) = set_again {
                        self.replay_record_unapplied(
                            "kv",
                            "restore_identity",
                            record_lsn,
                            &e.to_string(),
                        );
                        return Some(0);
                    }
                }
                None => {
                    self.kv_engine.end_live_row_at(
                        database_id,
                        tenant_id,
                        &collection,
                        &key,
                        ordinal,
                    );
                }
            }
        }
        self.note_replay_write(
            database_id,
            tenant_id,
            &collection,
            Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
            record_lsn,
        );
        Some(1)
    }
}

impl CoreLoop {
    /// Whether `key` holds any version: a live row, an expired row not yet
    /// reaped, or an ended version.
    fn kv_key_holds_a_version(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
    ) -> bool {
        self.kv_engine
            .stored_image(database_id, tenant_id, collection, key)
            .is_some()
            || !self
                .kv_engine
                .key_history(database_id, tenant_id, collection, key)
                .is_empty()
    }
}

/// The key a restored KV record installs.
struct RestoredKey<'a> {
    database_id: u64,
    tenant_id: u64,
    collection: &'a str,
    key: &'a [u8],
    now_ms: u64,
    surrogate: Surrogate,
}

impl<'a> RestoredKey<'a> {
    /// The put that installs `live` under the key.
    fn put(&self, live: &'a KvRestoreLive) -> crate::engine::kv::KvPutParams<'a> {
        crate::engine::kv::KvPutParams {
            database_id: self.database_id,
            tenant_id: self.tenant_id,
            collection: self.collection,
            key: self.key,
            value: &live.value,
            // Unused by the absolute-expiry path.
            ttl_ms: 0,
            now_ms: self.now_ms,
            surrogate: self.surrogate,
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_wal::record::{RecordType, WalRecordArgs};
    use nodedb_wal::{TombstoneSet, WalRecord};

    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::wal::{KvRestoreLive, KvRestoreRedo, KvRestoreVersion, RedoRecord, RedoSubRecord};

    const TID: u64 = 1;

    fn restored() -> KvRestoreRedo {
        KvRestoreRedo {
            collection: "cache".into(),
            key: b"k".to_vec(),
            surrogate: 5,
            live: Some(KvRestoreLive {
                value: b"v2".to_vec(),
                expire_at_ms: 0,
                sys_from: 200,
            }),
            history: vec![KvRestoreVersion {
                sys_from: 100,
                sys_to: 150,
                value: b"v1".to_vec(),
                expire_at_ms: 0,
            }],
            reassert_at: None,
            clone_copy: false,
        }
    }

    fn redo_record(lsn: u64) -> WalRecord {
        redo_record_of(lsn, &restored())
    }

    fn redo_record_of(lsn: u64, restored: &KvRestoreRedo) -> WalRecord {
        let payload = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: restored.encode().expect("encode"),
            }],
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
        }
        .to_bytes()
        .expect("encode redo");
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn,
            tenant_id: TID,
            vshard_id: 0,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    /// Every version installs at its backed-up ordinal, and a second replay
    /// of the record installs nothing new.
    #[test]
    fn a_restored_key_keeps_every_ordinal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let record = redo_record(10);
        for _ in 0..2 {
            core.replay_transaction_redo_wal(
                std::slice::from_ref(&record),
                1,
                &TombstoneSet::new(),
            )
            .expect("replay");
        }
        let table = core
            .kv_engine
            .export_snapshot(0, TID, "cache")
            .expect("a table");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0].value, b"v2");
        assert_eq!(table.rows[0].sys_from, 200);
        let history = core.kv_engine.key_history(0, TID, "cache", b"k");
        assert_eq!(history.len(), 1, "the ended version installs once");
        assert_eq!((history[0].sys_from, history[0].sys_to), (100, 150));
        assert_eq!(history[0].value, b"v1");
    }

    /// A clone copy of a key the target holds is skipped: the target keeps
    /// its newer user version. A copy of a key the target lacks installs at
    /// the source ordinal.
    #[test]
    fn a_clone_copy_installs_only_where_the_key_holds_no_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let copy = |key: &[u8], surrogate: u32, sys_from: i64| KvRestoreRedo {
            collection: "cache".into(),
            key: key.to_vec(),
            surrogate,
            live: Some(KvRestoreLive {
                value: b"source".to_vec(),
                expire_at_ms: 0,
                sys_from,
            }),
            history: Vec::new(),
            reassert_at: None,
            clone_copy: true,
        };
        // The user's write into the clone: a newer version of `held`.
        let mut user = restored();
        user.key = b"held".to_vec();
        user.history = Vec::new();
        user.live = Some(KvRestoreLive {
            value: b"user".to_vec(),
            expire_at_ms: 0,
            sys_from: 900,
        });
        let records = [
            redo_record_of(10, &user),
            redo_record_of(11, &copy(b"held", 5, 100)),
            redo_record_of(12, &copy(b"fresh", 7, 100)),
        ];
        core.replay_transaction_redo_wal(&records, 1, &TombstoneSet::new())
            .expect("replay");

        let held = core
            .kv_engine
            .stored_image(0, TID, "cache", b"held")
            .expect("the user row");
        assert_eq!((held.value.as_slice(), held.sys_from), (&b"user"[..], 900));
        assert!(
            core.kv_engine
                .key_history(0, TID, "cache", b"held")
                .is_empty(),
            "the skipped copy installs no history either"
        );
        let fresh = core
            .kv_engine
            .stored_image(0, TID, "cache", b"fresh")
            .expect("the copied row");
        assert_eq!(
            (fresh.value.as_slice(), fresh.sys_from),
            (&b"source"[..], 100)
        );
    }
}
