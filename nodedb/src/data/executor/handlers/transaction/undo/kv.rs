// SPDX-License-Identifier: BUSL-1.1

//! KV undo entry application logic.
//!
//! Split out of `apply.rs` (which grouped every engine family in one file)
//! once the `KvTtl` / `SortedIndexDdl` arms pushed the KV family over this
//! crate's per-file line budget.

use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::kv::current_ms;

use super::UndoEntry;

fn kv_key<'a>(
    did: u64,
    tid: u64,
    collection: &'a str,
    key: &'a [u8],
) -> crate::engine::kv::KvKeyRef<'a> {
    crate::engine::kv::KvKeyRef {
        database_id: did,
        tenant_id: tid,
        collection,
        key,
    }
}

impl CoreLoop {
    pub(super) fn apply_undo_kv(
        &mut self,
        did: u64,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::KvPut {
                collection,
                key,
                prior,
            } => {
                self.kv_engine.reinstate_entry(
                    kv_key(did, tid, &collection, &key),
                    prior.as_ref(),
                    current_ms(),
                );
                Ok(())
            }
            UndoEntry::KvDelete {
                collection,
                key,
                prior,
            } => {
                self.kv_engine.restore_entry_image(
                    kv_key(did, tid, &collection, &key),
                    &prior,
                    current_ms(),
                );
                Ok(())
            }
            UndoEntry::KvBatchPut {
                collection,
                entries,
            } => {
                let now_ms = current_ms();
                for (key, prior) in entries {
                    self.kv_engine.reinstate_entry(
                        kv_key(did, tid, &collection, &key),
                        prior.as_ref(),
                        now_ms,
                    );
                }
                Ok(())
            }
            UndoEntry::KvTransfer {
                collection,
                source_key,
                source_prior,
                dest_key,
                dest_prior,
            } => {
                let now_ms = current_ms();
                self.kv_engine.restore_entry_image(
                    kv_key(did, tid, &collection, &source_key),
                    &source_prior,
                    now_ms,
                );
                self.kv_engine.reinstate_entry(
                    kv_key(did, tid, &collection, &dest_key),
                    dest_prior.as_ref(),
                    now_ms,
                );
                Ok(())
            }
            UndoEntry::KvTransferItem {
                source_collection,
                dest_collection,
                item_key,
                dest_key,
                source_prior,
                dest_prior,
            } => {
                // Cross-collection move: the forward op deleted `item_key` from
                // `source_collection` and wrote `dest_key` in `dest_collection`.
                // Both halves are reinstated: the source row always existed,
                // the destination key may have been absent.
                let now_ms = current_ms();
                self.kv_engine.restore_entry_image(
                    kv_key(did, tid, &source_collection, &item_key),
                    &source_prior,
                    now_ms,
                );
                self.kv_engine.reinstate_entry(
                    kv_key(did, tid, &dest_collection, &dest_key),
                    dest_prior.as_ref(),
                    now_ms,
                );
                Ok(())
            }
            UndoEntry::KvTruncate { collection, rows } => {
                // Every write after the truncate was reversed first, so the
                // collection holds what the truncate left. Empty it and
                // reinstall every row it held.
                let now_ms = current_ms();
                self.kv_engine.truncate(did, tid, &collection);
                for row in rows {
                    self.kv_engine.restore_entry_image(
                        kv_key(did, tid, &collection, &row.key),
                        &crate::engine::kv::KvEntryImage {
                            value: row.value,
                            expire_at_ms: row.expire_at_ms,
                            surrogate: row.surrogate,
                        },
                        now_ms,
                    );
                }
                Ok(())
            }
            UndoEntry::KvTtl {
                collection,
                key,
                prior_expiry,
            } => {
                // The forward `Expire`/`Persist` only succeeds when the key
                // exists (mirrors the live handler's `NotFound` on an absent
                // key), so this undo entry is only ever pushed after that
                // precondition held. If a sibling undo already applied and
                // this key is now genuinely missing, that is a broken
                // invariant, not a soft "nothing to do" case.
                let restored = match prior_expiry {
                    Some(expire_at_ms) => self.kv_engine.expire_with_absolute_expiry(
                        did,
                        tid,
                        &collection,
                        &key,
                        expire_at_ms,
                    ),
                    None => self.kv_engine.persist(did, tid, &collection, &key),
                };
                if restored {
                    Ok(())
                } else {
                    let detail = format!(
                        "kv ttl undo: key missing in {collection} during rollback of Expire/Persist"
                    );
                    error!(
                        core = self.core_id,
                        entry_index,
                        error = %detail,
                        "transaction undo: kv ttl restore failed; shard state unknown"
                    );
                    Err((entry_index, detail))
                }
            }
            UndoEntry::SortedIndexDdl {
                database_id,
                tenant_id,
                index_name,
                prior_def,
            } => {
                match prior_def {
                    // An index existed under this name before the forward op
                    // (an overwritten `RegisterSortedIndex`, or the index a
                    // `DropSortedIndex` removed) -- restore it. `register`
                    // rebuilds the order-statistic tree by backfilling from
                    // the KV collection's CURRENT contents, which is correct
                    // regardless of where this undo entry falls relative to
                    // sibling KV-write undos in the log (see `UndoEntry`
                    // doc comment).
                    Some(def) => {
                        let collection = def.collection.clone();
                        self.kv_engine.register_sorted_index(
                            database_id,
                            tenant_id,
                            &collection,
                            def,
                        );
                        Ok(())
                    }
                    // No index existed under this name before the forward op
                    // (a fresh `RegisterSortedIndex`) -- undo removes it.
                    None => {
                        if self
                            .kv_engine
                            .drop_sorted_index(database_id, tenant_id, &index_name)
                        {
                            Ok(())
                        } else {
                            let detail = format!(
                                "sorted index undo: '{index_name}' missing during rollback of RegisterSortedIndex"
                            );
                            error!(
                                core = self.core_id,
                                entry_index,
                                error = %detail,
                                "transaction undo: sorted index drop failed; shard state unknown"
                            );
                            Err((entry_index, detail))
                        }
                    }
                }
            }
            _ => Err((
                entry_index,
                "apply_undo_kv called with non-kv entry".to_string(),
            )),
        }
    }
}

/// Unit tests for `Expire`/`Persist`/`RegisterSortedIndex`/`DropSortedIndex`
/// at the COMMIT-replay level (`execute_tx_sub_plan` -> `execute_tx_kv`).
///
/// `plan_requires_txn_buffering`
/// (`control/server/shared/write_admission/predicate/txn_buffering.rs`)
/// classifies all four `true` (buffered), so a client statement issued
/// inside `BEGIN ... COMMIT` replays through this exact call at COMMIT.
/// Pre-fix, `execute_tx_kv`'s reject arm returned
/// `ErrorCode::Internal { detail: "KV DDL / TTL operations are not
/// permitted inside a TransactionBatch" }` for all four, so every
/// `.expect(...)` below on the sub-plan's result is the assertion that used
/// to fail with that error.
///
/// Two of the four have no SQL surface that reaches this path today, which
/// is why these tests drive `execute_tx_sub_plan` directly rather than a
/// `sql_transactions_*.rs` pgwire integration test:
///
/// - `Expire`/`Persist` have NO pgwire SQL surface at all (no `EXPIRE(...)`
///   / `PERSIST(...)` SQL function is wired in
///   `control/server/shared/ddl/neutral/router/string_engine_ops.rs`). The
///   RESP protocol's `EXPIRE`/`PERSIST` commands exist but RESP has no
///   `MULTI`/`EXEC` transaction support, so they can never reach a `BEGIN`
///   block. Only the native binary protocol threads a `txn_id` through
///   (`control/server/native/dispatch/direct_ops.rs`) for these ops.
/// - `RegisterSortedIndex`/`DropSortedIndex` DO have pgwire SQL syntax
///   (`CREATE SORTED INDEX` / `DROP SORTED INDEX`,
///   `control/server/shared/ddl/neutral/kv_sorted_index.rs`), but that
///   handler dispatches immediately via `dispatch_to_data_plane` without
///   ever consulting the connection's transaction state -- a separate,
///   pre-existing gap where SQL sorted-index DDL is never staged at all, so
///   it cannot replay at COMMIT via that surface regardless of this fix.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::kv::current_ms;
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn put_kv(core: &mut CoreLoop, collection: &str, key: &[u8], value: &[u8], ttl_ms: u64) {
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: DB,
            tenant_id: TID,
            collection,
            key,
            value,
            ttl_ms,
            now_ms: current_ms(),
            surrogate: nodedb_types::Surrogate::ZERO,
        });
    }

    fn ttl_ms(core: &CoreLoop, collection: &str, key: &[u8]) -> Option<i64> {
        core.kv_engine
            .get_ttl_ms(DB, TID, collection, key, current_ms())
    }

    // ── Expire ───────────────────────────────────────────────────────────────

    #[test]
    fn kv_expire_in_tx_commit_replay_sets_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        put_kv(&mut core, "cache", b"k", b"v", 0);
        assert_eq!(
            ttl_ms(&core, "cache", b"k"),
            Some(-1),
            "key must start persistent (no TTL)"
        );

        let plan = PhysicalPlan::Kv(KvOp::Expire {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            ttl_ms: 5_000,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        let mut undo_log = Vec::new();
        let mut crdt_deltas = Vec::new();
        core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
            .expect("EXPIRE sub-plan must succeed at COMMIT replay");

        let remaining = ttl_ms(&core, "cache", b"k").expect("key must still exist after EXPIRE");
        assert!(
            remaining > 0 && remaining <= 5_000,
            "TTL must be set by the COMMIT replay, got {remaining}"
        );
        assert_eq!(undo_log.len(), 1, "EXPIRE must push exactly one undo entry");
        assert!(
            matches!(
                undo_log[0],
                UndoEntry::KvTtl {
                    prior_expiry: None,
                    ..
                }
            ),
            "prior state (no TTL) must be captured for rollback"
        );
    }

    #[test]
    fn kv_expire_in_tx_rollback_reverts_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        put_kv(&mut core, "cache", b"k", b"v", 0);

        let plan = PhysicalPlan::Kv(KvOp::Expire {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            ttl_ms: 5_000,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        let mut undo_log = Vec::new();
        let mut crdt_deltas = Vec::new();
        core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
            .expect("EXPIRE sub-plan must succeed");
        assert!(ttl_ms(&core, "cache", b"k").unwrap() > 0);

        // A sibling sub-plan fails later in the same COMMIT: reverse the batch.
        core.rollback_undo_log(DB, TID, undo_log)
            .expect("rollback must succeed");

        assert_eq!(
            ttl_ms(&core, "cache", b"k"),
            Some(-1),
            "rollback must revert the key to its pre-EXPIRE persistent state"
        );
    }

    // ── Persist ──────────────────────────────────────────────────────────────

    #[test]
    fn kv_persist_in_tx_commit_replay_clears_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        put_kv(&mut core, "cache", b"k", b"v", 60_000);
        assert!(
            ttl_ms(&core, "cache", b"k").unwrap() > 0,
            "key must start with a TTL"
        );

        let plan = PhysicalPlan::Kv(KvOp::Persist {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        let mut undo_log = Vec::new();
        let mut crdt_deltas = Vec::new();
        core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
            .expect("PERSIST sub-plan must succeed at COMMIT replay");

        assert_eq!(
            ttl_ms(&core, "cache", b"k"),
            Some(-1),
            "TTL must be cleared by the COMMIT replay"
        );
        assert_eq!(undo_log.len(), 1);
        assert!(
            matches!(
                undo_log[0],
                UndoEntry::KvTtl {
                    prior_expiry: Some(_),
                    ..
                }
            ),
            "prior TTL instant must be captured for rollback"
        );
    }

    #[test]
    fn kv_persist_in_tx_rollback_restores_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        put_kv(&mut core, "cache", b"k", b"v", 60_000);
        let before = ttl_ms(&core, "cache", b"k").unwrap();

        let plan = PhysicalPlan::Kv(KvOp::Persist {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        let mut undo_log = Vec::new();
        let mut crdt_deltas = Vec::new();
        core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
            .expect("PERSIST sub-plan must succeed");
        assert_eq!(ttl_ms(&core, "cache", b"k"), Some(-1));

        core.rollback_undo_log(DB, TID, undo_log)
            .expect("rollback must succeed");

        let after = ttl_ms(&core, "cache", b"k").expect("key must still exist");
        assert!(
            after > 0 && after <= before,
            "rollback must restore a TTL close to the pre-PERSIST value \
             (before={before}, after={after})"
        );
    }

    // ── RegisterSortedIndex ──────────────────────────────────────────────────

    fn seed_players(core: &mut CoreLoop) {
        for (key, score) in [("p1", 10i64), ("p2", 30), ("p3", 20)] {
            let value = nodedb_types::json_to_msgpack(&serde_json::json!({
                "player_id": key,
                "score": score,
            }))
            .unwrap();
            put_kv(core, "players", key.as_bytes(), &value, 0);
        }
    }

    fn register_plan() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "players"),
            index_name: "lb".to_string(),
            sort_columns: vec![("score".to_string(), "DESC".to_string())],
            key_column: "player_id".to_string(),
            window_type: "none".to_string(),
            window_timestamp_column: String::new(),
            window_start_ms: 0,
            window_end_ms: 0,
        })
    }

    #[test]
    fn kv_register_sorted_index_in_tx_commit_replay_is_queryable() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed_players(&mut core);

        let plan = register_plan();
        let mut undo_log = Vec::new();
        let mut crdt_deltas = Vec::new();
        core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
            .expect("RegisterSortedIndex sub-plan must succeed at COMMIT replay");

        assert_eq!(undo_log.len(), 1);
        assert!(matches!(
            undo_log[0],
            UndoEntry::SortedIndexDdl {
                prior_def: None,
                ..
            }
        ));

        let top = core
            .kv_engine
            .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
            .expect("index must be queryable immediately after COMMIT replay");
        let ranked_keys: Vec<Vec<u8>> = top.into_iter().map(|(_, pk)| pk).collect();
        assert_eq!(
            ranked_keys,
            vec![b"p2".to_vec(), b"p3".to_vec(), b"p1".to_vec()],
            "DESC top-3 must rank by score: p2(30) > p3(20) > p1(10)"
        );
    }

    #[test]
    fn kv_register_sorted_index_in_tx_rollback_removes_index() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed_players(&mut core);

        let plan = register_plan();
        let mut undo_log = Vec::new();
        let mut crdt_deltas = Vec::new();
        core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
            .expect("RegisterSortedIndex sub-plan must succeed");
        assert!(
            core.kv_engine
                .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
                .is_some()
        );

        core.rollback_undo_log(DB, TID, undo_log)
            .expect("rollback must succeed");

        assert!(
            core.kv_engine
                .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
                .is_none(),
            "rollback must remove the index a fresh RegisterSortedIndex created"
        );
    }

    // ── DropSortedIndex ──────────────────────────────────────────────────────

    /// Register `lb` live (outside a transaction), exactly as
    /// `execute_kv_register_sorted_index` would -- the def this seeds is what
    /// the `DropSortedIndex` undo entry must capture and restore.
    fn seed_live_index(core: &mut CoreLoop) {
        seed_players(core);
        let def = crate::data::executor::handlers::kv::sorted_index_compute::build_sorted_index_def(
            crate::data::executor::handlers::kv::sorted_index_compute::BuildSortedIndexDefParams {
                collection: "players",
                index_name: "lb",
                sort_columns: &[("score".to_string(), "DESC".to_string())],
                key_column: "player_id",
                window_type: "",
                window_timestamp_column: "",
                window_start_ms: 0,
                window_end_ms: 0,
            },
        )
        .expect("build sorted index def");
        core.kv_engine
            .register_sorted_index(DB, TID, "players", def);
    }

    #[test]
    fn kv_drop_sorted_index_in_tx_commit_replay_removes_it() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed_live_index(&mut core);
        assert!(
            core.kv_engine
                .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
                .is_some()
        );

        let plan = PhysicalPlan::Kv(KvOp::DropSortedIndex {
            index_name: "lb".to_string(),
        });
        let mut undo_log = Vec::new();
        let mut crdt_deltas = Vec::new();
        core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
            .expect("DropSortedIndex sub-plan must succeed at COMMIT replay");

        assert!(
            core.kv_engine
                .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
                .is_none(),
            "index must be gone after COMMIT replay"
        );
        assert_eq!(undo_log.len(), 1);
        assert!(matches!(
            undo_log[0],
            UndoEntry::SortedIndexDdl {
                prior_def: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn kv_drop_sorted_index_in_tx_rollback_restores_it() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed_live_index(&mut core);

        let plan = PhysicalPlan::Kv(KvOp::DropSortedIndex {
            index_name: "lb".to_string(),
        });
        let mut undo_log = Vec::new();
        let mut crdt_deltas = Vec::new();
        core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
            .expect("DropSortedIndex sub-plan must succeed");
        assert!(
            core.kv_engine
                .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
                .is_none()
        );

        core.rollback_undo_log(DB, TID, undo_log)
            .expect("rollback must succeed");

        let top = core
            .kv_engine
            .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
            .expect("rollback must restore the dropped index, rebuilt from live KV data");
        let ranked_keys: Vec<Vec<u8>> = top.into_iter().map(|(_, pk)| pk).collect();
        assert_eq!(
            ranked_keys,
            vec![b"p2".to_vec(), b"p3".to_vec(), b"p1".to_vec()],
            "restored index must rank identically to the original"
        );
    }

    fn seed_with_expiry_and_surrogate(core: &mut CoreLoop) -> crate::engine::kv::KvEntryImage {
        let now_ms = current_ms();
        core.kv_engine.put_with_absolute_expiry(
            crate::engine::kv::KvPutParams {
                database_id: DB,
                tenant_id: TID,
                collection: "cache",
                key: b"k",
                value: b"old",
                ttl_ms: 0,
                now_ms,
                surrogate: nodedb_types::Surrogate::new(9),
            },
            now_ms + 3_600_000,
        );
        core.kv_engine
            .entry_image(DB, TID, "cache", b"k", now_ms)
            .expect("seeded key")
    }

    #[test]
    fn a_rolled_back_overwrite_restores_the_value_expiry_and_surrogate() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let before = seed_with_expiry_and_surrogate(&mut core);
        put_kv(&mut core, "cache", b"k", b"new", 0);

        core.rollback_undo_log(
            DB,
            TID,
            vec![UndoEntry::KvPut {
                collection: "cache".into(),
                key: b"k".to_vec(),
                prior: Some(before.clone()),
            }],
        )
        .expect("rollback");

        assert_eq!(
            core.kv_engine
                .entry_image(DB, TID, "cache", b"k", current_ms()),
            Some(before)
        );
    }

    #[test]
    fn a_rolled_back_delete_restores_the_expiry_and_surrogate() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let before = seed_with_expiry_and_surrogate(&mut core);
        core.kv_engine
            .delete(DB, TID, "cache", &[b"k".to_vec()], current_ms());

        core.rollback_undo_log(
            DB,
            TID,
            vec![UndoEntry::KvDelete {
                collection: "cache".into(),
                key: b"k".to_vec(),
                prior: before.clone(),
            }],
        )
        .expect("rollback");

        assert_eq!(
            core.kv_engine
                .entry_image(DB, TID, "cache", b"k", current_ms()),
            Some(before)
        );
    }

    #[test]
    fn a_rolled_back_truncate_reinstalls_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let before = seed_with_expiry_and_surrogate(&mut core);
        let rows = core.kv_engine.export_collection(DB, TID, "cache");
        core.kv_engine.truncate(DB, TID, "cache");

        core.rollback_undo_log(
            DB,
            TID,
            vec![UndoEntry::KvTruncate {
                collection: "cache".into(),
                rows,
            }],
        )
        .expect("rollback");

        assert_eq!(
            core.kv_engine
                .entry_image(DB, TID, "cache", b"k", current_ms()),
            Some(before)
        );
    }
}
