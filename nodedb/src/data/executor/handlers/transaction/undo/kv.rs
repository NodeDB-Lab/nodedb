// SPDX-License-Identifier: BUSL-1.1

//! KV undo entry application logic.

use super::{UndoEntry, UndoError};
use crate::data::executor::core_loop::CoreLoop;

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
    ) -> Result<(), UndoError> {
        match entry {
            UndoEntry::KvPut {
                collection,
                key,
                prior,
            } => self
                .kv_engine
                .restore_key_version(kv_key(did, tid, &collection, &key), prior.as_ref())
                .map_err(|e| UndoError::failed(entry_index, "reversing a KV write", e)),
            UndoEntry::KvDelete {
                collection,
                key,
                prior,
            } => self
                .kv_engine
                .restore_key_version(kv_key(did, tid, &collection, &key), Some(&prior))
                .map_err(|e| UndoError::failed(entry_index, "reversing a KV delete", e)),
            UndoEntry::KvTruncate { collection, rows } => {
                // Every write after the truncate was reversed first, so the
                // collection holds what the truncate left: no live row, and
                // one ended version per row it removed.
                for row in rows {
                    let image = crate::engine::kv::KvEntryImage {
                        value: row.value,
                        expire_at_ms: row.expire_at_ms,
                        surrogate: row.surrogate,
                        sys_from: row.sys_from,
                    };
                    self.kv_engine
                        .restore_key_version(kv_key(did, tid, &collection, &row.key), Some(&image))
                        .map_err(|e| {
                            UndoError::failed(
                                entry_index,
                                format!("restoring a truncated KV row of '{collection}'"),
                                e,
                            )
                        })?;
                }
                Ok(())
            }
            UndoEntry::KvHistory {
                collection,
                key,
                prior,
            } => {
                self.kv_engine
                    .replace_key_history(did, tid, &collection, &key, prior);
                Ok(())
            }
            _ => Err(UndoError::mismatch(
                entry_index,
                "apply_undo_kv called with non-kv entry",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::engine::kv::current_ms;
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn put_kv(core: &mut CoreLoop, collection: &str, key: &[u8], value: &[u8], ttl_ms: u64) {
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: DB,
                tenant_id: TID,
                collection,
                key,
                value,
                ttl_ms,
                now_ms: current_ms(),
                surrogate: crate::engine::kv::test_support::row_surrogate(key),
            })
            .expect("a bound row writes");
    }

    fn ttl_ms(core: &CoreLoop, collection: &str, key: &[u8]) -> Option<i64> {
        core.kv_engine
            .get_ttl_ms(DB, TID, collection, key, current_ms())
    }

    fn expire_plan(ttl_ms: u64) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Expire {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            ttl_ms,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    fn persist_plan() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Persist {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    fn assert_refused_at_install(response: &Response) {
        assert_eq!(response.status, Status::Error);
        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RetryableRefusal { .. })
            ),
            "the install fails after the transaction's writes: {:?}",
            response.error_code
        );
    }

    // ── Expire / Persist ─────────────────────────────────────────────────────

    #[test]
    fn a_committed_expire_sets_the_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        put_kv(&mut core, "cache", b"k", b"v", 0);
        assert_eq!(
            ttl_ms(&core, "cache", b"k"),
            Some(-1),
            "key starts persistent"
        );

        let response =
            core.commit_plans_for_test(&make_default_task(), TID, &[expire_plan(5_000)], 30);

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let remaining = ttl_ms(&core, "cache", b"k").expect("key exists after EXPIRE");
        assert!(
            remaining > 0 && remaining <= 5_000,
            "the committed EXPIRE sets the TTL, got {remaining}"
        );
    }

    #[test]
    fn a_refused_install_keeps_the_ttl_an_expire_changed() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        put_kv(&mut core, "cache", b"k", b"v", 0);

        let response = core.commit_plans_then_refuse_for_test(
            &make_default_task(),
            TID,
            &[expire_plan(5_000)],
            31,
        );

        assert_refused_at_install(&response);
        assert_eq!(
            ttl_ms(&core, "cache", b"k"),
            Some(-1),
            "the key stays persistent"
        );
    }

    #[test]
    fn an_expire_keeps_a_value_written_between_resolve_and_install() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        put_kv(&mut core, "cache", b"k", b"before", 0);

        let response = core.commit_plans_around_for_test(
            &make_default_task(),
            TID,
            &[expire_plan(5_000)],
            34,
            |core| put_kv(core, "cache", b"k", b"after", 0),
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(
            core.kv_engine.get(DB, TID, "cache", b"k", current_ms()),
            Some(b"after".to_vec()),
            "the install changes only the expiry of the value the key holds"
        );
        let remaining = ttl_ms(&core, "cache", b"k").expect("key exists after EXPIRE");
        assert!(
            remaining > 0 && remaining <= 5_000,
            "the committed EXPIRE sets the TTL, got {remaining}"
        );
    }

    #[test]
    fn a_persist_keeps_a_value_written_between_resolve_and_install() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        put_kv(&mut core, "cache", b"k", b"before", 60_000);

        let response = core.commit_plans_around_for_test(
            &make_default_task(),
            TID,
            &[persist_plan()],
            35,
            |core| put_kv(core, "cache", b"k", b"after", 60_000),
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(
            core.kv_engine.get(DB, TID, "cache", b"k", current_ms()),
            Some(b"after".to_vec())
        );
        assert_eq!(ttl_ms(&core, "cache", b"k"), Some(-1));
    }

    #[test]
    fn a_committed_persist_clears_the_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        put_kv(&mut core, "cache", b"k", b"v", 60_000);

        let response = core.commit_plans_for_test(&make_default_task(), TID, &[persist_plan()], 32);

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(ttl_ms(&core, "cache", b"k"), Some(-1));
    }

    #[test]
    fn a_refused_install_keeps_the_ttl_a_persist_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let before = seed_with_expiry_and_surrogate(&mut core);

        let response = core.commit_plans_then_refuse_for_test(
            &make_default_task(),
            TID,
            &[persist_plan()],
            33,
        );

        assert_refused_at_install(&response);
        assert_eq!(
            core.kv_engine
                .entry_image(DB, TID, "cache", b"k", current_ms()),
            Some(before),
            "the key keeps its value, expiry instant and surrogate"
        );
    }

    fn seed_with_expiry_and_surrogate(core: &mut CoreLoop) -> crate::engine::kv::KvEntryImage {
        let now_ms = current_ms();
        core.kv_engine
            .put_with_absolute_expiry(
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
            )
            .expect("a bound row writes");
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

    /// A rolled-back TRUNCATE leaves every secondary and sorted index
    /// registered and holding the rows it held before.
    #[test]
    fn a_rolled_back_truncate_refills_every_index() {
        use crate::engine::kv::sorted_index::key::{SortColumn, SortDirection, SortKeyEncoder};
        use crate::engine::kv::sorted_index::manager::SortedIndexDef;
        use crate::engine::kv::sorted_index::window::WindowConfig;

        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let now_ms = current_ms();
        core.kv_engine
            .register_index(crate::engine::kv::RegisterIndexParams {
                database_id: DB,
                tenant_id: TID,
                collection: "leaders",
                field: "region",
                field_position: 0,
                backfill: true,
                now_ms,
            });
        core.kv_engine.register_sorted_index(
            DB,
            TID,
            "leaders",
            SortedIndexDef {
                name: "lb".into(),
                collection: "leaders".into(),
                key_column: "player_id".into(),
                encoder: SortKeyEncoder::new(vec![SortColumn {
                    name: "score".into(),
                    direction: SortDirection::Desc,
                }]),
                window: WindowConfig::none(),
            },
        );
        for (key, score) in [("p1", 10), ("p2", 30)] {
            let value = nodedb_types::json_to_msgpack(&serde_json::json!({
                "player_id": key,
                "region": "eu",
                "score": score,
            }))
            .expect("encode row");
            core.kv_engine
                .put(crate::engine::kv::KvPutParams {
                    database_id: DB,
                    tenant_id: TID,
                    collection: "leaders",
                    key: key.as_bytes(),
                    value: &value,
                    ttl_ms: 0,
                    now_ms,
                    surrogate: crate::engine::kv::test_support::row_surrogate(key.as_bytes()),
                })
                .expect("a bound row writes");
        }
        let region = nodedb_types::Value::String("eu".into());
        let indexed = |core: &CoreLoop| {
            let mut keys = core
                .kv_engine
                .index_lookup_eq(DB, TID, "leaders", "region", &region);
            keys.sort();
            keys
        };
        let before = indexed(&core);
        assert_eq!(before.len(), 2);
        let ranked_before = core.kv_engine.sorted_index_top_k(DB, TID, "lb", 10, now_ms);

        let rows = core.kv_engine.export_collection(DB, TID, "leaders");
        core.kv_engine.truncate(DB, TID, "leaders");
        assert!(indexed(&core).is_empty(), "the truncate empties the index");
        assert_eq!(
            core.kv_engine.sorted_index_top_k(DB, TID, "lb", 10, now_ms),
            Some(Vec::new())
        );

        core.rollback_undo_log(
            DB,
            TID,
            vec![UndoEntry::KvTruncate {
                collection: "leaders".into(),
                rows,
            }],
        )
        .expect("rollback");

        assert_eq!(indexed(&core), before);
        assert_eq!(
            core.kv_engine.sorted_index_top_k(DB, TID, "lb", 10, now_ms),
            ranked_before
        );
        assert_eq!(
            ranked_before.map(|ranked| ranked.len()),
            Some(2),
            "both rows rank again"
        );
    }

    /// A rolled-back overwrite at a fixed apply ordinal leaves the key's
    /// newest ordinal and history as before, so a retry of the same record
    /// does not see the rolled-back write as a newer version.
    #[test]
    fn a_rolled_back_overwrite_leaves_no_version_at_the_apply_ordinal() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let scope = core.kv_engine.open_write_scope(Some(100));
        put_kv(&mut core, "cache", b"k", b"old", 0);
        core.kv_engine.close_write_scope(scope);
        let prior = core.kv_engine.stored_image(DB, TID, "cache", b"k");

        let scope = core.kv_engine.open_write_scope(Some(500));
        put_kv(&mut core, "cache", b"k", b"new", 0);
        core.rollback_undo_log(
            DB,
            TID,
            vec![UndoEntry::KvPut {
                collection: "cache".into(),
                key: b"k".to_vec(),
                prior: prior.clone(),
            }],
        )
        .expect("rollback");
        core.kv_engine.close_write_scope(scope);

        assert_eq!(
            core.kv_engine.key_newest_ordinal(DB, TID, "cache", b"k"),
            Some(100)
        );
        assert!(
            core.kv_engine
                .key_history(DB, TID, "cache", b"k")
                .is_empty(),
            "no phantom version"
        );
        assert_eq!(core.kv_engine.stored_image(DB, TID, "cache", b"k"), prior);
    }
}
