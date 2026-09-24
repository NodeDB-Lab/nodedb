// SPDX-License-Identifier: BUSL-1.1

//! KV undo entry application logic.

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
            UndoEntry::KvTtl {
                collection,
                key,
                prior_expire_at_ms,
            } => {
                if prior_expire_at_ms == crate::engine::kv::entry::NO_EXPIRY {
                    self.kv_engine.persist(did, tid, &collection, &key);
                } else {
                    self.kv_engine.expire_with_absolute_expiry(
                        did,
                        tid,
                        &collection,
                        &key,
                        prior_expire_at_ms,
                    );
                }
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
            _ => Err((
                entry_index,
                "apply_undo_kv called with non-kv entry".to_string(),
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
