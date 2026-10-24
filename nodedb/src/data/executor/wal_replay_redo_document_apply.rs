// SPDX-License-Identifier: BUSL-1.1

//! Restart-replay writes of one document redo sub-record.
//!
//! Each op runs through the shared core write path (`apply_point_put` /
//! `apply_point_delete`) in its own redb write transaction, with
//! `enforce = false` and no materialized-sum fold (see
//! `wal_replay_redo_document`).
//!
//! ## Hash-chained collections
//!
//! A redo put carries the SUBMITTED body. The chain link is derived at install
//! time from the head the install sees, so it is in no WAL record. Replay
//! therefore marks the row through [`ChainGuard::chain_redo_put`]: a row
//! already installed is stored again with its durable bytes and the head
//! stays, and a row the crash kept from installing links from the durable
//! head. The head advances in the row's own transaction. Re-applying a record
//! never links it twice.

use nodedb_types::Surrogate;

use super::core_loop::CoreLoop;
use super::enforcement::chain_guard::{
    AbandonedWrite, ChainGuard, abandon_write, abort_after_apply,
};
use super::handlers::point::apply_delete::PointDeleteParams;
use super::handlers::point::apply_put::PointPutParams;
use crate::engine::document::store::StorageKey;

/// One document row a restart-replay delete removes.
pub(super) struct ReplayDocDelete<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    /// The row's client key. The deleted-node tracker keys nodes by it.
    pub document_id: &'a str,
    pub surrogate: u32,
}

/// One document row a restart-replay put writes.
pub(super) struct ReplayDocPut<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub surrogate: u32,
    pub record_lsn: u64,
}

impl CoreLoop {
    /// Apply one document PUT through `apply_point_put` in its own redb write
    /// transaction. `enforce = false`: replayed writes were admission-checked
    /// when first committed. Returns whether the write was applied and
    /// committed.
    pub(super) fn apply_document_put(&mut self, row: ReplayDocPut<'_>, value: &[u8]) -> bool {
        let collection = row.collection;
        let surrogate = Surrogate::new(row.surrogate);
        let storage_key = StorageKey::for_surrogate(surrogate);

        let mut chain = ChainGuard::begin(self, row.database_id, row.tenant_id, collection);
        if let Err(e) = self.mark_replayed_put(&mut chain, &row, &storage_key, value) {
            tracing::warn!(
                core = self.core_id,
                %collection,
                error = %e,
                "WAL document redo: hash-chain link failed; skipping put"
            );
            return false;
        }

        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                chain.restore(self);
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "WAL document redo: begin_write failed; skipping put"
                );
                return false;
            }
        };
        // The row's in-memory index entries, kept so a failed settle or
        // commit can reverse them.
        let mut memory_undo = Vec::new();
        let applied = self
            .apply_point_put(
                &txn,
                PointPutParams {
                    database_id: row.database_id,
                    tid: row.tenant_id,
                    collection,
                    storage_key,
                    surrogate,
                    value,
                    index_text: true,
                    user_roles: &[],
                    enforce: false,
                    // Replay reinstalls committed history: its commit judged
                    // UNIQUE on each record's post-state.
                    unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
                    resolved_targets: &[],
                    wal_lsn: (row.record_lsn != 0).then(|| crate::types::Lsn::new(row.record_lsn)),
                    // A redo carries the submitted body: the guards inject
                    // again, and the commit already judged them.
                    guards: crate::data::executor::enforcement::typeguard::GuardScope::InjectOnly,
                },
            )
            .and_then(|mut outcome| {
                memory_undo = std::mem::take(&mut outcome.memory_undo);
                chain.settle(self, surrogate, &outcome.stored_value)
            })
            .and_then(|()| chain.persist_head(self, &txn));
        // An error drops the write txn un-committed, which rolls it back.
        let committed = applied.and_then(|()| {
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("WAL document redo commit: {e}"),
            })
        });
        match committed {
            Ok(()) => {
                self.checkpoint_coordinator.mark_dirty("sparse", 1);
                true
            }
            Err(e) => {
                let e = abort_after_apply(
                    self,
                    &mut chain,
                    AbandonedWrite::row(row.database_id, row.tenant_id, collection, &storage_key)
                        .undo(memory_undo),
                    e,
                );
                self.fail_stop_on_failed_undo(&e);
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "WAL document redo: put failed; skipping put"
                );
                false
            }
        }
    }

    /// Mark a replayed put on a hash-chained collection. A no-op when the
    /// collection declares no chain.
    fn mark_replayed_put(
        &mut self,
        chain: &mut ChainGuard,
        row: &ReplayDocPut<'_>,
        storage_key: &StorageKey,
        value: &[u8],
    ) -> crate::Result<()> {
        if !chain.enabled() {
            return Ok(());
        }
        let prior =
            self.current_row(row.database_id, row.tenant_id, row.collection, storage_key)?;
        // Restart replay applies committed records, whose origin already
        // judged any carried link.
        chain.chain_redo_put(
            self,
            Surrogate::new(row.surrogate),
            value,
            prior.as_deref(),
            true,
        )
    }

    /// Fail-stop the core when `error` reports a failed undo. An in-memory
    /// entry that did not reverse leaves the core's state unknown, so the
    /// core stops serving.
    fn fail_stop_on_failed_undo(&mut self, error: &crate::Error) {
        if let crate::Error::DataPlane(code) = error {
            self.fail_stop_on_rollback_code(code);
        }
    }

    /// Apply one document DELETE through `apply_point_delete` in its own redb
    /// write transaction. `enforce = false` for the same reason as the put
    /// path. The row's node keeps its edges: the WAL names every tombstone
    /// the delete's transaction stamped. Returns whether a row was removed.
    pub(super) fn apply_document_delete(&mut self, row: ReplayDocDelete<'_>) -> bool {
        let ReplayDocDelete {
            database_id,
            tenant_id,
            collection,
            document_id,
            surrogate: surrogate_u32,
        } = row;
        let surrogate = Surrogate::new(surrogate_u32);
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "WAL document redo: begin_write failed; skipping delete"
                );
                return false;
            }
        };
        match self.apply_point_delete(
            &txn,
            PointDeleteParams {
                database_id,
                tid: tenant_id,
                collection,
                document_id,
                surrogate,
                user_roles: &[],
                enforce: false,
                resolved_targets: &[],
            },
        ) {
            Ok(outcome) => match txn.commit() {
                Ok(()) => {
                    self.checkpoint_coordinator.mark_dirty("sparse", 1);
                    outcome.prior_value.is_some()
                }
                Err(e) => {
                    // The dropped txn reverses the durable writes only. The
                    // in-memory cascades are reversed here.
                    let storage_key = StorageKey::for_surrogate(surrogate);
                    let e = abandon_write(
                        self,
                        AbandonedWrite::row(database_id, tenant_id, collection, &storage_key)
                            .undo(outcome.memory_undo),
                        crate::Error::Storage {
                            engine: "sparse".into(),
                            detail: format!("WAL document redo commit: {e}"),
                        },
                    );
                    self.fail_stop_on_failed_undo(&e);
                    tracing::warn!(
                        core = self.core_id,
                        %collection,
                        error = %e,
                        "WAL document redo: commit failed; skipping delete"
                    );
                    false
                }
            },
            Err(e) => {
                // The write txn is dropped un-committed (rolled back) on the
                // early return.
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "WAL document redo: apply_point_delete failed; skipping delete"
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, StorageKey, Surrogate, TenantId};
    use nodedb_wal::record::{RecordType, WalRecordArgs};
    use nodedb_wal::{TombstoneSet, WalRecord};

    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::doc_format;
    use crate::data::executor::handlers::transaction::redo_apply::test_commit::doc_put_sub_record;
    use crate::engine::document::store::CollectionConfig;
    use crate::wal::RedoRecord;

    const TID: u64 = 1;
    const COLL: &str = "ledger";

    /// `(surrogate, document_id, submitted body)` in insertion order.
    type Row = (u32, String, Vec<u8>);

    fn chain_key() -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string())
    }

    /// Seed the collection's config the way boot does before WAL replay.
    fn seed(core: &mut CoreLoop) {
        let mut config = CollectionConfig::new(COLL);
        config.enforcement.append_only = true;
        config.enforcement.hash_chain = true;
        core.seed_doc_configs(&[(chain_key(), config)]);
    }

    fn rows() -> Vec<Row> {
        (1..=3u32)
            .map(|i| {
                let body = nodedb_types::json_to_msgpack(
                    &serde_json::json!({"amount": i64::from(i) * 10}),
                )
                .expect("encode body");
                (i, format!("doc-00{i}"), body)
            })
            .collect()
    }

    fn lsn_of(row: &Row) -> u64 {
        10 + u64::from(row.0)
    }

    /// The `TransactionRedo` record the commit of `row` appends to the WAL.
    fn redo_record(row: &Row) -> WalRecord {
        let (surrogate, doc_id, body) = row;
        let payload = RedoRecord {
            version: 1,
            ops: vec![doc_put_sub_record(COLL, doc_id, body, *surrogate)],
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
            lsn: lsn_of(row),
            tenant_id: TID,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    /// Install `row`'s record the way a live commit does.
    fn install(core: &mut CoreLoop, row: &Row) {
        let (surrogate, doc_id, body) = row;
        core.install_with_undo_for_test(
            TID,
            lsn_of(row),
            vec![doc_put_sub_record(COLL, doc_id, body, *surrogate)],
        );
    }

    fn replay(core: &mut CoreLoop, rows: &[Row]) {
        let records: Vec<WalRecord> = rows.iter().map(redo_record).collect();
        core.replay_transaction_redo_wal(&records, 1, &TombstoneSet::new())
            .expect("restart replay");
    }

    fn stored(core: &CoreLoop, surrogate: u32) -> Vec<u8> {
        core.sparse
            .get(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                COLL,
                &StorageKey::for_surrogate(Surrogate::new(surrogate)),
            )
            .expect("read row")
            .expect("row must exist")
    }

    fn stored_rows(core: &CoreLoop, rows: &[Row]) -> Vec<Vec<u8>> {
        rows.iter().map(|(s, _, _)| stored(core, *s)).collect()
    }

    /// The head's hash.
    fn head(core: &CoreLoop) -> Option<String> {
        core.chain
            .heads
            .get(&chain_key())
            .map(|head| head.hash.clone())
    }

    /// Walk the chain with the Data Plane verifier. Returns the last link.
    fn verified_chain(core: &CoreLoop, rows: &[Row]) -> String {
        let verdict = core
            .walk_hash_chain(DatabaseId::DEFAULT.as_u64(), TID, COLL)
            .expect("walk the chain");
        assert_eq!(verdict.broken, None, "the chain must verify from genesis");
        assert_eq!(verdict.entries, rows.len() as u64);
        verdict.last_hash
    }

    /// Restart replay of installed chained rows keeps each row's link and
    /// leaves the head where the installs put it.
    ///
    /// The redo record carries the submitted body only. Writing it back
    /// verbatim drops `_chain_hash` from every durable row.
    #[test]
    fn restart_replay_keeps_every_installed_link() {
        let dir = tempfile::tempdir().expect("tempdir");
        let rows = rows();

        let (before, head_before) = {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            seed(&mut core);
            for row in &rows {
                install(&mut core, row);
            }
            let last = verified_chain(&core, &rows);
            assert_eq!(head(&core), Some(last), "the head is the last link");
            (stored_rows(&core, &rows), head(&core))
        };

        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        seed(&mut core);
        for pass in 1..=2 {
            replay(&mut core, &rows);
            assert_eq!(
                stored_rows(&core, &rows),
                before,
                "replay pass {pass} must leave every installed chained row byte-identical"
            );
            assert_eq!(
                head(&core),
                head_before,
                "replay pass {pass} moved the head"
            );
            assert_eq!(Some(verified_chain(&core, &rows)), head_before);
        }
    }

    /// A crash after the WAL append and before the install leaves the row
    /// absent. Replay links it from the durable head, persists the new head,
    /// and a second replay changes nothing.
    #[test]
    fn restart_replay_links_rows_the_crash_kept_from_installing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let rows = rows();

        let first_row = {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            seed(&mut core);
            install(&mut core, &rows[0]);
            stored(&core, rows[0].0)
        };

        let (after, head_after) = {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            seed(&mut core);
            replay(&mut core, &rows);
            assert_eq!(stored(&core, rows[0].0), first_row);
            let last = verified_chain(&core, &rows);
            assert_eq!(head(&core), Some(last), "replay advanced the head per link");
            (stored_rows(&core, &rows), head(&core))
        };

        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        assert_eq!(
            head(&core),
            head_after,
            "replay persisted the head it advanced"
        );
        seed(&mut core);
        replay(&mut core, &rows);
        assert_eq!(stored_rows(&core, &rows), after);
        assert_eq!(head(&core), head_after);
    }

    /// The committed-redo apply over a row it already installed keeps the
    /// row's link and the head. A Calvin record takes this path in restart
    /// replay.
    #[test]
    fn a_committed_reapply_keeps_the_installed_link() {
        let dir = tempfile::tempdir().expect("tempdir");
        let rows = rows();
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        seed(&mut core);
        for row in &rows {
            install(&mut core, row);
        }
        let before = stored_rows(&core, &rows);
        let head_before = head(&core);

        install(&mut core, &rows[1]);

        assert_eq!(stored_rows(&core, &rows), before);
        assert_eq!(head(&core), head_before);
        assert_eq!(Some(verified_chain(&core, &rows)), head_before);
    }

    /// A stored row edited behind the chain's back is reported at its
    /// install-order position.
    #[test]
    fn a_tampered_stored_row_breaks_verification() {
        let dir = tempfile::tempdir().expect("tempdir");
        let rows = rows();
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        seed(&mut core);
        for row in &rows {
            install(&mut core, row);
        }
        verified_chain(&core, &rows);

        let mut doc = doc_format::decode_document(&stored(&core, rows[1].0)).expect("decode");
        doc["amount"] = serde_json::json!(999);
        core.sparse
            .put(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                COLL,
                &StorageKey::for_surrogate(Surrogate::new(rows[1].0)),
                &doc_format::encode_to_msgpack(&doc),
            )
            .expect("overwrite row");

        let verdict = core
            .walk_hash_chain(DatabaseId::DEFAULT.as_u64(), TID, COLL)
            .expect("walk the chain");
        let brk = verdict.broken.expect("a tampered row must break the chain");
        assert_eq!(brk.index, 1);
        assert_eq!(
            brk.document_id,
            Some(StorageKey::for_surrogate(Surrogate::new(rows[1].0)).to_string())
        );
        assert_eq!(verdict.entries, 1);
    }
}
