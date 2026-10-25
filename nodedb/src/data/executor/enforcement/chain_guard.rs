// SPDX-License-Identifier: BUSL-1.1

//! Advancing — and un-advancing — a collection's hash-chain head around one
//! INSERT.
//!
//! The chain writes the row's chain fields into the body `build_stored_body`
//! produces, so the link covers the row exactly as stored. The guard sets a
//! [`ChainIntent`] for the row before `apply_point_put` and settles it after.
//! What makes it need a guard rather than a function call is the head: it is
//! advanced in memory once the row is built and persisted inside the write's
//! own transaction. Every path that abandons the write between those two
//! points has to put the head back, and there is exactly one correct pre-image
//! to put back — the one captured before the advance.
//!
//! Holding the capture in a value the caller carries is what makes "restore it"
//! a single call at each abort site instead of a rule each handler re-derives.

use nodedb_types::Surrogate;
use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::hash_chain::{self, ChainHead};
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::memory::abort_error;
use crate::engine::document::store::StorageKey;
use crate::types::{DatabaseId, TenantId};

/// The chain link a pending write asks `build_stored_body` to write.
#[derive(Debug, Clone)]
pub(in crate::data::executor) enum ChainIntent {
    /// Link a new row at position `head.seq + 1`, after `head`.
    Link { head: ChainHead },
    /// Store the durable bytes of a row an earlier apply of the same record
    /// installed.
    Keep { stored: Vec<u8> },
}

/// A collection's hash-chain head across one write.
pub(in crate::data::executor) struct ChainGuard {
    /// Key the head is tracked under, in memory and on disk.
    key: (DatabaseId, TenantId, String),
    /// Whether the collection declares `HASH_CHAIN`.
    enabled: bool,
    /// The head as it stood BEFORE this write. `None` = not a hash-chain
    /// collection; `Some(None)` = no prior head (genesis); `Some(Some(prev))` =
    /// prior head present.
    prior: Option<Option<ChainHead>>,
    /// Whether this write actually advanced the head.
    mutated: bool,
    /// Surrogates of the rows whose intent this guard set and has not settled.
    pending: Vec<u32>,
}

impl ChainGuard {
    /// Capture the head pre-image before anything touches it.
    pub(in crate::data::executor) fn begin(
        core: &CoreLoop,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Self {
        let key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        let enabled = core
            .doc_configs
            .get(&key)
            .is_some_and(|c| c.enforcement.hash_chain);
        let prior = if enabled {
            Some(core.chain.heads.get(&key).cloned())
        } else {
            None
        };
        Self {
            key,
            enabled,
            prior,
            mutated: false,
            pending: Vec::new(),
        }
    }

    /// Whether the collection declares `HASH_CHAIN`.
    pub(in crate::data::executor) fn enabled(&self) -> bool {
        self.enabled
    }

    /// Mark the row under `surrogate` as the next link of the chain.
    ///
    /// The caller stores the submitted body. `build_stored_body` writes the
    /// link, and [`Self::settle`] advances the head. A body that will not
    /// decode, or that sets a chain field, fails the insert and leaves the
    /// head untouched. A no-op when the chain is disabled.
    pub(in crate::data::executor) fn chain_insert(
        &mut self,
        core: &mut CoreLoop,
        surrogate: Surrogate,
        value: &[u8],
    ) -> crate::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let doc = doc_format::decode_document(value)?;
        hash_chain::refuse_supplied_link(&self.key.2, &doc)?;
        let head = core
            .chain
            .heads
            .get(&self.key)
            .cloned()
            .unwrap_or_else(ChainHead::genesis);
        self.set_intent(core, surrogate, ChainIntent::Link { head });
        Ok(())
    }

    /// Mark one put of a committed redo record, given the row `prior` already
    /// stored under the record's surrogate.
    ///
    /// A committed transaction's redo carries the submitted body, never the
    /// link: the link is derived at install time from the head the install
    /// sees. A restored or cloned row carries its source link. With `relink`
    /// that carried link is a relink request: the row links from the
    /// destination head, and rows sent in source position order reproduce the
    /// source links. Without `relink` a carried chain field is refused.
    ///
    /// A chained collection admits no overwrite, so a present row is an
    /// earlier install of the same row. It is stored again with its durable
    /// bytes, so its link and every evaluated expression stay as installed
    /// and the head does not move. A carried row whose contents differ from
    /// the present row is a restore conflict, refused rather than dropped.
    ///
    /// `HASH_CHAIN` is declared only at CREATE, so every row of a chained
    /// collection carries a link. A present row without one is corrupt, and
    /// the put fails.
    pub(in crate::data::executor) fn chain_redo_put(
        &mut self,
        core: &mut CoreLoop,
        surrogate: Surrogate,
        value: &[u8],
        prior: Option<&[u8]>,
        relink: bool,
    ) -> crate::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let doc = doc_format::decode_document(value)?;
        let carried = relink && hash_chain::stored_link(&doc).is_some();
        let Some(stored) = prior else {
            if !carried {
                hash_chain::refuse_supplied_link(&self.key.2, &doc)?;
            }
            let head = core
                .chain
                .heads
                .get(&self.key)
                .cloned()
                .unwrap_or_else(ChainHead::genesis);
            self.set_intent(core, surrogate, ChainIntent::Link { head });
            return Ok(());
        };
        let view = core.decode_stored_document(core.chain_config(&self.key)?, stored)?;
        if hash_chain::stored_link(&view).is_none() {
            return Err(crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!(
                    "hash-chained collection '{}': stored row {} has no chain link; \
                     the row is corrupt",
                    self.key.2,
                    surrogate.as_u32()
                ),
            });
        }
        if carried
            && core.stored_contents_of(&self.key, surrogate, value)?
                != hash_chain::canonical_contents(&view)
        {
            return Err(crate::Error::RejectedConstraint {
                collection: self.key.2.clone(),
                constraint: "HASH_CHAIN".to_string(),
                detail: format!(
                    "restore conflict: row {} already exists with different contents",
                    surrogate.as_u32()
                ),
            });
        }
        self.set_intent(
            core,
            surrogate,
            ChainIntent::Keep {
                stored: stored.to_vec(),
            },
        );
        Ok(())
    }

    /// Settle the row under `surrogate` after `apply_point_put` wrote
    /// `stored`: a new link becomes the in-memory head. Call it after every
    /// successful put, before the next row of the same write is marked.
    pub(in crate::data::executor) fn settle(
        &mut self,
        core: &mut CoreLoop,
        surrogate: Surrogate,
        stored: &[u8],
    ) -> crate::Result<()> {
        let Some(intent) = core
            .chain
            .intents
            .remove(&(self.key.clone(), surrogate.as_u32()))
        else {
            return Ok(());
        };
        self.pending
            .retain(|pending| *pending != surrogate.as_u32());
        if let ChainIntent::Link { .. } = intent {
            let view = core.decode_stored_document(core.chain_config(&self.key)?, stored)?;
            let link = hash_chain::stored_link(&view).ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "hash-chained collection '{}': row {} was stored without its link",
                    self.key.2,
                    surrogate.as_u32()
                ),
            })?;
            core.chain.heads.insert(self.key.clone(), link);
            self.mutated = true;
        }
        Ok(())
    }

    fn set_intent(&mut self, core: &mut CoreLoop, surrogate: Surrogate, intent: ChainIntent) {
        core.chain
            .intents
            .insert((self.key.clone(), surrogate.as_u32()), intent);
        self.pending.push(surrogate.as_u32());
    }

    /// Persist the advanced head inside the caller's write transaction.
    ///
    /// Head and row commit or roll back as one atomic unit: a head that can
    /// advance without its row (or a row that lands without its head) is the
    /// broken-chain bug persistence exists to prevent. A no-op when this write
    /// advanced nothing.
    pub(in crate::data::executor) fn persist_head(
        &self,
        core: &CoreLoop,
        txn: &WriteTransaction,
    ) -> crate::Result<()> {
        if !self.mutated {
            return Ok(());
        }
        let Some(head) = core.chain.heads.get(&self.key).cloned() else {
            return Ok(());
        };
        core.sparse.put_chain_head_in_txn(
            txn,
            self.key.0.as_u64(),
            self.key.1.as_u64(),
            &self.key.2,
            &head,
        )
    }

    /// Clear every unsettled intent and put the captured head pre-image back
    /// after an abandoned write.
    ///
    /// In-memory only, and correctly so: every caller aborts before its write
    /// transaction commits, so the persisted head was never written. Reversing
    /// a head that already reached disk is the rollback path's job
    /// (`undo_chain_hash`).
    pub(in crate::data::executor) fn restore(&mut self, core: &mut CoreLoop) {
        for surrogate in self.pending.drain(..) {
            core.chain.intents.remove(&(self.key.clone(), surrogate));
        }
        if !self.mutated {
            return;
        }
        match &self.prior {
            Some(None) => {
                core.chain.heads.remove(&self.key);
            }
            Some(Some(prev)) => {
                core.chain.heads.insert(self.key.clone(), prev.clone());
            }
            None => {}
        }
    }

    /// The head pre-image a durable undo entry restores on rollback.
    pub(in crate::data::executor) fn prior(&self) -> Option<Option<ChainHead>> {
        self.prior.clone()
    }
}

/// A document write abandoned after `apply_point_put` ran, before its
/// transaction commits. One write can store several rows of one collection.
pub(in crate::data::executor) struct AbandonedWrite<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// Every row `apply_point_put` was called for, including a row whose
    /// call failed: it cached the row before it failed.
    pub row_keys: &'a [StorageKey],
    /// The rows' in-memory index entries to reverse, from
    /// `PointPutOutcome::memory_undo` or `PointDeleteOutcome::memory_undo`,
    /// in the order the rows were written.
    /// A row whose `apply_point_put` failed has none: the call reverses its
    /// own entries before it returns.
    pub memory_undo: Vec<UndoEntry>,
    /// The target rows the write's enforcement updated. Empty before
    /// enforcement ran.
    pub target_writes: Vec<TargetWrite>,
}

impl<'a> AbandonedWrite<'a> {
    /// A write of one row, with no in-memory entries to reverse yet.
    pub(in crate::data::executor) fn row(
        database_id: u64,
        tid: u64,
        collection: &'a str,
        row_key: &'a StorageKey,
    ) -> Self {
        Self::rows(database_id, tid, collection, std::slice::from_ref(row_key))
    }

    /// A write of several rows, with no in-memory entries to reverse yet.
    pub(in crate::data::executor) fn rows(
        database_id: u64,
        tid: u64,
        collection: &'a str,
        row_keys: &'a [StorageKey],
    ) -> Self {
        Self {
            database_id,
            tid,
            collection,
            row_keys,
            memory_undo: Vec::new(),
            target_writes: Vec::new(),
        }
    }

    /// The rows' `PointPutOutcome::memory_undo` or
    /// `PointDeleteOutcome::memory_undo`.
    pub(in crate::data::executor) fn undo(mut self, memory_undo: Vec<UndoEntry>) -> Self {
        self.memory_undo = memory_undo;
        self
    }

    /// The target rows the write's enforcement updated.
    pub(in crate::data::executor) fn targets(mut self, target_writes: Vec<TargetWrite>) -> Self {
        self.target_writes = target_writes;
        self
    }
}

/// Undo the in-memory side effects an abort AFTER `apply_point_put` leaves
/// behind, before the caller drops its transaction uncommitted. Returns the
/// error the caller reports: `error`, or `RollbackFailed` when an in-memory
/// entry did not reverse.
///
/// Dropping the redb transaction reverses the durable writes and nothing in
/// memory. Four things stay behind, and all four are reversed here, so no
/// abort site remembers one and forgets another:
/// - the document-cache entries `apply_point_put` wrote: reads would serve
///   rows absent from storage
/// - the R-tree, vector and sparse entries of the rows: spatial predicates
///   and vector search would answer with them
/// - the same cache and index entries of every materialized-sum target row
/// - the advanced hash-chain head
pub(in crate::data::executor) fn abort_after_apply(
    core: &mut CoreLoop,
    guard: &mut ChainGuard,
    write: AbandonedWrite<'_>,
    error: crate::Error,
) -> crate::Error {
    guard.restore(core);
    abandon_write(core, write, error)
}

/// [`abort_after_apply`] for a write that set no hash-chain intent.
pub(in crate::data::executor) fn abandon_write(
    core: &mut CoreLoop,
    write: AbandonedWrite<'_>,
    error: crate::Error,
) -> crate::Error {
    let AbandonedWrite {
        database_id,
        tid,
        collection,
        row_keys,
        memory_undo,
        target_writes,
    } = write;
    for row_key in row_keys {
        core.doc_cache
            .invalidate(database_id, tid, collection, row_key);
    }
    // A target is written after its row, so targets are reversed first.
    let targets = core.abandon_target_writes(database_id, tid, target_writes);
    let row = core.undo_memory_effects(database_id, tid, memory_undo);
    abort_error(error, targets.and(row))
}

impl CoreLoop {
    /// The current stored body of a row, from the versioned table on a
    /// bitemporal collection.
    pub(in crate::data::executor) fn current_row(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        storage_key: &crate::engine::document::store::StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        self.current_row_body(database_id, tid, collection, storage_key)
    }

    /// The config of a hash-chained collection.
    fn chain_config(
        &self,
        key: &(DatabaseId, TenantId, String),
    ) -> crate::Result<&crate::engine::document::store::CollectionConfig> {
        self.doc_configs
            .get(key)
            .ok_or_else(|| crate::Error::CollectionNotFound {
                tenant_id: key.1,
                collection: key.2.clone(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ErrorCode;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::transaction::redo_apply::test_commit::doc_put_sub_record;
    use crate::engine::document::store::CollectionConfig;
    use nodedb_physical::physical_plan::RedoOrigin;
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

    const TID: u64 = 1;
    const COLL: &str = "ledger";

    fn key() -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string())
    }

    fn register(core: &mut CoreLoop) {
        let mut config = CollectionConfig::new(COLL);
        config.enforcement.append_only = true;
        config.enforcement.hash_chain = true;
        core.doc_configs.insert(key(), config);
    }

    fn body(amount: i64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({"amount": amount})).expect("encode")
    }

    fn mark(core: &mut CoreLoop, surrogate: u32, value: &[u8]) -> crate::Result<()> {
        let mut guard = ChainGuard::begin(core, DatabaseId::DEFAULT.as_u64(), TID, COLL);
        guard.chain_insert(core, Surrogate::new(surrogate), value)
    }

    fn put_plan(surrogate: u32, value: Vec<u8>) -> PhysicalPlan {
        let doc_id = format!("doc-{surrogate}");
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
            document_id: doc_id.clone(),
            value,
            surrogate: Surrogate::new(surrogate),
            pk_bytes: doc_id.into_bytes(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        })
    }

    /// An unreadable body must fail the insert, never be hashed as an empty
    /// document: a link over nothing the row contains switches off the
    /// tamper evidence the chain exists for.
    #[test]
    fn an_undecodable_body_fails_the_insert_and_leaves_the_head() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        let mut value = body(10);
        value.push(0xC0);
        assert!(mark(&mut core, 1, &value).is_err());
        assert!(
            core.chain.heads.is_empty(),
            "a failed insert must not advance the head"
        );
        assert!(
            core.chain.intents.is_empty(),
            "a failed insert must mark nothing"
        );
    }

    #[test]
    fn a_supplied_chain_field_fails_the_insert() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        let value = nodedb_types::json_to_msgpack(&serde_json::json!({"_chain_hash": "x"}))
            .expect("encode");
        assert!(matches!(
            mark(&mut core, 1, &value),
            Err(crate::Error::RejectedConstraint { .. })
        ));
        assert!(core.chain.intents.is_empty());
    }

    /// Each committed row stores the link that became the head, one position
    /// after the previous one, and no intent outlives its write.
    #[test]
    fn each_link_advances_the_head_one_position() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        let task = make_default_task();
        for surrogate in 1..=2u32 {
            let resp = core.commit_plans_for_test(
                &task,
                TID,
                &[put_plan(surrogate, body(i64::from(surrogate)))],
                10 + u64::from(surrogate),
            );
            assert_eq!(resp.status, Status::Ok);
            let stored = core
                .sparse
                .get(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    COLL,
                    &nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate)),
                )
                .expect("read")
                .expect("row");
            let view = doc_format::decode_document(&stored).expect("decode");
            let link = hash_chain::stored_link(&view).expect("the row stores its link");
            assert_eq!(link.seq, u64::from(surrogate));
            assert_eq!(
                core.chain.heads.get(&key()),
                Some(&link),
                "the link is the head"
            );
        }
        assert!(core.chain.intents.is_empty());
    }

    #[test]
    fn a_disabled_chain_is_not_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        assert!(mark(&mut core, 1, b"anything at all").is_ok());
        assert!(core.chain.intents.is_empty());
    }

    /// The head is durable: a reopened core resumes the chain where the
    /// previous process left it, and the chain verifies across the restart.
    #[test]
    fn the_chain_head_survives_a_restart() {
        let db = DatabaseId::DEFAULT;
        let dir = tempfile::tempdir().expect("tempdir");
        let task = make_default_task();

        let head_before_restart = {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            register(&mut core);
            for surrogate in 1..=2u32 {
                let resp = core.commit_plans_for_test(
                    &task,
                    TID,
                    &[put_plan(surrogate, body(i64::from(surrogate)))],
                    10 + u64::from(surrogate),
                );
                assert_eq!(resp.status, Status::Ok, "pre-restart insert must succeed");
            }
            core.chain.heads.get(&key()).cloned()
        };
        assert_eq!(head_before_restart.as_ref().map(|head| head.seq), Some(2));

        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        assert_eq!(
            core.chain.heads.get(&key()).cloned(),
            head_before_restart,
            "the reopened core must rehydrate the persisted head"
        );
        register(&mut core);
        let resp = core.commit_plans_for_test(&task, TID, &[put_plan(3, body(3))], 13);
        assert_eq!(resp.status, Status::Ok, "post-restart insert must succeed");

        let verdict = core
            .walk_hash_chain(db.as_u64(), TID, COLL)
            .expect("walk the chain");
        assert_eq!(
            verdict.broken, None,
            "the chain must verify across the restart"
        );
        assert_eq!(verdict.entries, 3);
    }

    /// A bitemporal chained collection keeps its rows on the versioned table.
    /// Append-only refuses UPDATE and DELETE, so every row holds exactly one
    /// version, and walking the current versions walks the whole chain.
    #[test]
    fn a_bitemporal_chain_verifies() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let mut config = CollectionConfig::new(COLL);
        config.enforcement.append_only = true;
        config.enforcement.hash_chain = true;
        config.bitemporal = true;
        core.doc_configs.insert(key(), config);
        let task = make_default_task();
        for surrogate in 1..=2u32 {
            let resp = core.commit_plans_for_test(
                &task,
                TID,
                &[put_plan(surrogate, body(i64::from(surrogate)))],
                10 + u64::from(surrogate),
            );
            assert_eq!(resp.status, Status::Ok);
        }
        let verdict = core
            .walk_hash_chain(DatabaseId::DEFAULT.as_u64(), TID, COLL)
            .expect("walk the chain");
        assert_eq!(verdict.broken, None);
        assert_eq!(verdict.entries, 2);
    }

    /// The link covers the row as stored, generated columns included, and
    /// the verifier recomputes it from the stored bytes alone.
    #[test]
    fn the_link_covers_generated_columns_as_stored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let (expr, depends_on) =
            nodedb_query::expr_parse::parse_generated_expr("amount * 2").expect("parse");
        let mut config = CollectionConfig::new(COLL);
        config.enforcement.append_only = true;
        config.enforcement.hash_chain = true;
        config.enforcement.generated_columns =
            vec![nodedb_physical::physical_plan::GeneratedColumnSpec {
                name: "doubled".to_string(),
                expr,
                depends_on,
            }];
        core.doc_configs.insert(key(), config);
        let task = make_default_task();
        let resp = core.commit_plans_for_test(&task, TID, &[put_plan(1, body(21))], 11);
        assert_eq!(resp.status, Status::Ok);

        let stored = core
            .sparse
            .get(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                COLL,
                &nodedb_types::StorageKey::for_surrogate(Surrogate::new(1)),
            )
            .expect("read")
            .expect("row");
        let view = doc_format::decode_document(&stored).expect("decode");
        assert_eq!(view.get("doubled").and_then(|v| v.as_f64()), Some(42.0));
        let verdict = core
            .walk_hash_chain(DatabaseId::DEFAULT.as_u64(), TID, COLL)
            .expect("walk the chain");
        assert_eq!(verdict.broken, None);
        assert_eq!(verdict.entries, 1);
    }

    fn stored_row(core: &CoreLoop, surrogate: u32) -> Vec<u8> {
        core.sparse
            .get(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                COLL,
                &nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate)),
            )
            .expect("read")
            .expect("row")
    }

    fn stored_link_of(core: &CoreLoop, surrogate: u32) -> Option<ChainHead> {
        hash_chain::stored_link(
            &doc_format::decode_document(&stored_row(core, surrogate)).expect("decode"),
        )
    }

    /// Chained rows committed on a source core, as a backup captures them:
    /// `(stored body, link)` in position order.
    fn source_rows(dir: &std::path::Path) -> Vec<(Vec<u8>, ChainHead)> {
        let (mut core, _req, _resp) = make_core_with_dir(dir);
        register(&mut core);
        let task = make_default_task();
        for surrogate in [3u32, 1, 2] {
            let resp = core.commit_plans_for_test(
                &task,
                TID,
                &[put_plan(surrogate, body(i64::from(surrogate) * 10))],
                10 + u64::from(surrogate),
            );
            assert_eq!(resp.status, Status::Ok);
        }
        let mut rows: Vec<(Vec<u8>, ChainHead)> = [3u32, 1, 2]
            .into_iter()
            .map(|s| {
                let link = stored_link_of(&core, s).expect("source link");
                (stored_row(&core, s), link)
            })
            .collect();
        rows.sort_by_key(|(_, link)| link.seq);
        rows
    }

    fn restore(core: &mut CoreLoop, lsn: u64, surrogate: u32, body: &[u8]) -> Option<ErrorCode> {
        let op = doc_put_sub_record(COLL, &format!("r{surrogate}"), body, surrogate);
        core.install_from_for_test(TID, lsn, vec![op], RedoOrigin::Restore)
            .1
    }

    /// A restore sends each row as stored, carrying its source link. Into an
    /// empty collection in position order, every row relinks to its source
    /// link under whatever surrogate it lands, and a re-delivered record
    /// changes nothing.
    #[test]
    fn a_restored_row_relinks_to_its_source_link() {
        let source_dir = tempfile::tempdir().expect("tempdir");
        let rows = source_rows(source_dir.path());

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        for pass in 0..2u64 {
            for (i, (row, link)) in rows.iter().enumerate() {
                let surrogate = 50 + i as u32;
                assert_eq!(
                    restore(&mut core, 100 + pass * 10 + i as u64, surrogate, row),
                    None
                );
                assert_eq!(stored_link_of(&core, surrogate).as_ref(), Some(link));
            }
        }
        let verdict = core
            .walk_hash_chain(DatabaseId::DEFAULT.as_u64(), TID, COLL)
            .expect("walk the chain");
        assert_eq!(verdict.broken, None);
        assert_eq!(verdict.entries, rows.len() as u64);
    }

    /// A restored row whose surrogate already holds a different row is a
    /// conflict, never a silently dropped body.
    #[test]
    fn a_restore_onto_a_different_row_is_a_conflict() {
        let source_dir = tempfile::tempdir().expect("tempdir");
        let rows = source_rows(source_dir.path());

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        assert_eq!(restore(&mut core, 100, 50, &rows[0].0), None);
        let conflict = restore(&mut core, 101, 50, &rows[1].0);
        assert!(
            conflict.is_some(),
            "a different body on a present row must refuse"
        );
        assert_eq!(stored_row(&core, 50), rows[0].0, "the present row stays");
    }

    /// A committed transaction never carries a link: only a restore relinks.
    #[test]
    fn a_commit_that_carries_a_link_is_refused() {
        let source_dir = tempfile::tempdir().expect("tempdir");
        let rows = source_rows(source_dir.path());

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        let op = doc_put_sub_record(COLL, "r50", &rows[0].0, 50);
        let (_, error) = core.install_from_for_test(TID, 100, vec![op], RedoOrigin::Commit);
        assert!(
            error.is_some(),
            "a commit's put carrying a chain link must refuse"
        );
        assert!(core.chain.intents.is_empty());
    }

    fn strict_schema() -> nodedb_types::columnar::StrictSchema {
        use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
        StrictSchema::new(vec![
            ColumnDef::required("_rowid", ColumnType::Int64),
            ColumnDef::nullable("amount", ColumnType::Int64),
            ColumnDef::nullable(hash_chain::CHAIN_HASH_FIELD, ColumnType::String),
            ColumnDef::nullable(hash_chain::CHAIN_SEQ_FIELD, ColumnType::Int64),
        ])
        .expect("schema")
    }

    fn register_strict(core: &mut CoreLoop) {
        let mut config = CollectionConfig::new(COLL).with_storage_mode(
            nodedb_physical::physical_plan::StorageMode::Strict {
                schema: strict_schema(),
            },
        );
        config.enforcement.append_only = true;
        config.enforcement.hash_chain = true;
        core.doc_configs.insert(key(), config);
    }

    fn strict_link_of(core: &CoreLoop, surrogate: u32) -> Option<ChainHead> {
        let config = core.doc_configs.get(&key()).expect("config");
        let view = core
            .decode_stored_document(config, &stored_row(core, surrogate))
            .expect("decode strict row");
        hash_chain::stored_link(&view)
    }

    /// A strict source, backed up as re-issue sends it: each stored Binary
    /// Tuple decoded back to MessagePack, `(body, link)` in position order.
    fn strict_source_rows(dir: &std::path::Path) -> Vec<(Vec<u8>, ChainHead)> {
        let (mut core, _req, _resp) = make_core_with_dir(dir);
        register_strict(&mut core);
        let task = make_default_task();
        for surrogate in [3u32, 1, 2] {
            let resp = core.commit_plans_for_test(
                &task,
                TID,
                &[put_plan(surrogate, body(i64::from(surrogate) * 10))],
                10 + u64::from(surrogate),
            );
            assert_eq!(resp.status, Status::Ok);
        }
        let mut rows: Vec<(Vec<u8>, ChainHead)> = [3u32, 1, 2]
            .into_iter()
            .map(|s| {
                let body = crate::data::executor::strict_format::binary_tuple_to_msgpack(
                    &stored_row(&core, s),
                    &strict_schema(),
                )
                .expect("strict row decodes");
                (body, strict_link_of(&core, s).expect("source link"))
            })
            .collect();
        rows.sort_by_key(|(_, link)| link.seq);
        rows
    }

    #[test]
    fn a_restored_strict_row_relinks_to_its_source_link() {
        let source_dir = tempfile::tempdir().expect("tempdir");
        let rows = strict_source_rows(source_dir.path());

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_strict(&mut core);
        for (i, (row, link)) in rows.iter().enumerate() {
            let surrogate = 50 + i as u32;
            assert_eq!(restore(&mut core, 100 + i as u64, surrogate, row), None);
            assert_eq!(strict_link_of(&core, surrogate).as_ref(), Some(link));
        }
        let verdict = core
            .walk_hash_chain(DatabaseId::DEFAULT.as_u64(), TID, COLL)
            .expect("walk the chain");
        assert_eq!(verdict.broken, None);
        assert_eq!(verdict.entries, rows.len() as u64);
    }

    #[test]
    fn a_strict_restore_onto_a_different_row_is_a_conflict() {
        let source_dir = tempfile::tempdir().expect("tempdir");
        let rows = strict_source_rows(source_dir.path());

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_strict(&mut core);
        assert_eq!(restore(&mut core, 100, 50, &rows[0].0), None);
        let before = stored_row(&core, 50);
        assert_eq!(
            restore(&mut core, 101, 50, &rows[0].0),
            None,
            "the same row is kept"
        );
        assert!(
            restore(&mut core, 102, 50, &rows[1].0).is_some(),
            "a different body on a present strict row must refuse"
        );
        assert_eq!(stored_row(&core, 50), before, "the present row stays");
    }
}
