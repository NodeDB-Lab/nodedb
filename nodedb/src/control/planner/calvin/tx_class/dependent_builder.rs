// SPDX-License-Identifier: BUSL-1.1

//! `TxClass` construction for a dependent-read (OLLP) transaction: the OLLP
//! collection's write set comes from reconnaissance-predicted surrogates; all
//! other tasks use static surrogate extraction.
//!
//! Despite the "dependent" naming (shared with the OLLP reconnaissance
//! terminology), the `TxClass` built here carries `dependent_reads: None` —
//! it is NOT a Calvin dependent-read-barrier transaction (see
//! [`TxClass::new_dependent`]; that is a distinct mechanism for passive
//! vshards to broadcast reads to active participants before they write). This
//! builder is write-set-identity construction only, exactly like
//! `build_static_tx_class`, sourced from a pre-exec-scan surrogate
//! prediction instead of statically-known plan fields.

use crate::Error;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use nodedb_cluster::calvin::types::{ReadWriteSet, TxClass};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::{DatabaseId, TenantId};

use super::shared::{read_set_from, versioned_reads_from};
use super::write_keys::{WriteKeys, add_plan_write_keys, task_write_keys};
use crate::control::planner::calvin::is_dependent_predicate;
use crate::control::planner::calvin::write_class::is_write_plan;

/// Build a **multi-vshard** `TxClass` for a dependent-read (OLLP) transaction.
///
/// For `BulkUpdate`/`BulkDelete` plans that have `ollp_predicted_surrogates`
/// set, the OLLP collection's write set is built from `predicted_surrogates`.
/// All other tasks in the batch are included using static surrogate extraction,
/// exactly as `build_static_tx_class` does. This ensures multi-shard Calvin
/// txns that contain an OLLP bulk operation alongside static-key writes still
/// produce a valid multi-vshard `TxClass`. A write set that collapses to a
/// single vshard is rejected (`SingleVshardTxn`) — for the legitimate
/// contended single-collection predicate write, use
/// [`build_single_vshard_dependent_tx_class`].
///
/// `reads` is the neutral session read-set, projected onto the `TxClass`'s
/// routing/identity `read_set` (collection-homed) so read shards are enumerated
/// as participants, and onto `versioned_reads` (LSN-versioned OCC validation
/// set) for commit-time optimistic-concurrency validation. Autocommit paths
/// pass an empty slice.
///
/// Returns `Ok(None)` when the batch turns out to write nothing at all — the
/// predicate matched no rows and no other task in the batch carries a write.
/// Returns `Err` if encoding fails or the resulting TxClass is invalid.
pub fn build_dependent_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    collection: &str,
    predicted_surrogates: &[u32],
    reads: &[ReadSetEntry],
) -> crate::Result<Option<TxClass>> {
    build_dependent_tx_class_impl(
        tasks,
        tenant_id,
        collection,
        predicted_surrogates,
        reads,
        false,
    )
}

/// Build a `TxClass` for a dependent-read (OLLP) transaction that is permitted
/// to resolve to a **single vshard**.
///
/// Used only by the contended single-collection predicate write routing path
/// (`route_write_to_calvin`'s dependent-predicate branch): the write-admission
/// gate returned `RouteToCalvin` because a pending commit holds a key in the
/// predicate's range, so the write must sequence through the deterministic
/// scheduler to serialize on the SAME shared per-vShard `LockManager` the
/// holder is on. Identical extraction to [`build_dependent_tx_class`]; only
/// the participant floor differs (via [`TxClass::new_single_vshard`] — the
/// SAME opt-in the point-write path uses, since this `TxClass` shape is a
/// plain write-set-identity construction like the static builder, not a
/// Calvin dependent-read-barrier transaction).
pub fn build_single_vshard_dependent_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    collection: &str,
    predicted_surrogates: &[u32],
    reads: &[ReadSetEntry],
) -> crate::Result<Option<TxClass>> {
    build_dependent_tx_class_impl(
        tasks,
        tenant_id,
        collection,
        predicted_surrogates,
        reads,
        true,
    )
}

/// Shared body for the dependent builders. `allow_single_vshard` selects
/// between [`TxClass::new`] (multi-vshard, `>=2` floor) and
/// [`TxClass::new_single_vshard`] (single-vshard opt-in) — the same pair
/// `build_static_tx_class_impl` selects between; this builder only differs in
/// how the write set's surrogate identity is sourced.
fn build_dependent_tx_class_impl(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    collection: &str,
    predicted_surrogates: &[u32],
    reads: &[ReadSetEntry],
    allow_single_vshard: bool,
) -> crate::Result<Option<TxClass>> {
    let database_id = tasks
        .first()
        .map_or(DatabaseId::DEFAULT, |task| task.database_id);
    if tasks.iter().any(|task| task.database_id != database_id)
        || reads.iter().any(|read| read.database_id != database_id)
    {
        return Err(Error::BadRequest {
            detail: "Calvin transaction spans multiple databases".to_owned(),
        });
    }

    // A predicate that matched no rows, in a batch whose other tasks write
    // nothing either, decides an empty statement: there is no state change to
    // sequence, so no Calvin entry is proposed. The caller returns the
    // statement's zero-row result instead of retrying.
    if predicted_surrogates.is_empty() && !others_write(tasks, collection)? {
        return Ok(None);
    }

    // Every other task's keys, extracted as `build_static_tx_class` extracts
    // them. The OLLP collection's rows are the ones reconnaissance predicted.
    // Its predicate write keeps the whole-collection lock, so an insert that
    // would join the predicate's match set waits for it.
    let mut keys = task_write_keys(tasks)?;
    keys.drop_documents(collection);
    keys.rows(collection, predicted_surrogates.iter().copied());

    let write_set = ReadWriteSet::new(keys.into_key_sets());
    // Populate the routing/identity read_set from the session read-set (a txn
    // that writes shard A but reads shard B enumerates B as a participant). An
    // empty `reads` slice yields an empty read_set.
    let read_set = read_set_from(reads);

    let plans: Vec<&PhysicalPlan> = tasks.iter().map(|t| &t.plan).collect();
    let plans_bytes = zerompk::to_msgpack_vec(&plans).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode PhysicalPlan vec for Calvin dependent TxClass: {e}"),
    })?;

    // versioned_reads carries the LSN-versioned OCC validation set, populated
    // from the same session read-set the routing `read_set` above was built
    // from.
    let versioned_reads = versioned_reads_from(reads);

    let result = if allow_single_vshard {
        TxClass::new_single_vshard_in_database(
            read_set,
            write_set,
            plans_bytes,
            tenant_id,
            database_id,
            None,
            versioned_reads,
        )
    } else {
        TxClass::new_in_database(
            read_set,
            write_set,
            plans_bytes,
            tenant_id,
            database_id,
            None,
            versioned_reads,
        )
    };
    result.map(Some).map_err(|e| Error::BadRequest {
        detail: format!("invalid dependent TxClass: {e}"),
    })
}

/// Whether any task besides the OLLP predicate write on `collection` writes
/// a key.
fn others_write(tasks: &[PhysicalTask], collection: &str) -> crate::Result<bool> {
    let mut keys = WriteKeys::default();
    let others = tasks.iter().filter(|task| {
        is_write_plan(&task.plan)
            && !(is_dependent_predicate(&task.plan) && task.plan.collection() == Some(collection))
    });
    for task in others {
        add_plan_write_keys(&mut keys, &task.plan)?;
    }
    Ok(keys.into_key_sets().iter().any(|set| !set.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, VShardId};
    use nodedb_physical::physical_plan::DocumentOp;

    fn bulk_delete_task(collection: &str) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
                filters: vec![],
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: vec![],
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn single_collection_predicate_strict_rejects_but_single_vshard_builder_accepts() {
        // One BulkDelete task on ONE collection resolves to exactly one
        // vshard. This is exactly the shape the contended single-shard
        // predicate-write routing path builds.
        let tasks = vec![bulk_delete_task("users")];
        let want_vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users")
            .vshard()
            .as_u32();

        // Strict builder rejects the single-vshard write set.
        let strict = build_dependent_tx_class(&tasks, TenantId::new(1), "users", &[7, 8], &[]);
        assert!(
            matches!(strict, Err(crate::Error::BadRequest { .. })),
            "strict dependent builder must reject single-vshard write set"
        );

        // Single-vshard builder accepts it, with exactly one participating vshard.
        let tx =
            build_single_vshard_dependent_tx_class(&tasks, TenantId::new(1), "users", &[7, 8], &[])
                .expect("single-vshard dependent TxClass accepted")
                .expect("non-empty write set builds a TxClass");
        assert_eq!(tx.participating_vshards().len(), 1);
        assert_eq!(tx.participating_vshards()[0].as_u32(), want_vshard);
    }

    #[test]
    fn zero_match_predicate_builds_no_tx_class() {
        // A predicate matching no rows decides an empty write set: nothing to
        // sequence, so the builder reports "no transaction" rather than an
        // error the retry loop will mistake for predicate drift.
        let tasks = vec![bulk_delete_task("users")];
        let built =
            build_single_vshard_dependent_tx_class(&tasks, TenantId::new(1), "users", &[], &[])
                .expect("zero-match predicate is not an error");
        assert!(built.is_none(), "zero-match predicate builds no TxClass");
    }

    /// The lock request the scheduler takes for `tx_class` at `position`.
    fn locks(
        tx_class: TxClass,
        position: u32,
    ) -> std::collections::BTreeMap<
        crate::control::cluster::calvin::scheduler::LockKey,
        crate::control::cluster::calvin::scheduler::LockMode,
    > {
        crate::control::cluster::calvin::scheduler::driver::helpers::expand_rw_set(
            &nodedb_cluster::calvin::types::SequencedTxn {
                epoch: 1,
                position,
                tx_class,
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 2,
                lock_owner: None,
            },
        )
    }

    /// An OLLP predicate write locks its predicted rows and its whole
    /// collection. An insert of a row the prediction never saw (a phantom)
    /// waits for it, so the predicate cannot miss the row on one replica and
    /// match it on another.
    #[test]
    fn an_ollp_predicate_write_blocks_a_phantom_insert() {
        use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
        use crate::control::cluster::calvin::scheduler::{AcquireOutcome, LockManager};

        let predicate = build_single_vshard_dependent_tx_class(
            &[bulk_delete_task("users")],
            TenantId::new(1),
            "users",
            &[7],
            &[],
        )
        .expect("dependent TxClass")
        .expect("non-empty write set");
        let insert_task = PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
                document_id: "phantom".to_owned(),
                value: Vec::new(),
                if_absent: false,
                surrogate: nodedb_types::Surrogate::new(99),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        };
        let insert =
            super::super::build_single_vshard_tx_class(&[insert_task], TenantId::new(1), &[])
                .expect("insert TxClass");

        let (first, second) = (TxnId::new(1, 0), TxnId::new(1, 1));
        let mut table = LockManager::new();
        assert_eq!(
            table.acquire(first, locks(predicate, 0)),
            AcquireOutcome::Ready
        );
        assert_eq!(
            table.acquire(second, locks(insert, 1)),
            AcquireOutcome::Blocked,
            "a phantom insert waits for the predicate write"
        );
        assert_eq!(table.release(first), vec![second]);
    }
}
