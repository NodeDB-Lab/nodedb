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
//! `build_static_tx_class`, just sourced from a pre-exec-scan surrogate
//! prediction instead of statically-known plan fields.

use crate::Error;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::types::VShardId;
use nodedb_cluster::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass};
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::{DatabaseId, TenantId};

use super::shared::{
    collection_name_from_plan, read_set_from, surrogate_from_plan, versioned_reads_from,
};

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
    use std::collections::BTreeMap;

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

    // Accumulate per-collection surrogate sets. The OLLP collection uses the
    // predicted surrogates; all other tasks use static key extraction.
    let mut doc_surrogates: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    // Graph edges (appended implicit-edge deletes) route by from_key(src)/
    // from_key(dst), NOT by collection — mirror `build_static_tx_class`'s edge
    // handling so an `EdgeDelete` appended to a dependent txn is classified as
    // an `EngineKeySet::Edge` (and dual-homed/locked) rather than misrouted as a
    // document write via `surrogate_from_plan`.
    let mut edge_pairs: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
    let mut edge_homes: BTreeMap<String, Vec<u32>> = BTreeMap::new();

    // Seed with the OLLP collection's predicted surrogates.
    doc_surrogates
        .entry(collection.to_owned())
        .or_default()
        .extend_from_slice(predicted_surrogates);

    // Add static surrogates for all non-OLLP tasks.
    for task in tasks {
        // Edges first: collect surrogate-pair identity + from_key routing homes,
        // then skip the doc-surrogate path. EdgePut/EdgeDelete share identity
        // fields so both produce an `EngineKeySet::Edge`.
        if let PhysicalPlan::Graph(
            GraphOp::EdgePut {
                collection: edge_coll,
                src_id,
                dst_id,
                src_surrogate,
                dst_surrogate,
                ..
            }
            | GraphOp::EdgeDelete {
                collection: edge_coll,
                src_id,
                dst_id,
                src_surrogate,
                dst_surrogate,
                ..
            },
        ) = &task.plan
        {
            edge_pairs
                .entry(edge_coll.to_string())
                .or_default()
                .push((src_surrogate.as_u32(), dst_surrogate.as_u32()));
            let homes = edge_homes.entry(edge_coll.to_string()).or_default();
            homes.push(VShardId::from_key(src_id.as_bytes()).as_u32());
            homes.push(VShardId::from_key(dst_id.as_bytes()).as_u32());
            continue;
        }

        let coll = collection_name_from_plan(&task.plan);
        if coll.is_empty() || coll == collection {
            continue;
        }
        let surrogate = surrogate_from_plan(&task.plan);
        doc_surrogates.entry(coll).or_default().push(surrogate);
    }

    let mut write_sets: Vec<EngineKeySet> = doc_surrogates
        .into_iter()
        .map(|(coll, surrogates)| EngineKeySet::Document {
            collection: coll,
            surrogates: SortedVec::new(surrogates),
        })
        .collect();
    // Emit one Edge keyset per edge collection, with the SAME missing-homes-is-
    // hard-error guard `build_static_tx_class` uses: `edge_pairs` and
    // `edge_homes` are populated in lockstep, so a missing homes entry is an
    // invariant violation, not an empty-participant write.
    for (edge_coll, pairs) in edge_pairs {
        let homes = edge_homes.remove(&edge_coll).ok_or_else(|| Error::Internal {
            detail: format!(
                "build_dependent_tx_class invariant violated: no edge_homes for collection {edge_coll}"
            ),
        })?;
        write_sets.push(EngineKeySet::Edge {
            collection: edge_coll,
            edges: SortedVec::new(pairs),
            home_vshards: SortedVec::new(homes),
        });
    }
    write_sets.sort_by(|a, b| a.collection().cmp(b.collection()));

    let write_set = ReadWriteSet::new(write_sets);
    // A predicate that matched no rows, in a batch whose other tasks write
    // nothing either, decides an empty write set: there is no state change to
    // sequence, so no Calvin entry is proposed. `TxClass` rejects an empty
    // write set, and that rejection is not a retryable condition — the caller
    // returns the statement's zero-row result instead of retrying.
    if write_set.is_empty() {
        return Ok(None);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DatabaseId;
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
        let want_vshard =
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, "users").as_u32();

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
        // error the retry loop would mistake for predicate drift.
        let tasks = vec![bulk_delete_task("users")];
        let built =
            build_single_vshard_dependent_tx_class(&tasks, TenantId::new(1), "users", &[], &[])
                .expect("zero-match predicate is not an error");
        assert!(built.is_none(), "zero-match predicate builds no TxClass");
    }
}
