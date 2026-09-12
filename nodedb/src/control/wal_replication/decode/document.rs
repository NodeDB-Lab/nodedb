// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Document`.
//!
//! Materialized-sum resolution is read off the record, never re-derived: the
//! pk → surrogate binding needs an async round-trip to another node's leader,
//! and asking twice could get different answers.

use super::ctx::{DecodeCtx, bind_or_lookup};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::wal_replication::types::{BalanceDeltaFields, ReplicatedSumTarget};
use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget, ReturningSpec, UpdateValue};

/// A decoded RETURNING projection spec plus the read filters gating it.
/// Bundled to stay under clippy's arity lint once every point op carries it.
pub(super) struct ReturningFields<'a> {
    pub returning: Option<ReturningSpec>,
    pub rls_filters: &'a [u8],
}

/// The two slots a record carries its materialized-sum resolution in. Travel
/// together — passing only the older one silently strips every entry's target.
pub(super) struct WireSumResolution<'a> {
    /// The AUTHORITATIVE slot: `(target collection, join value)` → surrogate.
    pub bindings: &'a [ReplicatedSumTarget],
    /// The superseded `(join value, surrogate)` slot. Read only when `bindings`
    /// is empty — see [`plan_targets`].
    pub legacy: &'a [(String, u32)],
}

/// Lift the wire resolution back into plan shape. `bindings` wins whenever
/// non-empty; `legacy` entries name no target, so lift untargeted.
fn plan_targets(wire: &WireSumResolution<'_>) -> Vec<ResolvedSumTarget> {
    if !wire.bindings.is_empty() {
        return wire
            .bindings
            .iter()
            .map(|entry| {
                ResolvedSumTarget::new(
                    &entry.target_collection,
                    &entry.join_value,
                    nodedb_types::Surrogate::new(entry.surrogate),
                )
            })
            .collect();
    }
    wire.legacy
        .iter()
        .map(|(join_value, surrogate)| {
            ResolvedSumTarget::untargeted(join_value, nodedb_types::Surrogate::new(*surrogate))
        })
        .collect()
}

pub(super) fn point_put(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    surrogate: u32,
    resolved_sum_targets: &WireSumResolution<'_>,
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let pk_bytes = document_id.as_bytes().to_vec();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            collection,
            &pk_bytes,
            carried,
        )?,
        None => carried,
    };
    Ok(PhysicalPlan::Document(DocumentOp::PointPut {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        surrogate,
        pk_bytes,
        // Carried on the record — a replay re-executes for the originating request.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
    }))
}

/// The materialized-sum decisions the proposer made, carried on the record.
/// Travel together: the proposer resolved-and-folds or deferred to a sibling
/// task per binding; splitting these risks a double-counted or dropped balance.
pub(super) struct SumDecisions<'a> {
    /// `(target collection, join value)` → target surrogate, resolved by the
    /// node that accepted the statement. Never re-resolved here — see this
    /// module's doc.
    pub resolved: WireSumResolution<'a>,
    /// Bindings whose delta a sibling task owns, so the inline fold skips them.
    pub deferred: &'a [String],
}

/// `point_insert`'s materialized-sum decisions plus its RETURNING pair,
/// bundled together — plain positional arguments there exceed clippy's arity
/// lint once `returning` joins the signature.
pub(super) struct PointInsertOptions<'a> {
    pub sums: SumDecisions<'a>,
    pub returning: ReturningFields<'a>,
}

pub(super) fn point_insert(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    if_absent: bool,
    surrogate: u32,
    options: PointInsertOptions<'_>,
) -> crate::Result<PhysicalPlan> {
    let PointInsertOptions { sums, returning } = options;
    let SumDecisions {
        resolved: resolved_sum_targets,
        deferred: deferred_sum_targets,
    } = sums;
    let pk_bytes = document_id.as_bytes();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            collection,
            pk_bytes,
            carried,
        )?,
        None => carried,
    };
    Ok(PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        if_absent,
        surrogate,
        // Carried on the record — see `point_put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(&resolved_sum_targets),
        deferred_sum_targets: deferred_sum_targets.to_vec(),
    }))
}

pub(super) fn point_delete(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    surrogate: u32,
    resolved_sum_targets: &WireSumResolution<'_>,
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let pk_bytes = document_id.as_bytes().to_vec();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, &pk_bytes, carried)?;
    Ok(PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        document_id: document_id.to_owned(),
        surrogate,
        pk_bytes,
        // Carried on the record — see `point_put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // No predicate: writing identity isn't available on a follower; the
        // leader enforces RLS before proposing.
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
    }))
}

/// `point_update`'s materialized-sum resolution, its RETURNING pair, and the
/// target's declared primary key, bundled together — plain positional
/// arguments exceed clippy's arity lint.
pub(super) struct PointUpdateExtras<'a> {
    pub resolved_sum_targets: &'a WireSumResolution<'a>,
    pub returning: ReturningFields<'a>,
    pub declared_primary_key: Option<String>,
}

pub(super) fn point_update(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    updates: &[(String, UpdateValue)],
    surrogate: u32,
    extras: PointUpdateExtras<'_>,
) -> crate::Result<PhysicalPlan> {
    let PointUpdateExtras {
        resolved_sum_targets,
        returning,
        declared_primary_key,
    } = extras;
    let pk_bytes = document_id.as_bytes().to_vec();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, &pk_bytes, carried)?;
    Ok(PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        document_id: document_id.to_owned(),
        surrogate,
        pk_bytes,
        updates: updates.to_vec(),
        // Carried on the record — see `point_put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // No predicate on replay — see `point_delete`.
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
        // Read off the record so this apply enforces NOT NULL on the
        // computed post-image, the same as the proposer's own apply.
        declared_primary_key,
    }))
}

/// `doc_upsert`'s materialized-sum resolution plus its RETURNING pair,
/// bundled together — plain positional arguments there exceed clippy's arity
/// lint once `returning` joins the signature.
pub(super) struct UpsertExtras<'a> {
    pub resolved_sum_targets: &'a WireSumResolution<'a>,
    pub returning: ReturningFields<'a>,
}

pub(super) fn doc_upsert(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    value: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
    surrogate: u32,
    extras: UpsertExtras<'_>,
) -> crate::Result<PhysicalPlan> {
    let UpsertExtras {
        resolved_sum_targets,
        returning,
    } = extras;
    let pk_bytes = document_id.as_bytes().to_vec();
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, &pk_bytes, carried)?;
    Ok(PhysicalPlan::Document(DocumentOp::Upsert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        on_conflict_updates: on_conflict_updates.to_vec(),
        surrogate,
        // No predicate on replay — see `point_delete`.
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        // Carried on the record — see `point_put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
    }))
}

/// Reconstruct a `BatchInsert` plan, binding each row's carried surrogate to
/// its `document_id` on this replica (mirrors `kv::batch_put`). Idempotent
/// under exactly-once, LSN-ordered Raft apply.
pub(super) fn batch_insert(
    ctx: &DecodeCtx,
    collection: &str,
    documents: &[(String, Vec<u8>)],
    surrogates: &[u32],
    resolved_sum_targets: &WireSumResolution<'_>,
    deferred_sum_targets: &[String],
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    // `zip` stops at the shorter side, silently truncating rows with no identity.
    // Refuse here, where the discrepancy is still visibly a malformed record.
    if documents.len() != surrogates.len() {
        return Err(crate::Error::Serialization {
            format: "replicated_write".into(),
            detail: format!(
                "batch insert record for '{collection}' carries {} documents but {} \
                 surrogates; every row must carry its own surrogate",
                documents.len(),
                surrogates.len(),
            ),
        });
    }
    let resolved = documents
        .iter()
        .zip(surrogates.iter())
        .map(|((document_id, _value), carried)| {
            let carried = nodedb_types::Surrogate::new(*carried);
            match ctx.assigner {
                Some(a) => a.bind(
                    ctx.database_id,
                    ctx.tenant_id,
                    collection,
                    document_id.as_bytes(),
                    carried,
                ),
                None => Ok(carried),
            }
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(PhysicalPlan::Document(DocumentOp::BatchInsert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        documents: documents.to_vec(),
        surrogates: resolved,
        // Carried on the record — see `point_put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
        deferred_sum_targets: deferred_sum_targets.to_vec(),
    }))
}

/// Reconstruct the bulk plan in its plain (non-OLLP) form. `ollp_predicted_surrogates
/// = None` selects the local-scan path — deterministic since Raft log order gives
/// identical prior state across replicas. No surrogate binding needed.
pub(super) fn bulk_dml(
    collection: &str,
    filters: &[u8],
    is_update: bool,
    updates: &[(String, UpdateValue)],
    resolved_sum_targets: &WireSumResolution<'_>,
    returning: ReturningFields<'_>,
    declared_primary_key: Option<String>,
) -> PhysicalPlan {
    // Matches are re-derived locally; target identity is read off the record.
    let resolved_sum_targets = plan_targets(resolved_sum_targets);
    if is_update {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
            filters: filters.to_vec(),
            updates: updates.to_vec(),
            // Carried on the record — see `point_put`.
            returning: returning.returning,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: returning.rls_filters.to_vec(),
            // No predicate on replay — see `point_delete`.
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            resolved_sum_targets,
            // Read off the record — see `point_update`.
            declared_primary_key,
        })
    } else {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
            filters: filters.to_vec(),
            // Carried on the record — see `point_put`.
            returning: returning.returning,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: returning.rls_filters.to_vec(),
            // No predicate on replay — see `point_delete`.
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            resolved_sum_targets,
            // Read off the record — see `point_update`.
            declared_primary_key,
        })
    }
}

/// Reconstruct a `Truncate` plan. Autocommit-only, idempotent, deterministic —
/// no surrogate binding since there's no per-row identity.
pub(super) fn truncate(
    collection: &str,
    restart_identity: bool,
    resolved_sum_targets: &WireSumResolution<'_>,
    declared_primary_key: Option<String>,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Truncate {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        restart_identity,
        // Read off the record — see this module's doc.
        resolved_sum_targets: plan_targets(resolved_sum_targets),
        declared_primary_key,
    })
}

pub(super) fn insert_select(
    target_collection: &str,
    source_collection: &str,
    source_filters: &[u8],
    source_limit: usize,
    column_map: &[u8],
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::InsertSelect {
        target_collection: nodedb_types::QualifiedCollection::from_stored(
            target_collection.to_owned(),
        ),
        source_collection: nodedb_types::QualifiedCollection::from_stored(
            source_collection.to_owned(),
        ),
        source_filters: source_filters.to_vec(),
        source_limit,
        column_map: column_map.to_vec(),
    })
}

/// Reconstruct an `ApplyBalanceDelta` plan. No surrogate binding: the document
/// id here IS the hex surrogate, not a primary key. Idempotent like `KvIncr`.
pub(super) fn apply_balance_delta(fields: BalanceDeltaFields<'_>) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
        collection: nodedb_types::QualifiedCollection::from_stored(fields.collection.to_owned()),
        document_id: fields.document_id.to_owned(),
        surrogate: nodedb_types::Surrogate::new(fields.surrogate),
        column: fields.column.to_owned(),
        delta: fields.delta.to_owned(),
        join_column: fields.join_column.to_owned(),
        join_value: fields.join_value.to_owned(),
        declared_primary_key: fields.declared_primary_key.map(str::to_owned),
    })
}

/// Reconstruct a resolved document write plan (`DocumentOp::ResolvedWrite`).
/// Every mutation's surrogate binds via the assigner against its own
/// `(collection, primary key)`; everything else travels on the record.
pub(super) fn resolved_write(
    ctx: &DecodeCtx,
    mutations: &[super::super::types::DocumentResolvedMutationWire],
    response_payload: &[u8],
) -> crate::Result<PhysicalPlan> {
    use super::super::types::DocumentResolvedMutationWire as W;
    use nodedb_physical::physical_plan::DocumentResolvedMutation as M;

    let decoded = mutations
        .iter()
        .map(|m| -> crate::Result<M> {
            Ok(match m {
                W::Put {
                    collection,
                    document_id,
                    surrogate,
                    value,
                    precondition,
                    resolved_sum_targets,
                } => {
                    let pk_bytes = document_id.as_bytes().to_vec();
                    let carried = nodedb_types::Surrogate::new(*surrogate);
                    M::Put {
                        surrogate: bind_or_lookup(ctx, collection, &pk_bytes, carried)?,
                        collection: nodedb_types::QualifiedCollection::from_stored(
                            collection.clone(),
                        ),
                        document_id: document_id.clone(),
                        pk_bytes,
                        value: value.clone(),
                        precondition: precondition.clone(),
                        resolved_sum_targets: plan_targets(&WireSumResolution {
                            bindings: resolved_sum_targets,
                            legacy: &[],
                        }),
                    }
                }
                W::Delete {
                    collection,
                    document_id,
                    surrogate,
                    precondition,
                    resolved_sum_targets,
                } => {
                    let pk_bytes = document_id.as_bytes().to_vec();
                    let carried = nodedb_types::Surrogate::new(*surrogate);
                    M::Delete {
                        surrogate: bind_or_lookup(ctx, collection, &pk_bytes, carried)?,
                        collection: nodedb_types::QualifiedCollection::from_stored(
                            collection.clone(),
                        ),
                        document_id: document_id.clone(),
                        pk_bytes,
                        precondition: precondition.clone(),
                        resolved_sum_targets: plan_targets(&WireSumResolution {
                            bindings: resolved_sum_targets,
                            legacy: &[],
                        }),
                    }
                }
            })
        })
        .collect::<crate::Result<Vec<M>>>()?;

    Ok(PhysicalPlan::Document(DocumentOp::ResolvedWrite {
        mutations: decoded,
        response_payload: response_payload.to_vec(),
        // Decided before this entry was proposed; the record proves it.
        rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::decode;
    use crate::control::wal_replication::types::{ReplicatedEntry, ReplicatedWrite};
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_types::{QualifiedCollection, Surrogate};

    /// Decide + encode in one call, so each test names only the plan it encodes.
    fn to_replicated_entry(
        tenant_id: TenantId,
        database_id: DatabaseId,
        vshard_id: VShardId,
        plan: &PhysicalPlan,
    ) -> crate::Result<Option<ReplicatedEntry>> {
        let write = crate::control::wal_replication::ReplicableWrite::decide_for_replication(plan)?;
        crate::control::wal_replication::encode::to_replicated_entry(
            tenant_id,
            database_id,
            vshard_id,
            &write,
        )
    }

    /// The materialized-sum resolution survives the wire on the insert and
    /// predicate shapes. A lost resolution fails the fold silently; a lost
    /// deferral double-counts against the sibling `ApplyBalanceDelta` entry.
    #[test]
    fn materialized_sum_resolution_roundtrips() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".into(),
            value: vec![1, 2, 3],
            if_absent: false,
            surrogate: Surrogate::new(900),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: vec![
                ResolvedSumTarget::new("accounts", "acc-1", Surrogate::new(4242)),
                ResolvedSumTarget::new("accounts", "acc-2", Surrogate::new(4243)),
                // A second binding on the same join column, different target.
                ResolvedSumTarget::new("audit_totals", "acc-1", Surrogate::new(9001)),
            ],
            deferred_sum_targets: vec!["accounts_elsewhere".to_string()],
        });
        let bytes = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("a document insert must replicate")
            .to_bytes();
        let (_, _, decoded, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded {
            PhysicalPlan::Document(DocumentOp::PointInsert {
                resolved_sum_targets,
                deferred_sum_targets,
                ..
            }) => {
                assert_eq!(
                    resolved_sum_targets,
                    vec![
                        ResolvedSumTarget::new("accounts", "acc-1", Surrogate::new(4242)),
                        ResolvedSumTarget::new("accounts", "acc-2", Surrogate::new(4243)),
                        ResolvedSumTarget::new("audit_totals", "acc-1", Surrogate::new(9001)),
                    ],
                    "a replica cannot resolve a join key itself — the table must arrive with \
                     the write, and each entry must arrive with the TARGET it was resolved \
                     against"
                );
                assert_eq!(
                    deferred_sum_targets,
                    vec!["accounts_elsewhere".to_string()],
                    "a lost deferral is a double count, not a missing one"
                );
            }
            other => panic!("expected PointInsert, got {other:?}"),
        }

        let bulk = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            filters: vec![7, 7],
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: vec![ResolvedSumTarget::new(
                "accounts",
                "acc-1",
                Surrogate::new(4242),
            )],
            declared_primary_key: Some("sku".to_string()),
        });
        let bytes = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &bulk)
            .expect("encode must not error")
            .expect("a single-shard bulk delete must replicate")
            .to_bytes();
        let (_, _, decoded, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded {
            PhysicalPlan::Document(DocumentOp::BulkDelete {
                resolved_sum_targets,
                declared_primary_key,
                ..
            }) => {
                assert_eq!(
                    resolved_sum_targets,
                    vec![ResolvedSumTarget::new(
                        "accounts",
                        "acc-1",
                        Surrogate::new(4242)
                    )],
                    "a replica re-derives which rows matched, never which target they credit"
                );
                assert_eq!(
                    declared_primary_key.as_deref(),
                    Some("sku"),
                    "the declared primary key travels on the record"
                );
            }
            other => panic!("expected BulkDelete, got {other:?}"),
        }
    }

    /// A record committed before the target collection travelled on the wire
    /// still decodes: the superseded slot's entries are lifted untargeted and
    /// match any binding by join value alone.
    #[test]
    fn a_record_without_target_collections_decodes_as_untargeted() {
        let entry = ReplicatedEntry::new(
            1,
            0,
            0,
            ReplicatedWrite::PointDelete {
                collection: "entries".into(),
                document_id: "e1".into(),
                surrogate: 900,
                resolved_sum_targets: vec![("acc-1".into(), 4242)],
                resolved_sum_target_bindings: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
            },
        );
        let (_, _, decoded, _) = decode::from_replicated_entry(&entry.to_bytes(), None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded {
            PhysicalPlan::Document(DocumentOp::PointDelete {
                resolved_sum_targets,
                ..
            }) => {
                assert_eq!(
                    resolved_sum_targets,
                    vec![ResolvedSumTarget::untargeted("acc-1", Surrogate::new(4242))],
                    "the older slot must still be read when the newer one is empty"
                );
                assert!(
                    resolved_sum_targets[0].addresses("accounts", "acc-1"),
                    "an untargeted entry answers for whichever binding asks"
                );
            }
            other => panic!("expected PointDelete, got {other:?}"),
        }
    }

    /// A current-node record carries the resolution in both slots; the newer one
    /// is authoritative. The older slot stays populated so an older-binary peer
    /// keeps working instead of seeing an empty resolution.
    #[test]
    fn a_current_record_carries_both_slots_and_reads_the_newer_one() {
        let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".into(),
            surrogate: Surrogate::new(900),
            pk_bytes: b"e1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: vec![
                ResolvedSumTarget::new("accounts", "acc-1", Surrogate::new(4242)),
                ResolvedSumTarget::new("audit_totals", "acc-1", Surrogate::new(9001)),
            ],
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("a point delete must replicate");
        match &entry.write {
            ReplicatedWrite::PointDelete {
                resolved_sum_targets,
                resolved_sum_target_bindings,
                ..
            } => {
                assert_eq!(
                    resolved_sum_target_bindings.len(),
                    2,
                    "both bindings must travel; the newer slot is the authoritative one"
                );
                assert_eq!(
                    resolved_sum_targets,
                    &vec![("acc-1".to_string(), 4242)],
                    "the superseded slot keeps its one-entry-per-value shape, so an older \
                     peer reads what it has always read"
                );
            }
            other => panic!("expected PointDelete, got {other:?}"),
        }

        let (_, _, decoded, _) = decode::from_replicated_entry(&entry.to_bytes(), None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded {
            PhysicalPlan::Document(DocumentOp::PointDelete {
                resolved_sum_targets,
                ..
            }) => assert_eq!(
                resolved_sum_targets,
                vec![
                    ResolvedSumTarget::new("accounts", "acc-1", Surrogate::new(4242)),
                    ResolvedSumTarget::new("audit_totals", "acc-1", Surrogate::new(9001)),
                ],
                "the newer slot wins, so the second binding keeps its own target row"
            ),
            other => panic!("expected PointDelete, got {other:?}"),
        }
    }

    #[test]
    fn doc_batch_insert_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let documents = vec![
            ("d1".to_string(), vec![1u8, 2, 3]),
            ("d2".to_string(), vec![4u8, 5]),
            ("d3".to_string(), vec![6u8, 7, 8, 9]),
        ];
        let surrogates = vec![Surrogate::new(11), Surrogate::new(22), Surrogate::new(33)];

        let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            documents: documents.clone(),
            surrogates: surrogates.clone(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("DocumentOp::BatchInsert should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        // No assigner: carried surrogates fall through verbatim.
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection,
                documents: decoded_docs,
                surrogates: decoded_surrogates,
                ..
            }) => {
                assert_eq!(collection.as_str(), "docs");
                assert_eq!(
                    decoded_docs, documents,
                    "every (doc_id, body) pair must round-trip"
                );
                assert_eq!(
                    decoded_surrogates, surrogates,
                    "every surrogate must round-trip in order, none dropped"
                );
            }
            other => panic!("expected Document(BatchInsert), got {other:?}"),
        }
    }

    #[test]
    fn doc_truncate_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            restart_identity: true,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: Some("sku".to_string()),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("DocumentOp::Truncate should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Document(DocumentOp::Truncate {
                collection,
                restart_identity,
                resolved_sum_targets: _,
                declared_primary_key,
            }) => {
                assert_eq!(collection.as_str(), "docs");
                assert!(restart_identity, "restart_identity must round-trip");
                assert_eq!(
                    declared_primary_key.as_deref(),
                    Some("sku"),
                    "declared_primary_key must round-trip"
                );
            }
            other => panic!("expected Document(Truncate), got {other:?}"),
        }
    }

    /// Encode must not drop `returning` / `rls_filters` on a document write —
    /// the leader re-derives its own plan from the committed entry, so this
    /// would drop `RETURNING` for the originating request too.
    #[test]
    fn document_point_put_returning_and_rls_filters_roundtrip() {
        let spec = ReturningSpec {
            columns: nodedb_physical::physical_plan::ReturningColumns::Star,
        };
        let plan = PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            document_id: "u1".into(),
            value: b"alice".to_vec(),
            surrogate: Surrogate::new(1),
            pk_bytes: b"u1".to_vec(),
            returning: Some(spec.clone()),
            rls_filters: b"rls-predicate".to_vec(),
            resolved_sum_targets: Vec::new(),
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("PointPut should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Document(DocumentOp::PointPut {
                returning,
                rls_filters,
                ..
            }) => {
                assert_eq!(
                    returning,
                    Some(spec),
                    "a RETURNING insert must not silently yield no rows on replication"
                );
                assert_eq!(
                    rls_filters,
                    b"rls-predicate".to_vec(),
                    "the RETURNING read-policy filter must survive replication"
                );
            }
            other => panic!("expected Document(PointPut), got {other:?}"),
        }
    }

    /// See `document_point_put_returning_and_rls_filters_roundtrip`; same bug,
    /// `DocumentOp::PointUpdate`.
    #[test]
    fn document_point_update_returning_and_rls_filters_roundtrip() {
        let spec = ReturningSpec {
            columns: nodedb_physical::physical_plan::ReturningColumns::Named(vec![
                nodedb_physical::physical_plan::ReturningItem {
                    name: "balance".into(),
                    alias: None,
                },
            ]),
        };
        let plan = PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: "a1".into(),
            surrogate: Surrogate::new(2),
            pk_bytes: b"a1".to_vec(),
            updates: vec![("balance".into(), UpdateValue::Literal(b"5".to_vec()))],
            returning: Some(spec.clone()),
            rls_filters: b"rls-predicate".to_vec(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("PointUpdate should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                returning,
                rls_filters,
                ..
            }) => {
                assert_eq!(
                    returning,
                    Some(spec),
                    "a RETURNING update must not silently yield no rows on replication"
                );
                assert_eq!(
                    rls_filters,
                    b"rls-predicate".to_vec(),
                    "the RETURNING read-policy filter must survive replication"
                );
            }
            other => panic!("expected Document(PointUpdate), got {other:?}"),
        }
    }

    /// See `document_point_put_returning_and_rls_filters_roundtrip`; same bug,
    /// `DocumentOp::PointDelete`.
    #[test]
    fn document_point_delete_returning_and_rls_filters_roundtrip() {
        let spec = ReturningSpec {
            columns: nodedb_physical::physical_plan::ReturningColumns::Star,
        };
        let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: "a1".into(),
            surrogate: Surrogate::new(3),
            pk_bytes: b"a1".to_vec(),
            returning: Some(spec.clone()),
            rls_filters: b"rls-predicate".to_vec(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("PointDelete should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Document(DocumentOp::PointDelete {
                returning,
                rls_filters,
                ..
            }) => {
                assert_eq!(
                    returning,
                    Some(spec),
                    "a RETURNING delete must not silently yield no rows on replication"
                );
                assert_eq!(
                    rls_filters,
                    b"rls-predicate".to_vec(),
                    "the RETURNING read-policy filter must survive replication"
                );
            }
            other => panic!("expected Document(PointDelete), got {other:?}"),
        }
    }

    /// See `document_point_put_returning_and_rls_filters_roundtrip`; same bug,
    /// `DocumentOp::Upsert` (`INSERT ... ON CONFLICT DO UPDATE`).
    #[test]
    fn document_upsert_returning_and_rls_filters_roundtrip() {
        let spec = ReturningSpec {
            columns: nodedb_physical::physical_plan::ReturningColumns::Star,
        };
        let plan = PhysicalPlan::Document(DocumentOp::Upsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: "a1".into(),
            value: b"{}".to_vec(),
            on_conflict_updates: vec![("balance".into(), UpdateValue::Literal(b"5".to_vec()))],
            surrogate: Surrogate::new(4),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: Some(spec.clone()),
            rls_filters: b"rls-predicate".to_vec(),
            resolved_sum_targets: Vec::new(),
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("Upsert should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Document(DocumentOp::Upsert {
                returning,
                rls_filters,
                ..
            }) => {
                assert_eq!(
                    returning,
                    Some(spec),
                    "a RETURNING upsert must not silently yield no rows on replication"
                );
                assert_eq!(
                    rls_filters,
                    b"rls-predicate".to_vec(),
                    "the RETURNING read-policy filter must survive replication"
                );
            }
            other => panic!("expected Document(Upsert), got {other:?}"),
        }
    }
}
