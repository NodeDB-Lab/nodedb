// SPDX-License-Identifier: BUSL-1.1

//! Entry point: encode a decided [`ReplicableWrite`] into a `ReplicatedEntry`
//! for Raft proposal, plus the shared provenance-encoding helper.
//!
//! `to_replicated_entry` is the single oracle deciding which `PhysicalPlan`
//! variants are proposed over Raft; exhaustive so a new variant is a compile error.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::replicable_write::ReplicableWrite;
use super::super::types::ReplicatedEntry;
use super::{
    crdt, entry_array, entry_columnar_family, entry_document, entry_graph, entry_kv, vector,
};
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, TenantId, VShardId};

/// Serialize optional sync provenance into the cross-node wire shape.
/// `.expect()`, not `.ok()`: losing provenance on a follower would defeat the
/// idempotency gate and risk double-apply.
pub(super) fn encode_provenance(
    provenance: &Option<nodedb_types::sync::wire::SyncProvenance>,
) -> Option<Vec<u8>> {
    provenance
        .as_ref()
        .map(|p| zerompk::to_msgpack_vec(p).expect("SyncProvenance serialization is infallible"))
}

/// Serialize an optional RETURNING projection spec, same infallible-encode
/// contract as `encode_provenance`: failure must fail loud, not silently
/// degrade a RETURNING request to an empty result.
pub(super) fn encode_returning(
    returning: &Option<nodedb_physical::physical_plan::ReturningSpec>,
) -> Option<Vec<u8>> {
    returning
        .as_ref()
        .map(|r| zerompk::to_msgpack_vec(r).expect("ReturningSpec serialization is infallible"))
}

/// Encode a [`ReplicableWrite`] into a `ReplicatedEntry`, or `Ok(None)` for a
/// non-replicated plan. Live-predicate refusal lives in
/// `ReplicableWrite::decide_for_replication` — nothing to re-check here.
pub fn to_replicated_entry(
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    write: &ReplicableWrite<'_>,
) -> crate::Result<Option<ReplicatedEntry>> {
    let plan = write.plan();
    let encoded = match plan {
        PhysicalPlan::Document(op) => entry_document::document_write(op),
        // Fallible: a governed predicate DML refuses rather than encode it bare.
        PhysicalPlan::Kv(op) => entry_kv::kv_write(op)?,
        // Exhaustive over their op enums — see their module docs.
        PhysicalPlan::Vector(op) => vector::encode(op),
        PhysicalPlan::Crdt(op) => crdt::encode(op),
        PhysicalPlan::Graph(op) => entry_graph::graph_write(op),
        // Fallible for the same reason as the Kv arm above.
        PhysicalPlan::Columnar(op) => entry_columnar_family::columnar_write(op)?,
        PhysicalPlan::Timeseries(op) => entry_columnar_family::timeseries_write(op),
        PhysicalPlan::Text(op) => entry_columnar_family::text_write(op),
        PhysicalPlan::Spatial(op) => entry_columnar_family::spatial_write(op),
        PhysicalPlan::Array(op) => entry_array::array_write(op),
        // Cluster-fanned array ops execute entirely on the Control Plane (`ArrayCoordinator`).
        PhysicalPlan::ClusterArray(_) => None,
        // Reads / query operators / metadata ops are never replicated writes.
        PhysicalPlan::Query(_) => None,
        PhysicalPlan::Meta(_) | PhysicalPlan::ClusterEvent(_) => None,
    };

    Ok(encoded.map(|write| {
        ReplicatedEntry::new(
            tenant_id.as_u64(),
            database_id.as_u64(),
            vshard_id.as_u32(),
            write,
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, KvOp};
    use nodedb_types::{QualifiedCollection, Surrogate};

    /// Decide + encode in one call, so each test names only the plan it encodes.
    /// Shadows this file's `to_replicated_entry`, which takes a decided
    /// [`ReplicableWrite`].
    fn to_replicated_entry(
        tenant_id: TenantId,
        database_id: DatabaseId,
        vshard_id: VShardId,
        plan: &PhysicalPlan,
    ) -> crate::Result<Option<ReplicatedEntry>> {
        let write = ReplicableWrite::decide_for_replication(plan)?;
        super::to_replicated_entry(tenant_id, database_id, vshard_id, &write)
    }

    #[test]
    fn to_replicated_entry_writes_only() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            document_id: "d".into(),
            value: vec![],
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(
            to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
                .expect("encode must not error")
                .is_some()
        );

        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            document_id: "d".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert!(
            to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
                .expect("encode must not error")
                .is_none()
        );
    }

    // ---- Pinned replication gaps: writes with no `ReplicatedWrite` shape yet,
    // so `to_replicated_entry` returns `None` on purpose. Each assertion is a
    // tripwire — wiring one of these must fail loudly and update this list.

    #[test]
    fn known_write_gaps_are_not_replicated() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let gaps: Vec<(&str, PhysicalPlan)> = vec![
            (
                "Document::Merge",
                PhysicalPlan::Document(DocumentOp::Merge {
                    target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                    source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "staging"),
                    source_alias: "s".into(),
                    target_join_col: "id".into(),
                    source_join_col: "id".into(),
                    clauses: Vec::new(),
                    returning: None,
                    resolved_inserts: None,
                    source_rows: None,
                    rls_filters: Vec::new(),
                    rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                    resolved_sum_targets: Vec::new(),
                    declared_primary_key: None,
                }),
            ),
            (
                "Document::UpdateFromJoin",
                PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
                    target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                    source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "staging"),
                    source_alias: "s".into(),
                    target_join_col: "id".into(),
                    source_join_col: "id".into(),
                    updates: Vec::new(),
                    target_filters: Vec::new(),
                    returning: None,
                    source_rows: None,
                    rls_filters: Vec::new(),
                    rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                    resolved_sum_targets: Vec::new(),
                    declared_primary_key: None,
                }),
            ),
            (
                "Crdt::RestoreToVersion",
                PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                    document_id: "id1".into(),
                    target_version_json: "{}".into(),
                    surrogate: Surrogate::new(1),
                }),
            ),
        ];

        for (name, plan) in &gaps {
            assert!(
                to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, plan)
                    .expect("encode must not error")
                    .is_none(),
                "{name} is a known replication gap; wiring is a tracked follow-up — \
                 this test fails loudly if someone wires it so they update the tracking"
            );
        }
    }

    #[test]
    fn representative_handled_writes_still_replicate() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        // Guard: a live document/KV write must still return `Some`.
        let point_put = PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".into(),
            value: vec![1, 2, 3],
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(
            to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &point_put)
                .expect("encode must not error")
                .is_some(),
            "Document::PointPut must still replicate"
        );

        let kv_put = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            key: vec![1],
            value: vec![2],
            ttl_ms: 0,
            surrogate: Surrogate::new(7),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(
            to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &kv_put)
                .expect("encode must not error")
                .is_some(),
            "Kv::Put must still replicate"
        );
    }
}
