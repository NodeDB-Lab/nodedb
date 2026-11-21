// SPDX-License-Identifier: BUSL-1.1

//! Footprints of CRDT ops.
//!
//! Each op keys its collection, whose Loro document and sparse projection
//! it reads or writes. A validated apply also reads and writes the tenant's
//! CRDT state: the constraint validator, its dead-letter and deferred
//! queues, and the array surrogate registry. Its foreign-key checks read
//! each referent collection the collection's constraint set names, which
//! the core's CRDT state resolves. A sync frame also advances its stream's
//! high-water mark. A compaction publishes every CRDT engine of the core.

use nodedb_physical::physical_plan::CrdtOp;
use nodedb_types::QualifiedCollection;

use crate::types::TenantId;

use super::class::{Footprint, Scope};
use super::keys::{KeySet, read_of, write_of};

pub(super) fn crdt_footprint(op: &CrdtOp, scope: &Scope<'_>) -> Footprint {
    match op {
        CrdtOp::Read { collection, .. }
        | CrdtOp::ReadAtVersion { collection, .. }
        | CrdtOp::GetVersionVector { collection }
        | CrdtOp::ExportDelta { collection, .. }
        // A restore previews its delta and writes nothing.
        | CrdtOp::RestoreToVersion { collection, .. } => read_of(collection, scope),
        CrdtOp::PreviewApply { collection, .. } => {
            validated(collection, scope.tenant, scope).read()
        }
        CrdtOp::Apply {
            collection,
            provenance,
            ..
        } => validated(collection, scope.tenant, scope)
            .sync(provenance.as_ref())
            .write(),
        CrdtOp::ApplyAuthenticated {
            collection,
            provenance,
            ..
        } => validated(collection, scope.tenant, scope)
            .sync(Some(provenance))
            .write(),
        CrdtOp::ImportSnapshot {
            tenant_id,
            collection,
            ..
        } => validated(collection, TenantId::new(*tenant_id), scope).write(),
        // Constraints and policies live in the tenant-wide validator.
        CrdtOp::SetConstraints { collection, .. }
        | CrdtOp::DropConstraints { collection, .. }
        | CrdtOp::SetPolicy { collection, .. } => KeySet::new(scope)
            .collection(collection)
            .crdt_tenant_of(scope.tenant)
            .write(),
        CrdtOp::ReadConstraints { collection } | CrdtOp::GetPolicy { collection } => {
            KeySet::new(scope)
                .collection(collection)
                .crdt_tenant_of(scope.tenant)
                .read()
        }
        CrdtOp::ListInsert { collection, .. }
        | CrdtOp::ListDelete { collection, .. }
        | CrdtOp::ListMove { collection, .. }
        | CrdtOp::DocUpsert { collection, .. }
        | CrdtOp::DocDelete { collection, .. } => write_of(collection, scope),
        // The compaction publishes a checkpoint of every CRDT engine.
        CrdtOp::CompactAtVersion { .. } => Footprint::Core,
    }
}

/// The keys of a validated apply into `collection` of `tenant`: the
/// collection, the tenant's CRDT state, and each foreign-key referent.
fn validated<'s>(
    collection: &QualifiedCollection,
    tenant: TenantId,
    scope: &'s Scope<'s>,
) -> KeySet<'s> {
    let referents = scope
        .registry
        .crdt_referents(scope.db, tenant, collection.as_str());
    referents.iter().fold(
        KeySet::new(scope)
            .collection(collection)
            .crdt_tenant_of(tenant),
        |keys, referent| keys.requested(referent),
    )
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_types::sync::wire::SyncProvenance;

    use super::super::keys::tests::{TENANT, coll, key, scope, write};
    use super::super::plan::plan_footprint;
    use super::super::registry::tests::{FakeRegistry, NO_REGISTRY};
    use super::*;
    use crate::data::executor::core_loop::install_fence::FenceKey;
    use crate::types::DatabaseId;

    /// A registry whose collection `orders` references `users`.
    const REFERENTS: FakeRegistry = FakeRegistry {
        referents: &[("orders", "users")],
        ..NO_REGISTRY
    };

    fn tenant() -> FenceKey {
        FenceKey::CrdtTenant {
            db: DatabaseId::DEFAULT,
            tenant: TENANT,
        }
    }

    fn apply(collection: &str, provenance: Option<SyncProvenance>) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: coll(collection),
            document_id: "d1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: nodedb_types::Surrogate::new(1),
            provenance,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        })
    }

    #[test]
    fn an_apply_keys_its_collection_its_tenant_and_its_referents() {
        let scope = Scope {
            registry: &REFERENTS,
            ..scope()
        };
        assert_eq!(
            plan_footprint(&apply("orders", None), &scope),
            write(vec![key("orders"), tenant(), key("users")])
        );
        assert_eq!(
            plan_footprint(&apply("notes", None), &scope),
            write(vec![key("notes"), tenant()])
        );
    }

    #[test]
    fn a_sync_apply_keys_its_stream() {
        let provenance = SyncProvenance {
            producer_id: 4,
            epoch: 1,
            stream_id: 6,
            seq: 1,
        };
        assert_eq!(
            plan_footprint(&apply("notes", Some(provenance)), &scope()),
            write(vec![
                key("notes"),
                tenant(),
                FenceKey::SyncStream {
                    producer_id: 4,
                    stream_id: 6,
                },
            ])
        );
    }

    #[test]
    fn a_policy_read_keys_its_collection_and_its_tenant() {
        let policy = PhysicalPlan::Crdt(CrdtOp::GetPolicy {
            collection: coll("cr"),
        });
        assert_eq!(
            plan_footprint(&policy, &scope()),
            Footprint::Keys(vec![key("cr"), tenant()])
        );
    }

    #[test]
    fn a_read_keys_its_collection_alone() {
        let read = PhysicalPlan::Crdt(CrdtOp::Read {
            collection: coll("cr"),
            document_id: "d1".into(),
        });
        assert_eq!(
            plan_footprint(&read, &scope()),
            Footprint::Keys(vec![key("cr")])
        );
    }
}
