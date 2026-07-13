//! Physical-plan authorization requirements.
//!
//! This deliberately does not use `shared::plan_util::extract_collection`: a
//! single collection cannot represent joins or source/target DML correctly.

#![deny(clippy::wildcard_enum_match_arm)]

use crate::control::security::identity::Permission;

mod collect;
mod order;
mod query;

pub use collect::plan_requirements;

/// A protected resource and the permission needed to use it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AuthorizationRequirement {
    /// A collection-scoped operation. The name is the physical-plan name and
    /// may be database-qualified; the authorization service normalizes it to
    /// the grant-store name before looking up grants.
    Collection {
        collection: String,
        permission: Permission,
    },
    /// An operation with no collection-level resource, such as an array or a
    /// tenant-wide maintenance action. It must still be authorized at tenant
    /// scope rather than silently allowed.
    Tenant { permission: Permission },
}

impl AuthorizationRequirement {
    fn collection(collection: impl Into<String>, permission: Permission) -> Self {
        Self::Collection {
            collection: collection.into(),
            permission,
        }
    }

    fn tenant(permission: Permission) -> Self {
        Self::Tenant { permission }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, KvOp, MetaOp, QueryOp};

    #[test]
    fn insert_select_requires_source_read_and_target_write() {
        let plan = crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: "target".into(),
            source_collection: "source".into(),
            source_filters: Vec::new(),
            source_limit: 0,
        });

        assert_eq!(
            plan_requirements(&plan),
            vec![
                AuthorizationRequirement::collection("source", Permission::Read),
                AuthorizationRequirement::collection("target", Permission::Write),
            ]
        );
    }

    fn provider_scan(provider: Option<&str>) -> crate::bridge::envelope::PhysicalPlan {
        crate::bridge::envelope::PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: provider.map(str::to_owned),
            rows: Vec::new(),
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        })
    }

    #[test]
    fn resource_less_provider_scan_requires_tenant_permission() {
        assert_eq!(
            plan_requirements(&provider_scan(Some("_system.audit_log"))),
            vec![AuthorizationRequirement::collection(
                "_system.audit_log",
                Permission::Read,
            )]
        );
        assert_eq!(
            plan_requirements(&provider_scan(None)),
            vec![AuthorizationRequirement::tenant(Permission::Read)]
        );
    }

    #[test]
    fn nested_resource_less_plan_keeps_tenant_fallback() {
        let plan = crate::bridge::envelope::PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![provider_scan(None)],
            txn_id: None,
        });
        assert_eq!(
            plan_requirements(&plan),
            vec![AuthorizationRequirement::tenant(Permission::Read)]
        );
    }

    #[test]
    fn crdt_constraint_reads_remain_collection_scoped() {
        let plan = crate::bridge::envelope::PhysicalPlan::Crdt(CrdtOp::ReadConstraints {
            collection: "documents".into(),
        });
        assert_eq!(
            plan_requirements(&plan),
            vec![AuthorizationRequirement::collection(
                "documents",
                Permission::Read,
            )]
        );
    }

    #[test]
    fn nested_collection_plan_does_not_require_tenant_permission() {
        let plan = crate::bridge::envelope::PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![crate::bridge::envelope::PhysicalPlan::Kv(KvOp::Get {
                collection: "orders".into(),
                key: Vec::new(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
            })],
            txn_id: None,
        });
        assert_eq!(
            plan_requirements(&plan),
            vec![AuthorizationRequirement::collection(
                "orders",
                Permission::Read,
            )]
        );
    }
}
