//! Physical-plan authorization requirements.
//!
//! This deliberately does not use `shared::plan_util::extract_collection`: a
//! single collection cannot represent joins or source/target DML correctly.

#![deny(clippy::wildcard_enum_match_arm)]

use crate::control::security::identity::Permission;

mod collect;
mod gathered;
mod order;
mod query;

pub use collect::plan_requirements;
pub use gathered::gathered_sources;

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
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn insert_select_requires_source_read_and_target_write() {
        let plan = crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "target"),
            source: nodedb_physical::physical_plan::RowSource::DocumentPages(
                nodedb_physical::physical_plan::DocumentPageSource {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "source"),
                    filters: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    offset: 0,
                    limit: None,
                    column_map: Vec::new(),
                },
            ),
        });

        assert_eq!(
            plan_requirements(&plan),
            vec![
                AuthorizationRequirement::collection("source", Permission::Read),
                AuthorizationRequirement::collection("target", Permission::Write),
            ]
        );
    }

    /// A query source requires what its own plan reads: a key-value source
    /// takes `Read` on that collection.
    #[test]
    fn insert_select_from_a_query_source_requires_its_reads() {
        let plan = crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "target"),
            source: nodedb_physical::physical_plan::RowSource::Query(
                nodedb_physical::physical_plan::QuerySource {
                    plan: Box::new(crate::bridge::envelope::PhysicalPlan::Kv(KvOp::Get {
                        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kvsrc"),
                        key: Vec::new(),
                        rls_filters: Vec::new(),
                        surrogate_ceiling: None,
                    })),
                    column_map: Vec::new(),
                },
            ),
        });

        assert_eq!(
            plan_requirements(&plan),
            vec![
                AuthorizationRequirement::collection("kvsrc", Permission::Read),
                AuthorizationRequirement::collection("target", Permission::Write),
            ]
        );
    }

    fn provider_scan(provider: Option<&str>) -> crate::bridge::envelope::PhysicalPlan {
        crate::bridge::envelope::PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: provider.map(str::to_owned),
            rows: Vec::new(),
            sources: Vec::new(),
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
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

    /// Gathered rows demand what the child they came from demanded, and
    /// nothing tenant-wide unless that child did.
    #[test]
    fn gathered_rows_require_their_sources() {
        let gathered = |sources| {
            crate::bridge::envelope::PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: Vec::new(),
                sources,
                filters: Vec::new(),
                projection: Vec::new(),
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            })
        };
        use nodedb_physical::physical_plan::GatheredSource;
        assert_eq!(
            plan_requirements(&gathered(vec![
                GatheredSource::Collection("orders".into()),
                GatheredSource::Collection("users".into()),
            ])),
            vec![
                AuthorizationRequirement::collection("orders", Permission::Read),
                AuthorizationRequirement::collection("users", Permission::Read),
            ]
        );
        assert_eq!(
            plan_requirements(&gathered(vec![
                GatheredSource::Collection("orders".into()),
                GatheredSource::Tenant,
            ])),
            vec![
                AuthorizationRequirement::tenant(Permission::Read),
                AuthorizationRequirement::collection("orders", Permission::Read),
            ]
        );
    }

    /// A post-processor and an input-sourced aggregate demand what their
    /// input demands: no tenant-wide check of their own, and no check on an
    /// empty collection name.
    #[test]
    fn wrappers_over_an_input_delegate_to_it() {
        let post_process = crate::bridge::envelope::PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(provider_scan(Some("orders"))),
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        });
        let aggregate = crate::bridge::envelope::PhysicalPlan::Query(QueryOp::Aggregate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, ""),
            input: Some(Box::new(post_process.clone())),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            filters: Vec::new(),
            having: Vec::new(),
            limit: usize::MAX,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            grouping_sets: Vec::new(),
            sort_keys: Vec::new(),
            group_key_aliases: Vec::new(),
        });
        let orders = vec![AuthorizationRequirement::collection(
            "orders",
            Permission::Read,
        )];
        assert_eq!(plan_requirements(&post_process), orders);
        assert_eq!(plan_requirements(&aggregate), orders);
        let constant = crate::bridge::envelope::PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(provider_scan(None)),
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        });
        assert_eq!(
            plan_requirements(&constant),
            vec![AuthorizationRequirement::tenant(Permission::Read)],
            "constant rows under a wrapper keep the tenant-wide check"
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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "documents"),
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
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
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
