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

#[test]
fn provider_scan_preserves_only_named_provider() {
    let named = crate::bridge::envelope::PhysicalPlan::Query(QueryOp::ProviderScan {
        provider: Some("_system.audit_log".into()),
        rows: Vec::new(),
        filters: Vec::new(),
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
    });
    let materialized = crate::bridge::envelope::PhysicalPlan::Query(QueryOp::ProviderScan {
        provider: None,
        rows: Vec::new(),
        filters: Vec::new(),
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
    });

    assert_eq!(
        plan_requirements(&named),
        vec![AuthorizationRequirement::collection(
            "_system.audit_log",
            Permission::Read,
        )]
    );
    assert_eq!(
        plan_requirements(&materialized),
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
fn transaction_batch_checks_its_tenant_scope_and_nested_resources() {
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
        vec![
            AuthorizationRequirement::tenant(Permission::Write),
            AuthorizationRequirement::collection("orders", Permission::Read),
        ]
    );
}
