// SPDX-License-Identifier: BUSL-1.1

//! Refusal of an orchestrated write whose source carries a redaction rule.
//!
//! `INSERT ... SELECT`, `MERGE`, and `UPDATE ... FROM` read their source on
//! the Control Plane and write its values into the target. Those rows never
//! pass the result-path masking hook, so a masked column would land in the
//! target unmasked, where the same requester can read it back. The write is
//! refused while any of the requester's roles has a rule on a source
//! collection.

use nodedb_physical::physical_plan::{DocumentOp, RowSource};

use crate::control::gateway::version_set::{provider_names, touched_collections};

use super::lookup::RefusalCtx;

/// The statement and source relation of an orchestrated write, or `None` for
/// any other document op.
pub(super) fn orchestrated_source(op: &DocumentOp) -> Option<(&'static str, &RowSource)> {
    match op {
        DocumentOp::InsertSelect { source, .. } => Some(("INSERT ... SELECT", source)),
        DocumentOp::Merge { source, .. } => Some(("MERGE", source)),
        DocumentOp::UpdateFromJoin { source, .. } => Some(("UPDATE ... FROM", source)),
        DocumentOp::ResolveWrite(inner) => orchestrated_source(inner),
        DocumentOp::PointGet { .. }
        | DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::ResolvedWrite { .. } => None,
    }
}

/// Refuse `source` when one of the requester's roles has a redaction rule on
/// a collection it reads.
pub(super) fn refuse_redacted_source(
    ctx: &RefusalCtx<'_>,
    statement: &str,
    source: &RowSource,
) -> crate::Result<()> {
    // A query source reads its collections and its provider relations: a
    // continuous aggregate or a catalog table routes nowhere, so it is no
    // touched collection, yet its rows reach the target all the same.
    let collections = match source {
        RowSource::DocumentPages(page) => vec![page.collection.as_str().to_owned()],
        RowSource::Query(query) => {
            let mut names = touched_collections(&query.plan);
            names.extend(provider_names(&query.plan).into_iter().map(|name| {
                nodedb_types::QualifiedCollection::new(ctx.database_id, &name)
                    .as_str()
                    .to_owned()
            }));
            names
        }
    };
    match collections
        .iter()
        .find(|collection| !collection.is_empty() && ctx.collection_is_redacted(collection))
    {
        Some(collection) => Err(ctx.refusal(format!(
            "redaction policies on '{collection}' are not supported as the source of \
             {statement}: the copied values bypass column redaction"
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{KvOp, QuerySource};
    use nodedb_types::{DatabaseId, QualifiedCollection};

    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::security::redaction::{
        RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
    };

    use super::*;

    const TENANT: u64 = 1;
    const ROLE: &str = "analyst";

    fn store_with_rule(collection: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_mask"),
            tenant_id: TENANT,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: ROLE.into(),
            rules: vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        store
    }

    fn check(store: &RedactionStore, op: &DocumentOp) -> crate::Result<()> {
        let roles = [ROLE.to_string()];
        let ctx = RefusalCtx {
            store,
            tenant_id: TENANT,
            roles: &roles,
            database_id: DatabaseId::DEFAULT,
        };
        match orchestrated_source(op) {
            Some((statement, source)) => refuse_redacted_source(&ctx, statement, source),
            None => Ok(()),
        }
    }

    fn insert_select(source: RowSource) -> DocumentOp {
        DocumentOp::InsertSelect {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "copy"),
            source,
        }
    }

    #[test]
    fn a_redacted_page_source_is_refused() {
        let store = store_with_rule("users");
        let op = insert_select(RowSource::whole_collection(QualifiedCollection::new(
            DatabaseId::DEFAULT,
            "users",
        )));
        match check(&store, &op) {
            Err(crate::Error::RejectedAuthz { resource, .. }) => {
                assert!(resource.contains("'users'") && resource.contains("INSERT ... SELECT"));
            }
            other => panic!("expected a redaction refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_redacted_query_source_is_refused() {
        let store = store_with_rule("profiles");
        let op = insert_select(RowSource::Query(QuerySource {
            plan: Box::new(PhysicalPlan::Kv(KvOp::Get {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "profiles"),
                key: b"k".to_vec(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
                system_time: nodedb_types::SystemTimeScope::Current,
            })),
            column_map: Vec::new(),
        }));
        assert!(matches!(
            check(&store, &op),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A continuous-aggregate source names no plan collection; its provider
    /// name is checked all the same.
    #[test]
    fn a_redacted_provider_source_is_refused() {
        let store = store_with_rule("salary_1h");
        let op = insert_select(RowSource::Query(QuerySource {
            plan: Box::new(PhysicalPlan::Query(
                nodedb_physical::physical_plan::QueryOp::ProviderScan {
                    provider: Some("salary_1h".into()),
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
                    system_time: nodedb_types::SystemTimeScope::Current,
                },
            )),
            column_map: Vec::new(),
        }));
        assert!(matches!(
            check(&store, &op),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    #[test]
    fn a_source_without_a_rule_is_admitted() {
        let store = store_with_rule("users");
        let op = insert_select(RowSource::whole_collection(QualifiedCollection::new(
            DatabaseId::DEFAULT,
            "orders",
        )));
        assert!(check(&store, &op).is_ok());
    }
}
