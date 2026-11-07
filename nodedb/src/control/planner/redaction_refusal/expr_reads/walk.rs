// SPDX-License-Identifier: BUSL-1.1

//! Refusal of a plan that reads a redacted column outside its projection.
//!
//! Redaction masks the result rows in the Control Plane. A plain projection
//! of a redacted column is masked there. Every other read consumes the
//! stored value on the Data Plane before any mask applies:
//!
//! - an expression computes over it (`upper(ssn)`, `ssn || ''`),
//! - a predicate filters on it (`WHERE ssn = '123-45-6789'`),
//! - a sort orders by it, a group key or `DISTINCT` buckets by it,
//! - a join or an index probe matches on it,
//! - an assignment copies it into another column.
//!
//! Each leaks the stored value or a fact about it, so the statement is
//! refused with `42501`, naming the column.
//!
//! The check runs before row-level security is injected. A read policy's
//! own predicate can read a redacted column. It is the administrator's
//! predicate, not the requester's, and it must not trigger the refusal.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::plan_children::children;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::redaction::RedactionStore;
use crate::control::server::response_shape::redaction::plan_source_collections;
use crate::types::TenantId;

use super::super::lookup::RefusalCtx;
use super::reads::{Clause, NodeReads};
use super::{array, columnar, document, kv, query, search};

/// The stores a refusal reads: the policy lookups, and the catalog that
/// names each collection's key column.
pub struct RedactionReadCheck<'a> {
    pub auth: &'a AuthContext,
    pub store: &'a RedactionStore,
    pub catalog: &'a SystemCatalog,
}

/// Refuse `plan` when it reads a column one of the requester's roles
/// redacts, outside a plain projection.
///
/// Call it before `rls_injection` rewrites the plan's predicates.
pub fn refuse_redacted_expression_reads(
    plan: &PhysicalPlan,
    tenant_id: TenantId,
    database_id: nodedb_types::DatabaseId,
    check: &RedactionReadCheck<'_>,
) -> crate::Result<()> {
    if check.auth.roles.is_empty() {
        return Ok(());
    }
    let ctx = RefusalCtx {
        store: check.store,
        tenant_id: tenant_id.as_u64(),
        roles: &check.auth.roles,
        database_id,
    };
    if !ctx.identity_has_any_rule() {
        return Ok(());
    }
    walk(plan, &ctx, check.catalog).map(|_| ())
}

/// [`refuse_redacted_expression_reads`] over every task, each under its own
/// tenant and database.
pub fn refuse_redacted_expression_reads_in_tasks(
    tasks: &[PhysicalTask],
    check: &RedactionReadCheck<'_>,
) -> crate::Result<()> {
    for task in tasks {
        refuse_redacted_expression_reads(&task.plan, task.tenant_id, task.database_id, check)?;
    }
    Ok(())
}

/// Check `plan` and its children. Returns the output names under which the
/// node emits a redacted stored value: a renamed redacted column.
fn walk(
    plan: &PhysicalPlan,
    ctx: &RefusalCtx<'_>,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<String>> {
    let mut renamed = Vec::new();
    for child in children(plan) {
        renamed.extend(walk(child, ctx, catalog)?);
    }
    array::refuse_array_reads(plan, ctx)?;
    let mut reads = NodeReads::default();
    node_reads(plan, &mut reads)?;
    if reads.is_empty() {
        return Ok(renamed);
    }
    let sources = plan_source_collections(plan, ctx.database_id);
    if reads.key_lookup {
        refuse_key_lookup(ctx, catalog, &sources)?;
    }
    let protected = |field: &str| -> Option<String> {
        if names_one_of(&renamed, field) {
            return Some("a renamed redacted column".to_string());
        }
        ctx.redacted_source(&sources, field)
            .map(|collection| format!("'{collection}'"))
    };

    for (field, clause) in &reads.reads {
        if let Some(origin) = protected(field) {
            return Err(refusal(ctx, field, &origin, *clause));
        }
    }
    if let Some(clause) = reads.every_column {
        let redacted = sources
            .iter()
            .find(|(_, collection)| ctx.collection_is_redacted(collection));
        if let Some((_, collection)) = redacted {
            return Err(refusal(ctx, "*", &format!("'{collection}'"), clause));
        }
        if let Some(name) = renamed.first() {
            return Err(refusal(ctx, name, "a renamed redacted column", clause));
        }
    }
    for name in &reads.passthrough {
        if names_one_of(&renamed, name) {
            return Err(refusal(
                ctx,
                name,
                "a renamed redacted column",
                Clause::Projection,
            ));
        }
    }
    let mut out = renamed.clone();
    for (alias, field) in &reads.renames {
        if protected(field).is_some() {
            out.push(alias.clone());
        }
    }
    Ok(out)
}

/// Whether `field`, as written or by its column part, is one of `names`.
fn names_one_of(names: &[String], field: &str) -> bool {
    let column = field.rsplit_once('.').map_or(field, |(_, column)| column);
    names.iter().any(|name| name == field || name == column)
}

fn refusal(ctx: &RefusalCtx<'_>, field: &str, origin: &str, clause: Clause) -> crate::Error {
    ctx.refusal(format!(
        "column '{field}' on {origin} is redacted for this role: {} is not permitted, \
         because the stored value is read before the redaction applies",
        clause.describe()
    ))
}

/// Refuse a lookup by key when the key column of a source is redacted.
///
/// The key column is the collection's primary key, `id` when it declares
/// none. A collection the catalog does not hold keys by `id`.
fn refuse_key_lookup(
    ctx: &RefusalCtx<'_>,
    catalog: &SystemCatalog,
    sources: &[(String, String)],
) -> crate::Result<()> {
    for (_, stored) in sources {
        if stored.is_empty() || !ctx.collection_is_redacted(stored) {
            continue;
        }
        let bare = nodedb_types::CollectionKey::from_qualified_str(ctx.database_id, stored)?.name();
        let key = catalog
            .get_collection(ctx.database_id, ctx.tenant_id, bare)?
            .and_then(|collection| {
                crate::control::planner::catalog_adapter::convert_collection_type(&collection).2
            })
            .unwrap_or_else(|| nodedb_types::DEFAULT_IDENTITY_COLUMN.to_string());
        if ctx.field_is_redacted(stored, &key) {
            return Err(refusal(
                ctx,
                &key,
                &format!("'{stored}'"),
                Clause::KeyLookup,
            ));
        }
    }
    Ok(())
}

/// The columns `plan` itself reads. Exhaustive over [`PhysicalPlan`].
///
/// A graph traversal over a redacted collection is refused whole by the
/// graph check. An array read under a policy is refused whole by
/// [`array::refuse_array_reads`]. CRDT and maintenance operations carry no
/// column predicate or expression.
fn node_reads(plan: &PhysicalPlan, out: &mut NodeReads) -> crate::Result<()> {
    match plan {
        PhysicalPlan::Query(op) => query::reads(op, out),
        PhysicalPlan::Document(op) => document::reads(op, out),
        PhysicalPlan::Kv(op) => kv::reads(op, out),
        PhysicalPlan::Columnar(op) => columnar::columnar_reads(op, out),
        PhysicalPlan::Timeseries(op) => columnar::timeseries_reads(op, out),
        PhysicalPlan::Spatial(op) => columnar::spatial_reads(op, out),
        PhysicalPlan::Text(op) => search::text_reads(op, out),
        PhysicalPlan::Vector(op) => search::vector_reads(op, out),
        PhysicalPlan::Graph(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{ClusterArrayOp, QueryOp};
    use nodedb_types::SystemTimeScope;

    use super::*;
    use crate::control::security::credential::CredentialStore;
    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};

    /// The walk reaches an array read under a post-processing tail, the shape
    /// `SELECT x FROM ARRAY_SLICE(...) WHERE secret > 0` plans to.
    #[test]
    fn the_walk_refuses_an_array_read_under_a_tail() {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: "p".to_string(),
            tenant_id: 1,
            collection: "grid".to_string(),
            display_collection: "grid".to_string(),
            for_role: "support".to_string(),
            rules: vec![RedactionRule {
                field: "secret".to_string(),
                mode: RedactionMode::Hash,
            }],
        });
        let slice = PhysicalPlan::ClusterArray(ClusterArrayOp::Slice {
            array_id: nodedb_array::types::ArrayId::new(TenantId::new(1), "grid"),
            slice_msgpack: Vec::new(),
            attr_projection: Vec::new(),
            limit: 0,
            slice_tile_keys: Vec::new(),
            prefix_bits: 8,
            system_time: SystemTimeScope::Current,
            valid_at_ms: None,
            zone_msgpack: Vec::new(),
            physical: false,
        });
        let tail = PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(slice),
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        });

        let tmp = tempfile::tempdir().expect("tmpdir");
        let credentials =
            CredentialStore::open(&tmp.path().join("system.redb")).expect("credential store");
        let roles = vec!["support".to_string()];
        let ctx = RefusalCtx {
            store: &store,
            tenant_id: 1,
            roles: &roles,
            database_id: nodedb_types::DatabaseId::DEFAULT,
        };
        assert!(matches!(
            walk(&tail, &ctx, credentials.catalog()),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }
}
