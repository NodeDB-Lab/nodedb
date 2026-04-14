//! RLS filter injection into physical plans.
//!
//! After the planner converts a logical plan into physical tasks, this module
//! injects Row-Level Security predicates based on the authenticated user's
//! context. The Data Plane receives only concrete `ScanFilter` values — no
//! session or JWT awareness.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{
    ColumnarOp, DocumentOp, GraphOp, KvOp, QueryOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};
use crate::bridge::scan_filter::FilterOp;
use crate::control::planner::physical::PhysicalTask;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::rls::RlsPolicyStore;
use crate::types::TenantId;

/// Inject RLS predicates into physical tasks after plan conversion.
///
/// This is the read-path RLS enforcement entry point. For each task:
/// 1. Extracts the collection name from the physical plan.
/// 2. Fetches RLS read policies for `(tenant_id, collection)`.
/// 3. Substitutes `$auth.*` references using the `AuthContext`.
/// 4. Injects the resulting concrete filters into the plan:
///    - **Scans**: merged into the existing `filters` field (AND-combined).
///    - **Point gets**: stored in `rls_filters` for post-fetch evaluation.
///    - **Search ops**: stored in `rls_filters` for post-candidate filtering.
///
/// **Caller**: Session query execution, after DataFusion logical planning.
/// **Superuser bypass**: Handled inside `combined_read_predicate_with_auth`.
///
/// Returns `Err` if a required `$auth` field is missing (fail-closed).
pub fn inject_rls(
    tasks: &mut [PhysicalTask],
    rls_store: &RlsPolicyStore,
    auth: &AuthContext,
) -> crate::Result<()> {
    for task in tasks.iter_mut() {
        let tenant_id = task.tenant_id.as_u32();
        inject_rls_for_plan(tenant_id, &mut task.plan, rls_store, auth)?;
    }
    Ok(())
}

/// Inject RLS into a single physical plan (public for native protocol dispatch).
pub fn inject_rls_for_single_plan(
    tenant_id: u32,
    plan: &mut PhysicalPlan,
    rls_store: &RlsPolicyStore,
    auth: &AuthContext,
) -> crate::Result<()> {
    inject_rls_for_plan(tenant_id, plan, rls_store, auth)
}

/// Core dispatch: inject RLS into a single physical plan.
fn inject_rls_for_plan(
    tenant_id: u32,
    plan: &mut PhysicalPlan,
    rls_store: &RlsPolicyStore,
    auth: &AuthContext,
) -> crate::Result<()> {
    match plan {
        // ── Plans with scan-style `filters` field (merge RLS into existing filters) ──
        PhysicalPlan::Document(DocumentOp::Scan {
            collection,
            filters,
            ..
        })
        | PhysicalPlan::Kv(KvOp::Scan {
            collection,
            filters,
            ..
        })
        | PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            filters,
            ..
        }) => {
            let rls = get_rls(rls_store, tenant_id, collection, auth)?;
            if !rls.is_empty() {
                merge_filters(filters, &rls)?;
            }
        }

        // ── Plans with `rls_filters` field (set directly) ──
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Kv(KvOp::Get {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Vector(VectorOp::Search {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Vector(VectorOp::MultiSearch {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Text(TextOp::Search {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Text(TextOp::HybridSearch {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Spatial(SpatialOp::Scan {
            collection,
            rls_filters,
            ..
        }) => {
            let rls = get_rls(rls_store, tenant_id, collection, auth)?;
            if !rls.is_empty() {
                *rls_filters = rls;
            }
        }

        // ── Plans that deny if RLS policies exist (unsupported) ──
        PhysicalPlan::Document(DocumentOp::RangeScan { collection, .. })
        | PhysicalPlan::Document(DocumentOp::IndexLookup { collection, .. })
        | PhysicalPlan::Kv(KvOp::BatchGet { collection, .. })
        | PhysicalPlan::Kv(KvOp::FieldGet { collection, .. }) => {
            let rls = get_rls(rls_store, tenant_id, collection, auth)?;
            if !rls.is_empty() {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "RLS policies on '{collection}' not supported with this operation type"
                    ),
                });
            }
        }

        // ── Graph: per-node RLS deferred to Data Plane handler ──
        PhysicalPlan::Graph(
            GraphOp::Hop { rls_filters, .. }
            | GraphOp::Neighbors { rls_filters, .. }
            | GraphOp::Path { rls_filters, .. }
            | GraphOp::Subgraph { rls_filters, .. },
        ) => {
            // Graph traversal RLS is applied per-node by the Data Plane handler.
            // Graph nodes accessed as documents get filtered via DocumentOp::PointGet RLS.
            let _ = rls_filters;
        }

        // Write operations, DDL, meta — no read RLS needed.
        _ => {}
    }

    Ok(())
}

/// Fetch RLS bytes for a (tenant, collection) pair.
fn get_rls(
    rls_store: &RlsPolicyStore,
    tenant_id: u32,
    collection: &str,
    auth: &AuthContext,
) -> crate::Result<Vec<u8>> {
    rls_store
        .combined_read_predicate_with_auth(tenant_id, collection, auth)
        .ok_or_else(|| rls_deny_error(tenant_id, collection))
}

/// Merge RLS filter bytes into existing filter bytes.
///
/// If existing filters are empty, replace. Otherwise deserialize both,
/// concatenate (AND-combine), and re-serialize.
///
/// Returns `Err` on serialization failure — fail-closed to prevent
/// silently dropping security filters.
fn merge_filters(existing: &mut Vec<u8>, rls_bytes: &[u8]) -> crate::Result<()> {
    if existing.is_empty() {
        *existing = rls_bytes.to_vec();
        return Ok(());
    }

    let mut all: Vec<crate::bridge::scan_filter::ScanFilter> = zerompk::from_msgpack(existing)
        .map_err(|e| crate::Error::PlanError {
            detail: format!("RLS filter deserialization failed (existing): {e}"),
        })?;
    let rls: Vec<crate::bridge::scan_filter::ScanFilter> = zerompk::from_msgpack(rls_bytes)
        .map_err(|e| crate::Error::PlanError {
            detail: format!("RLS filter deserialization failed (new): {e}"),
        })?;
    all.extend(rls);
    *existing = zerompk::to_msgpack_vec(&all).map_err(|e| crate::Error::PlanError {
        detail: format!("RLS filter serialization failed: {e}"),
    })?;
    Ok(())
}

/// Create a deny error for unresolved RLS auth references.
fn rls_deny_error(tenant_id: u32, collection: &str) -> crate::Error {
    crate::Error::RejectedAuthz {
        tenant_id: TenantId::new(tenant_id),
        resource: format!(
            "RLS policy on '{}': unresolved session variable (deny by default)",
            collection
        ),
    }
}

// ── Permission Tree Injection ──────────────────────────────────────────────

/// Inject permission tree filters into physical tasks.
///
/// For each task whose collection has a `PermissionTreeDef`, resolves the
/// set of accessible resource IDs for the current user and injects an
/// `IN (...)` ScanFilter on the resource column.
///
/// Called after `inject_rls()` — permission tree filters are AND-combined
/// with standard RLS filters.
///
/// Superusers bypass permission tree filtering entirely.
pub fn inject_permission_tree(
    tasks: &mut [PhysicalTask],
    cache: &crate::control::security::permission_tree::PermissionCache,
    auth: &AuthContext,
) -> crate::Result<()> {
    if auth.is_superuser() {
        return Ok(());
    }
    for task in tasks.iter_mut() {
        let tenant_id = task.tenant_id.as_u32();
        inject_permission_tree_for_plan(tenant_id, &mut task.plan, cache, auth)?;
    }
    Ok(())
}

/// Core dispatch: inject permission tree filter into a single physical plan.
///
/// Selects the required permission level based on the operation type:
/// - Scans/reads → `def.read_level`
/// - Upserts/inserts → `def.write_level`
/// - Deletes → `def.delete_level`
fn inject_permission_tree_for_plan(
    tenant_id: u32,
    plan: &mut PhysicalPlan,
    cache: &crate::control::security::permission_tree::PermissionCache,
    auth: &AuthContext,
) -> crate::Result<()> {
    // Extract (collection, mutable filters, required_level_key).
    let (collection, filters, level_key) = match plan {
        // Read operations.
        PhysicalPlan::Document(DocumentOp::Scan {
            collection,
            filters,
            ..
        })
        | PhysicalPlan::Kv(KvOp::Scan {
            collection,
            filters,
            ..
        })
        | PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            filters,
            ..
        }) => (collection.as_str(), Some(filters), PermTreeLevel::Read),

        // Write operations (upsert/insert routed through DocumentOp::Upsert).
        PhysicalPlan::Document(DocumentOp::Upsert { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. })
        | PhysicalPlan::Kv(KvOp::Put { collection, .. })
        | PhysicalPlan::Kv(KvOp::BatchPut { collection, .. }) => {
            (collection.as_str(), None, PermTreeLevel::Write)
        }

        // Delete operations.
        PhysicalPlan::Document(DocumentOp::PointDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Truncate { collection, .. })
        | PhysicalPlan::Kv(KvOp::Delete { collection, .. }) => {
            (collection.as_str(), None, PermTreeLevel::Delete)
        }

        _ => return Ok(()),
    };

    let Some(def) = cache.get_tree_def(tenant_id, collection) else {
        return Ok(()); // No permission tree on this collection.
    };

    let required_level = match level_key {
        PermTreeLevel::Read => &def.read_level,
        PermTreeLevel::Write => &def.write_level,
        PermTreeLevel::Delete => &def.delete_level,
    };

    let user_id = &auth.id;
    let user_roles = &auth.roles;

    // For write/delete operations without scan filters, do a blanket permission check:
    // the user must have at least the required level on ANY resource in the tree.
    // Per-row filtering is only possible for scan operations.
    if filters.is_none() {
        let accessible = crate::control::security::permission_tree::resolver::accessible_resources(
            cache,
            def,
            tenant_id,
            user_id,
            user_roles,
            required_level,
        );
        if accessible.is_empty() {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: TenantId::new(tenant_id),
                resource: format!(
                    "permission tree on '{collection}': user has no '{required_level}' access"
                ),
            });
        }
        return Ok(());
    }

    // For scan operations, inject an IN filter on the resource column.
    let accessible = crate::control::security::permission_tree::resolver::accessible_resources(
        cache,
        def,
        tenant_id,
        user_id,
        user_roles,
        required_level,
    );

    let in_filter = crate::bridge::scan_filter::ScanFilter {
        field: def.resource_column.clone(),
        op: FilterOp::In,
        value: nodedb_types::Value::Array(
            accessible
                .into_iter()
                .map(nodedb_types::Value::String)
                .collect(),
        ),
        clauses: Vec::new(),
        expr: None,
    };

    let filter_bytes =
        zerompk::to_msgpack_vec(&vec![in_filter]).map_err(|e| crate::Error::PlanError {
            detail: format!("permission tree filter serialization: {e}"),
        })?;

    if let Some(filters) = filters {
        merge_filters(filters, &filter_bytes)?;
    }

    Ok(())
}

/// Which permission level to check based on operation type.
enum PermTreeLevel {
    Read,
    Write,
    Delete,
}
