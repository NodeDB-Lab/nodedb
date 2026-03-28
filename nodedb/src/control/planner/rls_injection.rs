//! RLS filter injection into physical plans.
//!
//! After the planner converts a logical plan into physical tasks, this module
//! injects Row-Level Security predicates based on the authenticated user's
//! context. The Data Plane receives only concrete `ScanFilter` values — no
//! session or JWT awareness.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{
    DocumentOp, GraphOp, KvOp, QueryOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};
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
                merge_filters(filters, &rls);
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
fn merge_filters(existing: &mut Vec<u8>, rls_bytes: &[u8]) {
    if existing.is_empty() {
        *existing = rls_bytes.to_vec();
        return;
    }

    let mut all: Vec<crate::bridge::scan_filter::ScanFilter> =
        rmp_serde::from_slice(existing).unwrap_or_default();
    let rls: Vec<crate::bridge::scan_filter::ScanFilter> =
        rmp_serde::from_slice(rls_bytes).unwrap_or_default();
    all.extend(rls);
    *existing = rmp_serde::to_vec_named(&all).unwrap_or_default();
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
