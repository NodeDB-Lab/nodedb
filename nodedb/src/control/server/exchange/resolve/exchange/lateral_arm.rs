// SPDX-License-Identifier: BUSL-1.1

//! LATERAL outer-side resolution.
//!
//! A LATERAL task runs on the vShard that owns its inner collection, so the
//! Data Plane scans the inner side where its rows live. The outer side reads
//! another collection, or needs the coordinator (a set operation, a
//! post-processed body, a gather). It is materialized here on the vShards
//! that own it and embedded as a `ProviderScan` of its rows.

use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

use crate::control::server::exchange::full_scan::require_active_collection;
use crate::control::server::exchange::read_scope::ReadScope;
use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::state::SharedState;

use super::entry::Resolved;
use super::post_process_arm::{ChildRows, materialize_child_rows, provider_scan_of_rows};

/// Resolve a `LateralTopK` / `LateralLoop` node.
///
/// The inner collection must exist and be active: the Data Plane scans it by
/// name, and a by-name scan of a missing collection answers no rows. An outer
/// plan that already holds its rows passes through unchanged. Any other outer
/// plan is materialized on the coordinator, and its rows replace it.
pub(super) async fn resolve_lateral_outer(
    state: &SharedState,
    ctx: ReadScope,
    captures: &mut Vec<DistributedReadCapture>,
    mut plan: PhysicalPlan,
) -> crate::Result<Resolved> {
    if let PhysicalPlan::Query(
        QueryOp::LateralTopK {
            outer_plan,
            inner_collection,
            ..
        }
        | QueryOp::LateralLoop {
            outer_plan,
            inner_collection,
            ..
        },
    ) = &mut plan
    {
        require_active_collection(
            state,
            ctx.database_id,
            ctx.tenant_id,
            inner_collection.as_str(),
        )?;
        if let Some(name) = unfilled_provider(outer_plan) {
            // Provider rows are identity-scoped. They are filled before
            // resolution, where the request identity is live.
            return Err(crate::Error::Internal {
                detail: format!(
                    "LATERAL: the outer relation '{name}' reached resolution without its rows"
                ),
            });
        }
        if !holds_rows(outer_plan) {
            let outer = (**outer_plan).clone();
            match materialize_child_rows(state, ctx, captures, outer).await? {
                ChildRows::Rows { rows, sources } => {
                    **outer_plan = provider_scan_of_rows(rows, sources)
                }
                ChildRows::Passthrough(_) => {
                    return Err(crate::Error::Internal {
                        detail:
                            "LATERAL: the outer side resolved to a final result instead of rows"
                                .to_string(),
                    });
                }
            }
        }
    }
    Ok(Resolved::Plan(Box::new(plan)))
}

/// Whether `plan` is a `ProviderScan` that carries its rows, which runs on
/// any core.
fn holds_rows(plan: &PhysicalPlan) -> bool {
    matches!(plan, PhysicalPlan::Query(QueryOp::ProviderScan { .. }))
        && unfilled_provider(plan).is_none()
}

/// The relation name of a named `ProviderScan` whose rows are not filled.
/// A filled scan always holds an array header, even for zero rows.
fn unfilled_provider(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: Some(name),
            rows,
            ..
        }) if rows.is_empty() => Some(name),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::{DatabaseId, QualifiedCollection, SystemTimeScope, TenantId};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::catalog::StoredCollection;
    use crate::data::executor::response_codec::encode_binary_rows;
    use crate::types::TraceId;
    use crate::wal::WalManager;

    const TENANT: TenantId = TenantId::new(7);
    const DB: DatabaseId = DatabaseId::DEFAULT;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create lateral test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("lateral.wal"))
                .expect("open lateral test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(2, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct lateral state");
        (state, directory)
    }

    fn scope() -> ReadScope {
        ReadScope {
            database_id: DB,
            tenant_id: TENANT,
            trace_id: TraceId::ZERO,
            txn_id: None,
            linearizable: false,
        }
    }

    fn lateral(outer: PhysicalPlan, inner: &str) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::LateralLoop {
            outer_plan: Box::new(outer),
            outer_alias: "o".into(),
            inner_collection: QualifiedCollection::new(DB, inner),
            inner_filters: Vec::new(),
            correlation_predicates: Vec::new(),
            lateral_alias: "x".into(),
            projection: Vec::new(),
            left_join: false,
            outer_row_cap: 10,
            inner_system_time: SystemTimeScope::Current,
        })
    }

    fn filled_outer() -> PhysicalPlan {
        provider_scan_of_rows(encode_binary_rows(&[]), Vec::new())
    }

    fn unfilled_outer() -> PhysicalPlan {
        let mut plan = filled_outer();
        if let PhysicalPlan::Query(QueryOp::ProviderScan { provider, rows, .. }) = &mut plan {
            *provider = Some("pg_tables".into());
            rows.clear();
        }
        plan
    }

    fn put(state: &SharedState, name: &str, active: bool) {
        let mut stored = StoredCollection::stamped_for_test(TENANT.as_u64(), name, "tester");
        stored.is_active = active;
        state
            .credentials
            .catalog()
            .put_collection(DB, &stored)
            .expect("persist collection");
    }

    async fn resolve(state: &SharedState, plan: PhysicalPlan) -> crate::Result<Resolved> {
        let mut captures = Vec::new();
        resolve_lateral_outer(state, scope(), &mut captures, plan).await
    }

    #[tokio::test]
    async fn a_missing_inner_collection_is_collection_not_found() {
        let (state, _dir) = test_state();
        let error = resolve(&state, lateral(filled_outer(), "missing"))
            .await
            .err()
            .expect("a missing inner collection must not resolve");
        assert!(
            matches!(&error, crate::Error::CollectionNotFound { collection, .. } if collection == "missing"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_dropped_inner_collection_is_collection_deactivated() {
        let (state, _dir) = test_state();
        put(&state, "gone", false);
        let error = resolve(&state, lateral(filled_outer(), "gone"))
            .await
            .err()
            .expect("a dropped inner collection must not resolve");
        assert!(
            matches!(error, crate::Error::CollectionDeactivated { .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn an_unfilled_provider_outer_is_an_error_and_a_filled_one_passes() {
        let (state, _dir) = test_state();
        put(&state, "events", true);
        let error = resolve(&state, lateral(unfilled_outer(), "events"))
            .await
            .err()
            .expect("an unfilled provider must not read as zero rows");
        assert!(
            matches!(&error, crate::Error::Internal { detail } if detail.contains("pg_tables")),
            "{error:?}"
        );

        let plan = lateral(filled_outer(), "events");
        let Ok(Resolved::Plan(resolved)) = resolve(&state, plan.clone()).await else {
            panic!("a filled outer passes through");
        };
        assert_eq!(*resolved, plan);
    }
}
