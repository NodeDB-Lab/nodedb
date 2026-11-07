// SPDX-License-Identifier: BUSL-1.1

//! The named provider relations a plan reads.
//!
//! A `ProviderScan { provider: Some(name) }` names a relation the
//! coordinator materializes: a catalog table or a continuous aggregate. It
//! routes nowhere, so it is no touched collection, but a cached plan over a
//! continuous aggregate must stop matching once the aggregate changes.

use nodedb_physical::physical_plan::{ExchangeOp, PhysicalPlan, QueryOp};

use super::GatewayVersionSet;
use super::keys::continuous_aggregate_version_key;
use crate::control::security::catalog::SystemCatalog;

/// The catalog version of continuous aggregate `name`: the HLC of its last
/// applied catalog write, which a re-create always moves. `0` when it does
/// not exist.
pub fn continuous_aggregate_version(
    catalog: &SystemCatalog,
    database_id: u64,
    tenant_id: u64,
    name: &str,
) -> crate::Result<u64> {
    Ok(catalog
        .get_continuous_aggregate(database_id, tenant_id, name)?
        .map_or(0, |stored| stored.modification_hlc.wall_ns.max(1)))
}

/// `set` with one pseudo-entry per continuous aggregate `plan` reads.
pub fn with_continuous_aggregates(
    mut set: GatewayVersionSet,
    plan: &PhysicalPlan,
    catalog: &SystemCatalog,
    database_id: u64,
    tenant_id: u64,
) -> crate::Result<GatewayVersionSet> {
    for name in provider_names(plan) {
        let version = continuous_aggregate_version(catalog, database_id, tenant_id, &name)?;
        if version != 0 {
            set = set.with_extra(
                continuous_aggregate_version_key(database_id, tenant_id, &name),
                version,
            );
        }
    }
    Ok(set)
}

/// Every provider name `plan` reads, in plan order. Walks the same nesting
/// provider materialization walks.
pub fn provider_names(plan: &PhysicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    collect(plan, &mut out);
    out
}

fn collect(plan: &PhysicalPlan, out: &mut Vec<String>) {
    let PhysicalPlan::Query(op) = plan else {
        return;
    };
    match op {
        QueryOp::ProviderScan {
            provider: Some(name),
            ..
        } => out.push(name.clone()),
        QueryOp::Exchange(ExchangeOp { child, .. }) => collect(child, out),
        QueryOp::Aggregate {
            input: Some(input), ..
        }
        | QueryOp::PostProcess { input, .. } => collect(input, out),
        QueryOp::HashJoin {
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            ..
        } => {
            for side in [left_input, right_input, left_bitmap, right_bitmap]
                .into_iter()
                .flatten()
            {
                collect(side, out);
            }
        }
        QueryOp::LateralTopK { outer_plan, .. } | QueryOp::LateralLoop { outer_plan, .. } => {
            collect(outer_plan, out)
        }
        QueryOp::SetOp { inputs, .. } => {
            for input in inputs {
                collect(input, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(name: &str) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: Some(name.into()),
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
        })
    }

    #[test]
    fn nested_providers_are_found() {
        let plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(provider("cpu_1m")),
            mode: nodedb_physical::physical_plan::ExchangeMode::Broadcast,
        }));
        assert_eq!(provider_names(&plan), vec!["cpu_1m".to_string()]);
        assert!(
            provider_names(&PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
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
            }))
            .is_empty()
        );
    }
}
