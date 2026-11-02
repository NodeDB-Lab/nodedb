// SPDX-License-Identifier: BUSL-1.1

//! The arrays a plan reads.
//!
//! Arrays live in their own catalog, so no array is a touched collection.
//! A cached plan over an array still carries its schema (georef, attrs,
//! retention), so it must stop matching once the array changes.

use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp, PhysicalPlan};
use nodedb_types::{DatabaseId, TenantId};

use super::GatewayVersionSet;
use super::keys::array_version_key;
use crate::control::array_catalog::ArrayCatalogHandle;
use crate::control::planner::plan_children::children;

/// The catalog version of array `name` ([`ArrayCatalogEntry::catalog_version`]).
/// `0` when it does not exist.
///
/// [`ArrayCatalogEntry::catalog_version`]: crate::control::array_catalog::ArrayCatalogEntry::catalog_version
pub fn array_version(
    arrays: &ArrayCatalogHandle,
    database_id: u64,
    tenant_id: u64,
    name: &str,
) -> u64 {
    // A poisoned lock still holds a consistent mirror: every writer
    // replaces whole entries.
    let catalog = arrays
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    catalog
        .lookup_by_name_in_database(TenantId::new(tenant_id), DatabaseId::new(database_id), name)
        .map_or(0, |entry| entry.catalog_version())
}

/// `set` with one pseudo-entry per array `plan` reads.
pub fn with_arrays(
    mut set: GatewayVersionSet,
    plan: &PhysicalPlan,
    arrays: &ArrayCatalogHandle,
    database_id: u64,
    tenant_id: u64,
) -> GatewayVersionSet {
    for name in array_names(plan) {
        let version = array_version(arrays, database_id, tenant_id, &name);
        set = set.with_extra(array_version_key(database_id, tenant_id, &name), version);
    }
    set
}

/// Every array name `plan` reads, at any depth. Both operands of an
/// elementwise op count.
pub fn array_names(plan: &PhysicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    collect(plan, &mut out);
    out
}

fn collect(plan: &PhysicalPlan, out: &mut Vec<String>) {
    match plan {
        PhysicalPlan::Array(op) => match op {
            ArrayOp::Elementwise { left, right, .. } => push_pair(left, right, out),
            other => out.push(other.primary_array().name.clone()),
        },
        PhysicalPlan::ClusterArray(op) => match op {
            ClusterArrayOp::Elementwise { left, right, .. } => push_pair(left, right, out),
            other => out.push(other.array_id().name.clone()),
        },
        _ => {}
    }
    for child in children(plan) {
        collect(child, out);
    }
}

fn push_pair(left: &ArrayId, right: &ArrayId, out: &mut Vec<String>) {
    out.push(left.name.clone());
    out.push(right.name.clone());
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, QueryOp};

    fn aid(name: &str) -> ArrayId {
        ArrayId::in_database(TenantId::new(1), DatabaseId::new(1), name)
    }

    fn flush(name: &str) -> PhysicalPlan {
        PhysicalPlan::Array(ArrayOp::Flush {
            array_id: aid(name),
            wal_lsn: 0,
        })
    }

    #[test]
    fn a_nested_array_op_is_found() {
        let plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(flush("scene")),
            mode: ExchangeMode::Broadcast,
        }));
        assert_eq!(array_names(&plan), vec!["scene".to_string()]);
    }

    #[test]
    fn a_plan_without_arrays_names_none() {
        let plan = PhysicalPlan::Query(QueryOp::ProviderScan {
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
        });
        assert!(array_names(&plan).is_empty());
    }
}
