// SPDX-License-Identifier: BUSL-1.1

//! Resolve the [`KvCounterShape`] a KV counter op carries: the row an absent
//! key becomes.

use std::sync::Arc;

use nodedb_physical::physical_plan::KvCounterShape;
use nodedb_sql::SqlCatalog;
use nodedb_sql::planner::dml_helpers::{
    KvCounterFreshRow, KvCounterKind, plan_kv_counter_fresh_row,
};

use super::value::{write_msgpack_map_header, write_msgpack_str, write_msgpack_value};
use crate::control::planner::catalog_adapter::OriginCatalog;
use crate::control::planner::plan_error_map::map_plan_error;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// The shape a SQL counter op on `collection` gives an absent `key`.
///
/// A typed collection creates the row `INSERT (key, column) VALUES (key, n)`
/// stores, DEFAULTs included. A raw collection, or one the catalog does not
/// hold, stores decimal text.
pub(crate) fn kv_counter_shape(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    key: &str,
    kind: KvCounterKind,
) -> crate::Result<KvCounterShape> {
    let catalog = OriginCatalog::new(
        Arc::clone(&state.credentials),
        tenant_id.as_u64(),
        database_id,
        Some(Arc::clone(&state.retention_policy_registry)),
    )
    .with_sequence_registry(Arc::clone(&state.sequence_registry));
    let info = catalog
        .get_collection(database_id, collection)
        .map_err(|e| map_plan_error(e.into(), tenant_id))?;
    let Some(info) = info else {
        return Ok(KvCounterShape::Raw);
    };
    let fresh = plan_kv_counter_fresh_row(&info, key, kind, &catalog)
        .map_err(|e| map_plan_error(e, tenant_id))?;
    Ok(match fresh {
        KvCounterFreshRow::Raw => KvCounterShape::Raw,
        KvCounterFreshRow::Typed { column, cells } => {
            let mut template = Vec::with_capacity(cells.len() * 32);
            write_msgpack_map_header(&mut template, cells.len());
            for (name, value) in &cells {
                write_msgpack_str(&mut template, name);
                write_msgpack_value(&mut template, value);
            }
            KvCounterShape::Typed { column, template }
        }
    })
}
