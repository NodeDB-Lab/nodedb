// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::AlterArray` → `PhysicalTask` lowering.
//!
//! Conversion only reads the current catalog entry and validates the requested
//! change. The front door proposes the updated entry as a replicated
//! `PutArray` (`array_catalog::ddl`).

use nodedb_sql::AlterArrayVisitArgs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::array_catalog::attr_meta::remeta_schema;
use crate::control::array_catalog::georef::regeoref_schema;
use crate::control::array_catalog::retention::check_retention_floor;
use crate::types::TenantId;
use nodedb_physical::physical_plan::MetaOp;

use super::convert::ConvertContext;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Convert `SqlPlan::AlterArray` to a `PhysicalTask`.
///
/// Double-`Option` semantics for diff fields:
/// - `None`          = key absent from SET clause → field unchanged.
/// - `Some(None)`    = key present with value `NULL` → unregister.
/// - `Some(Some(v))` = key present with value `v` → update.
///
/// A georef or attr metadata change re-encodes the schema. Its content bytes
/// stay equal, so `schema_hash`, segments, and the Data Plane open check stay
/// valid.
pub(super) fn convert_alter_array(
    args: AlterArrayVisitArgs<'_>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let AlterArrayVisitArgs {
        name,
        audit_retain_ms,
        minimum_audit_retain_ms,
        georef,
        attr_meta,
    } = args;
    ctx.require_execute("ALTER ARRAY")?;
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "ALTER ARRAY: no array catalog wired into convert context".into(),
        })?;
    // 1. Load current entry.
    let current: ArrayCatalogEntry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name_in_database(tenant_id, ctx.database_id, name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("ALTER ARRAY {name}: not found"),
            })?
    };

    // 2. Floor validation before any state mutation. The catalog apply
    // repeats it against the committed entry.
    check_retention_floor(
        name,
        &current,
        audit_retain_ms,
        minimum_audit_retain_ms.map(Some),
    )?;

    // 3. Re-encode the schema for a georef or attr metadata change. One
    // statement makes one change, so at most one applies.
    let statement = format!("ALTER ARRAY {name}");
    let schema_msgpack = match (georef, attr_meta) {
        (Some(change), _) => Some(regeoref_schema(
            &statement,
            &current.schema_msgpack,
            change,
            &ctx.crs_context(),
        )?),
        (None, Some(change)) => Some(remeta_schema(&statement, &current.schema_msgpack, change)?),
        (None, None) => None,
    };

    let vshard = ctx.collection_key(name).vshard();
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Meta(MetaOp::AlterArray {
            array_id: name.to_string(),
            audit_retain_ms,
            minimum_audit_retain_ms: minimum_audit_retain_ms.map(Some),
            schema_msgpack,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
