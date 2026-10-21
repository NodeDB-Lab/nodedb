// SPDX-License-Identifier: BUSL-1.1

//! `CREATE ARRAY` / `DROP ARRAY` lowering to `PhysicalTask`.

use std::sync::Arc;

use nodedb_types::config::retention::BitemporalRetention;

use nodedb_array::schema::{
    ArraySchema, ArraySchemaBuilder, AttrSpec, CellOrder, DimSpec, DimType as EngineDimType,
    TileOrder,
};
use nodedb_array::types::ArrayId;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_sql::types_array::{
    ArrayAttrAst, ArrayCellOrderAst, ArrayDimAst, ArrayDimType, ArrayDomainBound, ArrayGeorefAst,
    ArrayTileOrderAst,
};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::array_catalog::attr_meta::meta_from_ast;
use crate::control::array_catalog::georef::georef_from_ast;
use crate::control::array_catalog::schema_recode::schema_error;
use crate::types::TenantId;
use nodedb_physical::physical_plan::ArrayOp;

use super::super::convert::ConvertContext;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// All inputs for `CREATE ARRAY` lowering, bundled to stay under
/// the 7-parameter clippy limit.
pub(in super::super) struct CreateArrayArgs<'a> {
    pub name: &'a str,
    pub dims: &'a [ArrayDimAst],
    pub attrs: &'a [ArrayAttrAst],
    pub tile_extents: &'a [i64],
    pub cell_order: ArrayCellOrderAst,
    pub tile_order: ArrayTileOrderAst,
    pub georef: Option<&'a ArrayGeorefAst>,
    pub prefix_bits: u8,
    pub audit_retain_ms: Option<u64>,
    pub minimum_audit_retain_ms: Option<u64>,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

pub(in super::super) fn convert_create_array(
    args: CreateArrayArgs<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let CreateArrayArgs {
        name,
        dims,
        attrs,
        tile_extents,
        cell_order,
        tile_order,
        georef,
        prefix_bits,
        audit_retain_ms,
        minimum_audit_retain_ms,
        tenant_id,
        ctx,
    } = args;
    ctx.require_execute("CREATE ARRAY")?;
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "CREATE ARRAY: no array catalog wired into convert context".into(),
        })?;
    // 1a. Validate retention policy before touching shared state.
    if audit_retain_ms.is_some() || minimum_audit_retain_ms.is_some() {
        let retention = BitemporalRetention {
            data_retain_ms: 0,
            audit_retain_ms: audit_retain_ms.unwrap_or(0),
            minimum_audit_retain_ms: minimum_audit_retain_ms.unwrap_or(0),
        };
        retention.validate().map_err(|e| crate::Error::PlanError {
            detail: format!("CREATE ARRAY {name}: {e}"),
        })?;
    }

    // 1. Build typed schema.
    let schema = build_schema(SchemaParts {
        name,
        dims,
        attrs,
        tile_extents,
        cell_order,
        tile_order,
        georef,
        crs: &ctx.crs_context(),
    })?;

    // 2. Encode + hash.
    let schema_msgpack =
        zerompk::to_msgpack_vec(&*schema).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema encode: {e}"),
        })?;
    let content = schema
        .content_msgpack()
        .map_err(|e| schema_error(&format!("CREATE ARRAY {name}"), e))?;
    let schema_hash = stable_schema_hash(&content);

    // Retention is carried as `u64` from the parser and stored signed, because
    // downstream it is subtracted from a wall-clock millisecond
    // (`now_ms - audit_retain_ms`) to place the purge horizon. Both parsers
    // reject a negative, so today every value arrives inside `i64`; this is
    // checked rather than cast so that a future entry point which skips that
    // validation fails here instead of wrapping to a negative retention, which
    // will put the horizon in the future and purge every version the array has.
    let audit_retain_ms = audit_retain_ms
        .map(i64::try_from)
        .transpose()
        .map_err(|_| crate::Error::PlanError {
            detail: format!(
                "CREATE ARRAY {name}: audit_retain_ms exceeds the maximum supported \
                 retention of {} ms",
                i64::MAX
            ),
        })?;

    // 3. Build DDL metadata only. The catalog changes through the replicated
    // `PutArray` entry the front door proposes, never while converting.
    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let entry = ArrayCatalogEntry {
        array_id: aid.clone(),
        name: name.to_string(),
        schema_msgpack: schema_msgpack.clone(),
        schema_hash,
        created_at_ms: now_epoch_ms(),
        prefix_bits,
        audit_retain_ms,
        minimum_audit_retain_ms,
        modification_hlc: nodedb_types::Hlc::ZERO,
        incarnation: nodedb_types::Hlc::ZERO,
    };
    {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        if cat.lookup_by_id(&aid).is_some() {
            return Err(crate::Error::PlanError {
                detail: format!("CREATE ARRAY {name}: already exists"),
            });
        }
    }

    // 4. Emit OpenArray. The front door hands it to the replicated array
    // catalog (`array_catalog::ddl`), which opens it on every node.
    let vshard = ctx.collection_key(name).vshard();
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Array(ArrayOp::OpenArray {
            array_id: aid,
            schema_msgpack,
            schema_hash,
            prefix_bits,
            audit_retain_ms: entry.audit_retain_ms,
            minimum_audit_retain_ms: entry.minimum_audit_retain_ms,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(in super::super) fn convert_drop_array(
    name: &str,
    if_exists: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    ctx.require_execute("DROP ARRAY")?;
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "DROP ARRAY: no array catalog wired into convert context".into(),
        })?;
    let removed_array_id: Option<ArrayId> = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name_in_database(tenant_id, ctx.database_id, name)
            .map(|entry| entry.array_id)
    };
    let Some(aid) = removed_array_id else {
        if if_exists {
            return Ok(Vec::new());
        }
        return Err(crate::Error::PlanError {
            detail: format!("DROP ARRAY {name}: not found"),
        });
    };
    let vshard = ctx.collection_key(name).vshard();
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Array(ArrayOp::DropArray { array_id: aid }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

// ── Schema construction helpers ──────────────────────────────────────

/// The SQL parts of an array schema.
pub(super) struct SchemaParts<'a> {
    pub name: &'a str,
    pub dims: &'a [ArrayDimAst],
    pub attrs: &'a [ArrayAttrAst],
    pub tile_extents: &'a [i64],
    pub cell_order: ArrayCellOrderAst,
    pub tile_order: ArrayTileOrderAst,
    pub georef: Option<&'a ArrayGeorefAst>,
    pub crs: &'a nodedb_crs::CrsContext<'a>,
}

pub(super) fn build_schema(parts: SchemaParts<'_>) -> crate::Result<Arc<ArraySchema>> {
    let SchemaParts {
        name,
        dims,
        attrs,
        tile_extents,
        cell_order,
        tile_order,
        georef,
        crs,
    } = parts;
    let engine_dims: Vec<DimSpec> = dims
        .iter()
        .map(|d| {
            let dtype = match d.dtype {
                ArrayDimType::Int64 => EngineDimType::Int64,
                ArrayDimType::Float64 => EngineDimType::Float64,
                ArrayDimType::TimestampMs => EngineDimType::TimestampMs,
                ArrayDimType::String => EngineDimType::String,
            };
            let lo = bound_to_engine(&d.lo);
            let hi = bound_to_engine(&d.hi);
            DimSpec::new(d.name.clone(), dtype, Domain::new(lo, hi))
        })
        .collect();
    let mut builder = ArraySchemaBuilder::new(name);
    if let Some(georef) = georef {
        builder = builder.georef(georef_from_ast(name, georef, &engine_dims, crs)?);
    }
    for d in engine_dims {
        builder = builder.dim(d);
    }
    for a in attrs {
        let dtype = super::attr_type::engine_attr_type(a.dtype);
        builder = builder.attr(
            AttrSpec::new(a.name.clone(), dtype, a.nullable).with_meta(meta_from_ast(&a.meta)),
        );
    }
    let extents: Vec<u64> = tile_extents.iter().map(|n| *n as u64).collect();
    builder = builder
        .tile_extents(extents)
        .cell_order(map_cell_order(cell_order))
        .tile_order(map_tile_order(tile_order));
    let schema = builder
        .build()
        .map_err(|e| schema_error(&format!("CREATE ARRAY {name}"), e))?;
    Ok(Arc::new(schema))
}

fn bound_to_engine(b: &ArrayDomainBound) -> DomainBound {
    match b {
        ArrayDomainBound::Int64(v) => DomainBound::Int64(*v),
        ArrayDomainBound::Float64(v) => DomainBound::Float64(*v),
        ArrayDomainBound::TimestampMs(v) => DomainBound::TimestampMs(*v),
        ArrayDomainBound::String(v) => DomainBound::String(v.clone()),
    }
}

fn map_cell_order(o: ArrayCellOrderAst) -> CellOrder {
    match o {
        ArrayCellOrderAst::RowMajor => CellOrder::RowMajor,
        ArrayCellOrderAst::ColMajor => CellOrder::ColMajor,
        ArrayCellOrderAst::Hilbert => CellOrder::Hilbert,
        ArrayCellOrderAst::ZOrder => CellOrder::ZOrder,
    }
}

fn map_tile_order(o: ArrayTileOrderAst) -> TileOrder {
    match o {
        ArrayTileOrderAst::RowMajor => TileOrder::RowMajor,
        ArrayTileOrderAst::ColMajor => TileOrder::ColMajor,
        ArrayTileOrderAst::Hilbert => TileOrder::Hilbert,
        ArrayTileOrderAst::ZOrder => TileOrder::ZOrder,
    }
}

/// Deterministic schema hash: CRC32C of msgpack bytes split into two halves, OR'd into u64.
pub(super) fn stable_schema_hash(bytes: &[u8]) -> u64 {
    if bytes.len() <= 4 {
        return crc32c::crc32c(bytes) as u64;
    }
    let mid = bytes.len() / 2;
    let lo = crc32c::crc32c(&bytes[..mid]) as u64;
    let hi = crc32c::crc32c(&bytes[mid..]) as u64;
    (hi << 32) | lo
}

pub(super) fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
