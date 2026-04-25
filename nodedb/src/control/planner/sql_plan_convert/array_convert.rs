//! `SqlPlan::{CreateArray, DropArray, InsertArray, DeleteArray}`
//! → `PhysicalTask` lowering.
//!
//! This is the Control-Plane DDL persistence path 1c-a deferred (the
//! Data-Plane handler updates the in-memory `ArrayCatalog` only and
//! must not touch redb). `CreateArray` here:
//!
//! 1. Builds the typed `nodedb_array::ArraySchema` from the engine-
//!    agnostic AST,
//! 2. zerompk-encodes it,
//! 3. Computes a deterministic schema hash via CRC64-of-msgpack,
//! 4. Persists to `_system.arrays` and the in-memory `ArrayCatalog`,
//! 5. Emits `PhysicalPlan::Array(ArrayOp::OpenArray { … })` so the
//!    Data Plane verifies the schema hash and opens the engine side.
//!
//! `DropArray` does the inverse on the catalog and emits no physical
//! task — per-core array store cleanup happens lazily on next access.
//!
//! `InsertArray` / `DeleteArray` resolve the array's typed schema from
//! the catalog, coerce literals to `CoordValue` / `CellValue`, allocate
//! a `wal_lsn` from the central WAL allocator, and emit
//! `ArrayOp::Put` / `ArrayOp::Delete`.

use std::sync::Arc;

use nodedb_array::schema::{
    ArraySchema, ArraySchemaBuilder, AttrSpec, AttrType as EngineAttrType, CellOrder, DimSpec,
    DimType as EngineDimType, TileOrder,
};
use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_sql::types_array::{
    ArrayAttrAst, ArrayAttrLiteral, ArrayAttrType, ArrayCellOrderAst, ArrayCoordLiteral,
    ArrayDimAst, ArrayDimType, ArrayDomainBound, ArrayInsertRow, ArrayTileOrderAst,
};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::ArrayOp;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::engine::array::wal::ArrayPutCell;
use crate::types::{TenantId, VShardId};

use super::super::physical::{PhysicalTask, PostSetOp};
use super::convert::ConvertContext;

#[allow(clippy::too_many_arguments)]
pub(super) fn convert_create_array(
    name: &str,
    dims: &[ArrayDimAst],
    attrs: &[ArrayAttrAst],
    tile_extents: &[i64],
    cell_order: ArrayCellOrderAst,
    tile_order: ArrayTileOrderAst,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "CREATE ARRAY: no array catalog wired into convert context".into(),
        })?;
    let credentials = ctx
        .credentials
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "CREATE ARRAY: no credential store wired into convert context".into(),
        })?;

    // 1. Build typed schema.
    let schema = build_schema(name, dims, attrs, tile_extents, cell_order, tile_order)?;

    // 2. Encode + hash.
    //
    // The full schema (including `name`) goes on the wire, but the hash
    // is computed over the *content* fields only — see
    // `ArraySchema::content_msgpack()`. That way two structurally-
    // identical arrays with different names share a `schema_hash` and
    // can participate in cross-array element-wise ops.
    let schema_msgpack =
        zerompk::to_msgpack_vec(&*schema).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema encode: {e}"),
        })?;
    let schema_hash = stable_schema_hash(&schema.content_msgpack());

    // 3. Persist + register. Reject duplicates with a typed error.
    let aid = ArrayId::new(tenant_id, name);
    let entry = ArrayCatalogEntry {
        array_id: aid.clone(),
        name: name.to_string(),
        schema_msgpack: schema_msgpack.clone(),
        schema_hash,
        created_at_ms: now_epoch_ms(),
    };
    {
        let mut cat = array_catalog.write().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        if cat.lookup_by_name(name).is_some() {
            return Err(crate::Error::PlanError {
                detail: format!("CREATE ARRAY {name}: already exists"),
            });
        }
        cat.register(entry.clone())
            .map_err(|e| crate::Error::PlanError {
                detail: format!("array catalog register: {e}"),
            })?;
    }
    if let Some(catalog) = credentials.catalog().as_ref() {
        crate::control::array_catalog::persist::persist(catalog, &entry).map_err(|e| {
            crate::Error::PlanError {
                detail: format!("array catalog persist: {e}"),
            }
        })?;
    }

    // 4. Emit OpenArray so the Data Plane opens the engine side.
    let vshard = VShardId::from_collection(name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::OpenArray {
            array_id: aid,
            schema_msgpack,
            schema_hash,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_drop_array(
    name: &str,
    if_exists: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "DROP ARRAY: no array catalog wired into convert context".into(),
        })?;
    let credentials = ctx
        .credentials
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "DROP ARRAY: no credential store wired into convert context".into(),
        })?;
    // Capture the entry's `array_id` *before* removal so we can scatter the
    // `DropArray` op to every core with the same id the catalog used.
    let removed_array_id: Option<ArrayId> = {
        let mut cat = array_catalog.write().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        let aid = cat.lookup_by_name(name).map(|e| e.array_id.clone());
        if aid.is_some() {
            cat.unregister(name);
        }
        aid
    };
    if removed_array_id.is_none() && !if_exists {
        return Err(crate::Error::PlanError {
            detail: format!("DROP ARRAY {name}: not found"),
        });
    }
    if let Some(catalog) = credentials.catalog().as_ref() {
        if let Err(e) = crate::control::array_catalog::persist::remove(catalog, name) {
            return Err(crate::Error::PlanError {
                detail: format!("array catalog remove: {e}"),
            });
        }
        // Wipe the surrogate ↔ PK map for this array. Surrogates are
        // collection/array-scoped; leaving them behind on drop would
        // just be allocator-bloat (and would break any later
        // CREATE-with-same-name observability assertion).
        if let Err(e) = catalog.delete_all_surrogates_for_collection(name) {
            return Err(crate::Error::PlanError {
                detail: format!("array surrogate-map cleanup: {e}"),
            });
        }
    }
    // Broadcast `ArrayOp::DropArray` to every core so each core releases
    // its per-core store and removes the on-disk segment dir. The
    // dispatcher recognizes `ArrayOp::DropArray` and fans out via
    // `broadcast_count_to_all_cores`. If the array wasn't registered
    // (IF EXISTS path), we have nothing to scatter.
    let Some(aid) = removed_array_id else {
        return Ok(Vec::new());
    };
    let vshard = VShardId::from_collection(name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::DropArray { array_id: aid }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_insert_array(
    name: &str,
    rows: &[ArrayInsertRow],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "INSERT INTO ARRAY: no array catalog wired into convert context".into(),
        })?;
    let wal = ctx.wal.as_ref().ok_or_else(|| crate::Error::PlanError {
        detail: "INSERT INTO ARRAY: no WAL wired into convert context".into(),
    })?;

    let entry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name(name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("INSERT INTO ARRAY {name}: not found"),
            })?
    };
    let schema: ArraySchema =
        zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema decode: {e}"),
        })?;

    // Coerce every row to typed engine cells.
    let mut cells: Vec<ArrayPutCell> = Vec::with_capacity(rows.len());
    for row in rows {
        let coord = coerce_coords(&row.coords, &schema)?;
        let attrs = coerce_attrs(&row.attrs, &schema)?;
        cells.push(ArrayPutCell { coord, attrs });
    }
    let cells_msgpack =
        zerompk::to_msgpack_vec(&cells).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array put cells encode: {e}"),
        })?;

    let aid = ArrayId::new(tenant_id, name);
    let wal_lsn = wal.next_lsn().as_u64();
    let vshard = VShardId::from_collection(name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Put {
            array_id: aid,
            cells_msgpack,
            wal_lsn,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_delete_array(
    name: &str,
    coords: &[Vec<ArrayCoordLiteral>],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "DELETE FROM ARRAY: no array catalog wired into convert context".into(),
        })?;
    let wal = ctx.wal.as_ref().ok_or_else(|| crate::Error::PlanError {
        detail: "DELETE FROM ARRAY: no WAL wired into convert context".into(),
    })?;
    let entry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name(name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("DELETE FROM ARRAY {name}: not found"),
            })?
    };
    let schema: ArraySchema =
        zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema decode: {e}"),
        })?;

    let mut typed: Vec<Vec<CoordValue>> = Vec::with_capacity(coords.len());
    for row in coords {
        typed.push(coerce_coords(row, &schema)?);
    }
    let coords_msgpack =
        zerompk::to_msgpack_vec(&typed).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array delete coords encode: {e}"),
        })?;
    let aid = ArrayId::new(tenant_id, name);
    let wal_lsn = wal.next_lsn().as_u64();
    let vshard = VShardId::from_collection(name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Delete {
            array_id: aid,
            coords_msgpack,
            wal_lsn,
        }),
        post_set_op: PostSetOp::None,
    }])
}

// ── Helpers ─────────────────────────────────────────────────────────

fn build_schema(
    name: &str,
    dims: &[ArrayDimAst],
    attrs: &[ArrayAttrAst],
    tile_extents: &[i64],
    cell_order: ArrayCellOrderAst,
    tile_order: ArrayTileOrderAst,
) -> crate::Result<Arc<ArraySchema>> {
    let mut builder = ArraySchemaBuilder::new(name);
    for d in dims {
        let dtype = match d.dtype {
            ArrayDimType::Int64 => EngineDimType::Int64,
            ArrayDimType::Float64 => EngineDimType::Float64,
            ArrayDimType::TimestampMs => EngineDimType::TimestampMs,
            ArrayDimType::String => EngineDimType::String,
        };
        let lo = bound_to_engine(&d.lo);
        let hi = bound_to_engine(&d.hi);
        builder = builder.dim(DimSpec::new(d.name.clone(), dtype, Domain::new(lo, hi)));
    }
    for a in attrs {
        let dtype = match a.dtype {
            ArrayAttrType::Int64 => EngineAttrType::Int64,
            ArrayAttrType::Float64 => EngineAttrType::Float64,
            ArrayAttrType::String => EngineAttrType::String,
            ArrayAttrType::Bytes => EngineAttrType::Bytes,
        };
        builder = builder.attr(AttrSpec::new(a.name.clone(), dtype, a.nullable));
    }
    let extents: Vec<u64> = tile_extents.iter().map(|n| *n as u64).collect();
    builder = builder
        .tile_extents(extents)
        .cell_order(map_cell_order(cell_order))
        .tile_order(map_tile_order(tile_order));
    let schema = builder.build().map_err(|e| crate::Error::PlanError {
        detail: format!("CREATE ARRAY {name}: {e}"),
    })?;
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

fn coerce_coords(
    coords: &[ArrayCoordLiteral],
    schema: &ArraySchema,
) -> crate::Result<Vec<CoordValue>> {
    if coords.len() != schema.dims.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "coord arity {} does not match dim count {}",
                coords.len(),
                schema.dims.len()
            ),
        });
    }
    let mut out = Vec::with_capacity(coords.len());
    for (i, c) in coords.iter().enumerate() {
        let dim = &schema.dims[i];
        let v = match (c, dim.dtype) {
            (ArrayCoordLiteral::Int64(n), EngineDimType::Int64) => CoordValue::Int64(*n),
            (ArrayCoordLiteral::Int64(n), EngineDimType::TimestampMs) => {
                CoordValue::TimestampMs(*n)
            }
            (ArrayCoordLiteral::Int64(n), EngineDimType::Float64) => CoordValue::Float64(*n as f64),
            (ArrayCoordLiteral::Float64(f), EngineDimType::Float64) => CoordValue::Float64(*f),
            (ArrayCoordLiteral::String(s), EngineDimType::String) => CoordValue::String(s.clone()),
            (got, want) => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "coord literal for dim `{}`: got {got:?}, expected dim type {want:?}",
                        dim.name
                    ),
                });
            }
        };
        out.push(v);
    }
    Ok(out)
}

fn coerce_attrs(attrs: &[ArrayAttrLiteral], schema: &ArraySchema) -> crate::Result<Vec<CellValue>> {
    if attrs.len() != schema.attrs.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "attr arity {} does not match attr count {}",
                attrs.len(),
                schema.attrs.len()
            ),
        });
    }
    let mut out = Vec::with_capacity(attrs.len());
    for (i, a) in attrs.iter().enumerate() {
        let spec = &schema.attrs[i];
        let v = match (a, spec.dtype) {
            (ArrayAttrLiteral::Null, _) if spec.nullable => CellValue::Null,
            (ArrayAttrLiteral::Null, _) => {
                return Err(crate::Error::PlanError {
                    detail: format!("attr `{}` is NOT NULL", spec.name),
                });
            }
            (ArrayAttrLiteral::Int64(n), EngineAttrType::Int64) => CellValue::Int64(*n),
            (ArrayAttrLiteral::Int64(n), EngineAttrType::Float64) => CellValue::Float64(*n as f64),
            (ArrayAttrLiteral::Float64(f), EngineAttrType::Float64) => CellValue::Float64(*f),
            (ArrayAttrLiteral::String(s), EngineAttrType::String) => CellValue::String(s.clone()),
            (ArrayAttrLiteral::Bytes(b), EngineAttrType::Bytes) => CellValue::Bytes(b.clone()),
            (got, want) => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "attr literal for `{}`: got {got:?}, expected attr type {want:?}",
                        spec.name
                    ),
                });
            }
        };
        out.push(v);
    }
    Ok(out)
}

/// Deterministic schema hash. CRC32C of the msgpack bytes split into
/// two halves and OR'd into a u64 — same algorithm used elsewhere in
/// the workspace for content-addressed schema digests, deterministic
/// across runs because it depends only on the encoded bytes.
fn stable_schema_hash(bytes: &[u8]) -> u64 {
    if bytes.len() <= 4 {
        return crc32c::crc32c(bytes) as u64;
    }
    let mid = bytes.len() / 2;
    let lo = crc32c::crc32c(&bytes[..mid]) as u64;
    let hi = crc32c::crc32c(&bytes[mid..]) as u64;
    (hi << 32) | lo
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
