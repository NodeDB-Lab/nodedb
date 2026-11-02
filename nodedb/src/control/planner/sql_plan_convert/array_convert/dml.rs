// SPDX-License-Identifier: BUSL-1.1

//! `INSERT INTO ARRAY` / `DELETE FROM ARRAY` lowering to `PhysicalTask`.

use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::TilePrefixMemo;
use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_sql::types_array::{ArrayCoordLiteral, ArrayInsertRow};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};
use crate::types::TenantId;
use nodedb_physical::physical_plan::ClusterArrayOp;
use nodedb_types::Surrogate;

use super::super::convert::ConvertContext;
use super::helpers::{coerce_attrs, coerce_coords};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// The catalog entry and decoded schema of the array `name` an
/// `INSERT INTO ARRAY` writes.
fn insert_target(
    name: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<(ArrayCatalogEntry, ArraySchema)> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "INSERT INTO ARRAY: no array catalog wired into convert context".into(),
        })?;
    let entry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name_in_database(tenant_id, ctx.database_id, name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("INSERT INTO ARRAY {name}: not found"),
            })?
    };
    let schema: ArraySchema =
        zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema decode: {e}"),
        })?;
    Ok((entry, schema))
}

/// The coerced coordinate and identity key of every cell an
/// `INSERT INTO ARRAY` writes, in row order. The bind step reads them once,
/// and conversion reuses them.
#[derive(Debug)]
pub(in super::super) struct ArrayCellKeys {
    pub coords: Vec<Vec<CoordValue>>,
    /// Each coordinate, msgpack-encoded: the cell's surrogate key.
    pub pks: Vec<Vec<u8>>,
    /// Each cell's bound surrogate, in row order, when the bind step bound
    /// the batch. Empty otherwise: conversion then resolves each key.
    pub surrogates: Vec<Surrogate>,
}

/// The [`ArrayCellKeys`] of the rows an `INSERT INTO ARRAY` writes.
pub(in super::super) fn insert_array_cell_keys(
    name: &str,
    rows: &[ArrayInsertRow],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<ArrayCellKeys> {
    let (_, schema) = insert_target(name, tenant_id, ctx)?;
    let mut coords = Vec::with_capacity(rows.len());
    let mut pks = Vec::with_capacity(rows.len());
    for row in rows {
        let coord = coerce_coords(&row.coords, &schema)?;
        pks.push(coord_pk(&coord)?);
        coords.push(coord);
    }
    Ok(ArrayCellKeys {
        coords,
        pks,
        surrogates: Vec::new(),
    })
}

/// Row `index`'s coerced coordinate and surrogate: the bind step's when it
/// kept them, else coerced here and resolved through `ctx`.
fn cell_identity(
    kept: Option<&ArrayCellKeys>,
    index: usize,
    row: &ArrayInsertRow,
    schema: &ArraySchema,
    key: nodedb_types::CollectionKey<'_>,
    ctx: &ConvertContext,
) -> crate::Result<(Vec<CoordValue>, Surrogate)> {
    if let Some(keys) = kept
        && let (Some(coord), Some(pk)) = (keys.coords.get(index), keys.pks.get(index))
    {
        let surrogate = match keys.surrogates.get(index) {
            Some(&bound) => bound,
            None => ctx.surrogate_for_pk(key, pk)?,
        };
        return Ok((coord.clone(), surrogate));
    }
    let coord = coerce_coords(&row.coords, schema)?;
    let surrogate = ctx.surrogate_for_pk(key, &coord_pk(&coord)?)?;
    Ok((coord, surrogate))
}

pub(in super::super) fn convert_insert_array(
    name: &str,
    rows: &[ArrayInsertRow],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let (entry, schema) = insert_target(name, tenant_id, ctx)?;

    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let key = ctx.collection_key(name);
    let vshard = key.vshard();
    // A cell's valid time opens at the statement's wall-clock instant. Its
    // system time is the ordinal the owning core's apply stamps: the plan
    // carries none (`ArrayPutCell::system_from_ms` stays 0).
    let valid_now_ms = chrono::Utc::now().timestamp_millis();
    let kept = ctx.prefetched.array_keys(key, rows);

    let mut partitioned: Vec<(u64, Vec<u8>)> = Vec::with_capacity(rows.len());
    let mut tiles = TilePrefixMemo::new();
    for (index, row) in rows.iter().enumerate() {
        let (coord, surrogate) = cell_identity(kept, index, row, &schema, key, ctx)?;
        let attrs = coerce_attrs(&row.attrs, &schema)?;
        // A cell routes by its tile's key, the key a slice's shard
        // fan-out and each shard's tile filter use.
        let tile_key = tiles
            .cell_prefix(&schema, &coord)
            .map_err(|e| crate::Error::PlanError {
                detail: format!("INSERT INTO ARRAY {name}: tile key: {e}"),
            })?;
        let cell = ArrayPutCell {
            coord,
            attrs,
            surrogate,
            system_from_ms: 0,
            valid_from_ms: valid_now_ms,
            valid_until_ms: i64::MAX,
        };
        let cell_bytes =
            zerompk::to_msgpack_vec(&cell).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array put cell encode: {e}"),
            })?;
        partitioned.push((tile_key, cell_bytes));
    }
    let array_id_msgpack =
        zerompk::to_msgpack_vec(&aid).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array id encode: {e}"),
        })?;
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::ClusterArray(ClusterArrayOp::Put {
            array_id: aid,
            array_id_msgpack,
            cells: partitioned,
            // A routing wrapper carries no LSN of its own: the coordinator
            // fans these cells out to the owning shards and each owner's
            // apply mints the redo — and the LSN — for the cells it holds.
            wal_lsn: 0,
            prefix_bits: entry.prefix_bits,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(in super::super) fn convert_delete_array(
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
    let entry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name_in_database(tenant_id, ctx.database_id, name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("DELETE FROM ARRAY {name}: not found"),
            })?
    };
    let schema: ArraySchema =
        zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema decode: {e}"),
        })?;

    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let vshard = ctx.collection_key(name).vshard();
    // The tombstone's system time is the ordinal the owning core's apply
    // stamps: the plan carries none (`ArrayDeleteCell::system_from_ms`
    // stays 0).

    let mut partitioned: Vec<(u64, Vec<u8>)> = Vec::with_capacity(coords.len());
    let mut tiles = TilePrefixMemo::new();
    for row in coords {
        let typed = coerce_coords(row, &schema)?;
        let tile_key = tiles
            .cell_prefix(&schema, &typed)
            .map_err(|e| crate::Error::PlanError {
                detail: format!("DELETE FROM ARRAY {name}: tile key: {e}"),
            })?;
        let cell = ArrayDeleteCell {
            coord: typed,
            system_from_ms: 0,
            erasure: false,
        };
        let cell_bytes =
            zerompk::to_msgpack_vec(&cell).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array delete cell encode: {e}"),
            })?;
        partitioned.push((tile_key, cell_bytes));
    }
    let array_id_msgpack =
        zerompk::to_msgpack_vec(&aid).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array id encode: {e}"),
        })?;
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::ClusterArray(ClusterArrayOp::Delete {
            array_id: aid,
            array_id_msgpack,
            coords: partitioned,
            // See `convert_insert_array`: the owning shard's apply mints the
            // redo, so the routing wrapper carries no LSN.
            wal_lsn: 0,
            prefix_bits: entry.prefix_bits,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// The identity key of an array cell: its coerced coordinate, msgpack-encoded.
fn coord_pk(coord: &[CoordValue]) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&coord).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("array coord pk encode: {e}"),
    })
}
