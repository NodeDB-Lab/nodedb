//! Shared test fixtures for `ArrayEngine` unit tests across the
//! split write/flush/compact/read modules.

#![cfg(test)]

use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_types::TenantId;

use super::engine::ArrayEngine;
use super::wal::ArrayPutCell;

pub(super) fn schema() -> Arc<ArraySchema> {
    Arc::new(
        ArraySchemaBuilder::new("a")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4, 4])
            .build()
            .unwrap(),
    )
}

pub(super) fn aid() -> ArrayId {
    ArrayId::new(TenantId::new(1), "g")
}

pub(super) fn put_one(e: &mut ArrayEngine, x: i64, y: i64, v: i64, lsn: u64) {
    e.put_cells(
        &aid(),
        vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
            attrs: vec![CellValue::Int64(v)],
            surrogate: nodedb_types::Surrogate::ZERO,
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }],
        lsn,
    )
    .unwrap();
}
