// SPDX-License-Identifier: BUSL-1.1
//! 3-node cluster test for `ARRAY_SLICE` with a WGS 84 zone over a
//! georeferenced array.
//!
//! The test matches `binary(/cluster/)` in nextest.toml, so it runs in the
//! serial `cluster` test group. The coordinator forwards the zone to every
//! shard it fans out to. Every node must answer the same row set, and that
//! set must equal the cells the engine's own zone test admits.

use std::collections::BTreeSet;

use nodedb_array::query::region::{PolygonZone, ZonePolygon};
use nodedb_array::query::zone_frame::build_zone_frame;
use nodedb_array::schema::{
    ArrayGeoref, ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType, GeoTransform,
};
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_types::geometry::Srid;

use crate::common;

use common::cluster_harness::TestCluster;

const SIZE: i64 = 24;
const GT: [f64; 6] = [550_000.0, 30.0, 0.0, 664_000.0, 0.0, -30.0];

/// A WGS 84 quadrilateral over the middle of the grid.
const RING: [[f64; 2]; 5] = [
    [99.4535, 6.0060],
    [99.4580, 6.0056],
    [99.4575, 6.0022],
    [99.4540, 6.0026],
    [99.4535, 6.0060],
];

fn ewkt() -> String {
    let points: Vec<String> = RING
        .iter()
        .map(|p| format!("{:?} {:?}", p[0], p[1]))
        .collect();
    format!("'SRID=4326;POLYGON(({}))'", points.join(", "))
}

/// The cells the engine's zone test admits, from the same schema and ring.
fn expected_cells() -> BTreeSet<(i64, i64)> {
    let dim = |name: &str| {
        DimSpec::new(
            name,
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(SIZE - 1)),
        )
    };
    let schema = ArraySchemaBuilder::new("czone")
        .dim(dim("row"))
        .dim(dim("col"))
        .attr(AttrSpec::new("b1", AttrType::Float64, true))
        .tile_extents(vec![4, 4])
        .build()
        .expect("schema")
        .with_georef(Some(ArrayGeoref {
            srid: Srid::new(32647).unwrap(),
            x_dim: 1,
            y_dim: 0,
            transform: Some(GeoTransform(GT)),
        }))
        .expect("georef");
    let polygons = vec![ZonePolygon {
        exterior: RING.to_vec(),
        holes: Vec::new(),
    }];
    let frame = build_zone_frame(
        &schema,
        Srid::WGS84,
        &polygons,
        &nodedb_crs::CrsContext::builtin_only(),
    )
    .expect("frame");
    let zone = PolygonZone {
        x_dim: 1,
        y_dim: 0,
        polygons,
        frame: Some(frame),
    };
    let prepared = zone.prepare();
    let mut out = BTreeSet::new();
    for row in 0..SIZE {
        for col in 0..SIZE {
            if prepared.contains(&[CoordValue::Int64(row), CoordValue::Int64(col)]) {
                out.insert((row, col));
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_slice_zone_answers_one_row_set_on_every_node() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("3-node cluster spawn");
    let ddl_idx = cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE ARRAY czone DIMS (row INT64 [0..{last}], col INT64 [0..{last}]) \
             ATTRS (b1 FLOAT64) TILE_EXTENTS (4, 4) \
             GEOREF (SRID 32647, X col, Y row, TRANSFORM ({}, {}, {}, {}, {}, {}))",
            GT[0],
            GT[1],
            GT[2],
            GT[3],
            GT[4],
            GT[5],
            last = SIZE - 1
        ))
        .await
        .expect("CREATE ARRAY");
    let writer = &cluster.nodes[(ddl_idx + 1) % cluster.nodes.len()];
    for row in 0..SIZE {
        let cells: Vec<String> = (0..SIZE)
            .map(|col| format!("COORDS ({row}, {col}) VALUES ({}.0)", row * SIZE + col))
            .collect();
        writer
            .exec(&format!("INSERT INTO ARRAY czone {}", cells.join(", ")))
            .await
            .expect("INSERT INTO ARRAY");
    }
    writer
        .exec("SELECT ARRAY_FLUSH('czone')")
        .await
        .expect("ARRAY_FLUSH");
    cluster
        .wait_for_full_apply_convergence(std::time::Duration::from_secs(10))
        .await;

    let expected = expected_cells();
    assert!(
        expected.len() > 10,
        "the zone holds cells: {}",
        expected.len()
    );
    let sql = format!(
        "SELECT * FROM ARRAY_SLICE('czone', '{{row: [0, {}]}}', '*', 0, {}, NULL)",
        SIZE - 1,
        ewkt()
    );
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = node
            .query_named_rows(&sql)
            .await
            .unwrap_or_else(|e| panic!("node {idx}: {sql}: {e}"));
        let got: BTreeSet<(i64, i64)> = rows
            .iter()
            .map(|r| {
                let num = |c: &str| {
                    r[c].parse::<f64>()
                        .unwrap_or_else(|e| panic!("node {idx}: {c} in {r:?}: {e}"))
                        as i64
                };
                (num("row"), num("col"))
            })
            .collect();
        assert_eq!(rows.len(), got.len(), "node {idx}: no duplicate rows");
        assert_eq!(got, expected, "node {idx}: the zone's cells");
    }
    cluster.shutdown().await;
}
