//! Shared helpers for array CRDT sync integration tests.

use std::sync::Arc;

use nodedb::control::array_catalog::entry::ArrayCatalogEntry;
use nodedb::control::state::SharedState;
use nodedb_array::schema::array_schema::ArraySchema;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::cell_order::{CellOrder, TileOrder};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::sync::HlcGenerator;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::replica_id::ReplicaId;
use nodedb_array::sync::schema_crdt::SchemaDoc;
use nodedb_array::types::ArrayId;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_types::TenantId;

#[allow(dead_code)]
pub fn rep(id: u64) -> ReplicaId {
    ReplicaId::new(id)
}

#[allow(dead_code)]
pub fn hlc(ms: u64, rid: u64) -> Hlc {
    Hlc::new(ms, 0, rep(rid)).expect("valid HLC")
}

/// One-dimensional Int64 schema over [0, 99] with attribute "v" (Float64).
#[allow(dead_code)]
pub fn simple_schema(name: &str) -> ArraySchema {
    ArraySchema {
        name: name.into(),
        dims: vec![DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
        )],
        attrs: vec![AttrSpec::new("v", AttrType::Float64, true)],
        tile_extents: vec![10],
        cell_order: CellOrder::RowMajor,
        tile_order: TileOrder::RowMajor,
    }
}

/// Build a real Loro snapshot for `array_name` and return
/// `(snapshot_bytes, schema_hlc)` suitable for `import_snapshot` calls.
#[allow(dead_code)]
pub fn build_schema_snapshot(array_name: &str) -> (Vec<u8>, Hlc) {
    let hlc_gen = HlcGenerator::new(rep(1));
    let schema = simple_schema(array_name);
    let doc = SchemaDoc::from_schema(rep(1), &schema, &hlc_gen).expect("from_schema");
    let bytes = doc.export_snapshot().expect("export_snapshot");
    (bytes, doc.schema_hlc())
}

/// Import a pre-built snapshot on `shared`. Use this with a single
/// `(bytes, schema_hlc)` tuple shared across all nodes so every replica
/// observes the same remote HLC.
#[allow(dead_code)]
pub fn import_schema_snapshot(
    shared: &Arc<SharedState>,
    array_name: &str,
    bytes: &[u8],
    schema_hlc: Hlc,
) {
    shared
        .array_sync_schemas
        .import_snapshot(array_name, bytes, schema_hlc)
        .expect("import_snapshot");
}

/// Register an array catalog entry on `shared` so the Data Plane can open
/// the array when applying ops. Independent of schema-CRDT registration.
#[allow(dead_code)]
pub fn register_catalog_entry(shared: &Arc<SharedState>, array_name: &str) {
    let schema = simple_schema(array_name);
    let schema_msgpack = zerompk::to_msgpack_vec(&schema).expect("encode schema");
    let entry = ArrayCatalogEntry {
        array_id: ArrayId::new(TenantId::new(0), array_name),
        name: array_name.to_string(),
        schema_msgpack,
        schema_hash: 0,
        created_at_ms: 0,
        prefix_bits: 8,
        audit_retain_ms: None,
        minimum_audit_retain_ms: None,
    };
    let mut cat = shared.array_catalog.write().expect("array catalog lock");
    if cat.lookup_by_name(array_name).is_none() {
        cat.register(entry).expect("register catalog entry");
    }
}

/// Build one snapshot and import it on every node, returning the shared
/// schema HLC. Use this HLC when stamping ops so the schema-gating check
/// in `OriginArrayInbound` accepts them on every replica.
#[allow(dead_code)]
pub fn register_schema_on_all(shareds: &[&Arc<SharedState>], array_name: &str) -> Hlc {
    let (bytes, schema_hlc) = build_schema_snapshot(array_name);
    for shared in shareds {
        import_schema_snapshot(shared, array_name, &bytes, schema_hlc);
        register_catalog_entry(shared, array_name);
    }
    schema_hlc
}

/// Register a single node with a freshly built snapshot. Used by tests that
/// add a new node mid-flight (e.g. snapshot-install learner). Returns the
/// schema HLC, but tests should typically use the HLC produced by the
/// initial `register_schema_on_all` call instead.
#[allow(dead_code)]
pub fn register_schema(shared: &Arc<SharedState>, array_name: &str) -> Hlc {
    let (bytes, schema_hlc) = build_schema_snapshot(array_name);
    import_schema_snapshot(shared, array_name, &bytes, schema_hlc);
    register_catalog_entry(shared, array_name);
    schema_hlc
}
