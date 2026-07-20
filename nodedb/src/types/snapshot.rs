// SPDX-License-Identifier: BUSL-1.1

/// Serializable snapshot of a tenant's Data Plane state.
///
/// Shared between Control Plane (backup/restore DDL) and Data Plane
/// (snapshot creation/restoration). Lives in `types` to avoid
/// cross-plane module visibility leaks.
///
/// Map-encoded (`#[msgpack(map)]`) so fields can be added with
/// `#[msgpack(default)]` and older snapshots (serialized before the field
/// existed) still decode without a migration — new fields appear with their
/// `Default` value. This is the same evolution pattern used by
/// `ContinuousAggregateDef` and `RetentionPolicyDef`.
#[derive(
    serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack, Default,
)]
#[msgpack(map)]
pub struct TenantDataSnapshot {
    /// Sparse engine documents: `[("{tid}:{collection}:{doc_id}", value_bytes), ...]`
    pub documents: Vec<(String, Vec<u8>)>,
    /// Sparse engine index entries: `[("{tid}:{collection}:{field}:{value}:{doc_id}", []), ...]`
    pub indexes: Vec<(String, Vec<u8>)>,
    /// Graph edges: `[("{collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:020}", value_bytes), ...]`
    ///
    /// The edge key does NOT carry the owning tenant. This (no-tenant) field is
    /// used by the per-tenant user RESTORE path, which dispatches with the
    /// correct tenant from context. For the multi-tenant merged Raft snapshot
    /// (which spans tenants and applies once with no per-tenant dispatch), use
    /// the tenant-aware companion [`Self::tenant_edges`] instead.
    pub edges: Vec<(String, Vec<u8>)>,
    /// Vector collections: `[("{tid}:{collection}", serialized_vectors_msgpack), ...]`
    /// Each value is a MessagePack-serialized list of `(vector_id, f32_data, doc_id)`.
    /// HNSW graph is NOT serialized — it's rebuilt on restore from raw vectors.
    pub vectors: Vec<(String, Vec<u8>)>,
    /// KV tables: `[("{tid}:{collection}", serialized_entries_msgpack), ...]`
    /// Each value is a MessagePack-serialized list of `(key_bytes, value_bytes, expire_at_ms)`.
    pub kv_tables: Vec<(String, Vec<u8>)>,
    /// CRDT state, one entry per `(tenant, collection)`:
    /// `[(tenant_id, collection, loro_export_bytes), ...]`. Each collection
    /// owns its own LoroDoc. `tenant_id` is carried explicitly because the
    /// merged multi-tenant Raft snapshot is applied with a dispatch tenant of 0.
    /// `#[msgpack(default)]`: snapshots written before this field decode empty.
    #[msgpack(default)]
    #[serde(default)]
    pub crdt_state: Vec<(u64, u64, String, Vec<u8>)>,
    /// Timeseries memtable data: `[("{tid}:{collection}", serialized_columns_msgpack), ...]`
    pub timeseries: Vec<(String, Vec<u8>)>,
    /// Flushed on-disk timeseries segments per collection.
    ///
    /// `#[msgpack(default)]`: snapshots created before this field was added
    /// decode with an empty Vec — the restore path treats an empty slice as
    /// "no flushed segments to restore", which is safe and correct.
    #[msgpack(default)]
    #[serde(default)]
    pub flushed_ts_segments: Vec<TsFlushedCollectionBlob>,

    /// Plain-columnar (and spatial) engine state per collection.
    ///
    /// Each entry is `(collection_key, msgpack_bytes)` where:
    /// - `collection_key` uses the same `"{database_id}:{tenant_id}:{collection}"`
    ///   format as every other scoped snapshot field.
    /// - `msgpack_bytes` is a zerompk-serialized `nodedb_columnar::ColumnarEngineSnapshot`
    ///   (stored as opaque bytes to keep this type decoupled from the columnar
    ///   wire layout and mirror the `vectors`/`kv_tables` encoding pattern).
    ///
    /// `#[msgpack(default)]`: snapshots created before this field was added
    /// decode with an empty Vec — safe because the restore path skips an
    /// empty slice.
    #[msgpack(default)]
    #[serde(default)]
    pub columnar_engines: Vec<(String, Vec<u8>)>,

    /// Per-collection HNSW parameters set via DDL, carried through snapshots
    /// so snapshot-installed followers rebuild vector collections with the
    /// original params instead of silently falling back to defaults.
    ///
    /// Key format: `"{db}:{tid}:{collection_key}"` (same as `vectors`).
    /// Value: msgpack-serialized `HnswParams`.
    ///
    /// `#[msgpack(default)]`: snapshots written before this field decode with
    /// an empty Vec — the restore path falls back to `HnswParams::default()`,
    /// which matches the pre-fix behavior.
    #[msgpack(default)]
    #[serde(default)]
    pub vector_params: Vec<(String, Vec<u8>)>,

    /// Per-collection index config (index type, PQ/IVF params) carried through
    /// snapshots. Without this a snapshot-installed node gets wrong index routing
    /// (`IndexType::Hnsw` default instead of the configured type).
    ///
    /// Key format: `"{db}:{tid}:{collection_key}"` (same as `vectors`).
    /// Value: msgpack-serialized `IndexConfig`.
    ///
    /// `#[msgpack(default)]`: snapshots written before this field decode with
    /// an empty Vec — the restore path skips the loop, leaving the collection
    /// to use default index routing, matching pre-fix behavior.
    #[msgpack(default)]
    #[serde(default)]
    pub index_configs: Vec<(String, Vec<u8>)>,

    /// PK → surrogate identity bindings for the snapshotted collections.
    ///
    /// The surrogate map (`surrogate_pk_v3` / `surrogate_pk_rev_v3` catalog
    /// tables) is DATA-derived per-node state: on the cluster apply path a
    /// follower binds it when it applies a replicated `PointInsert`/`PointPut`.
    /// The snapshot install path bypasses that apply path entirely (it installs
    /// doc blobs directly), so without carrying these bindings a
    /// snapshot-installed / restored node has documents but no PK→surrogate
    /// mapping — full scans work but PK point-lookups (`WHERE id=<pk>`) resolve
    /// to nothing. Carrying + rebinding these on the Control-Plane apply side
    /// closes that gap.
    ///
    /// `#[msgpack(default)]`: snapshots/backups created before this field was
    /// added decode with an empty Vec — the rebind step treats an empty slice
    /// as "nothing to rebind", which is safe. The Data-Plane snapshot builder
    /// (`create.rs`) has no catalog access and leaves this empty; it is filled
    /// by the Control-Plane snapshot builder / backup orchestrator and consumed
    /// by the Control-Plane applier / restore orchestrator.
    #[msgpack(default)]
    #[serde(default)]
    pub surrogate_pk: Vec<SurrogateBindEntry>,

    /// Graph edges WITH their owning tenant, for the per-group Raft snapshot
    /// (the merged snapshot spans multiple tenants and the edge key —
    /// `"{collection}\x00{src}\x00{label}\x00{dst}\x00{system:020}"` — does NOT
    /// carry the tenant, unlike every other section's key). Each entry is
    /// `(tenant_id, edge_key, value_bytes)`. The legacy `edges` field (no tenant)
    /// is still used by the per-tenant user RESTORE path, which dispatches with
    /// the correct tenant; this field is for the multi-tenant merged Raft path.
    ///
    /// `#[msgpack(default)]`: snapshots created before this field was added
    /// decode with an empty Vec — safe because the restore path skips an empty
    /// slice (same evolution pattern as `surrogate_pk`).
    #[msgpack(default)]
    #[serde(default)]
    pub tenant_edges: Vec<(u64, String, Vec<u8>)>,

    /// CRDT constraint state, one entry per `(tenant, collection)` that has an
    /// installed constraint set: `[(tenant_id, collection, constraint_version,
    /// per_constraint_zerompk_bytes), ...]`. Each inner `Vec<u8>` is one
    /// zerompk-encoded `nodedb_crdt::Constraint`. Stored as opaque bytes (NOT
    /// typed) to keep this type decoupled from the crdt wire layout — the same
    /// reason `crdt_state` stores raw loro bytes.
    ///
    /// Without this field a snapshot-installed follower ends up with
    /// `installed_constraint_version == 0` on every constrained collection,
    /// even though the leader has constraints installed, and the apply-time
    /// write-gate fences (rejects) every peer delta on that collection
    /// forever, since the delta's `constraint_version_required` can never be
    /// `<=` an installed version that never advances. Carrying the constraint
    /// set + version through the snapshot lets `restore_crdt_constraints`
    /// reconstruct the validator and the installed version in one shot.
    ///
    /// `#[msgpack(default)]`: snapshots written before this field decode with
    /// an empty Vec — the restore path skips the loop, reproducing the
    /// pre-fix (fail-safe, over-rejecting) behavior rather than a hard error.
    #[msgpack(default)]
    #[serde(default)]
    pub crdt_constraints: Vec<(u64, u64, String, u64, Vec<Vec<u8>>)>,
}

/// A single PK → surrogate identity binding carried in a snapshot/backup.
///
/// Mirrors one row of the `surrogate_pk_v3` catalog table for one
/// `(tenant_id, collection)`. Rebound on the Control-Plane apply side via
/// `SystemCatalog::put_surrogate` so PK point-lookups resolve on a
/// snapshot-installed / restored node.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Default,
)]
pub struct SurrogateBindEntry {
    /// Owning tenant of the `(collection, pk)` binding.
    pub tenant_id: u64,
    /// Collection name (DEFAULT database scope).
    pub collection: String,
    /// Primary-key bytes (the catalog forward-table key component).
    pub pk: Vec<u8>,
    /// Surrogate the PK is bound to.
    pub surrogate: u32,
}

/// Wire blob for all flushed partitions of one timeseries collection.
///
/// The `collection_key` uses the same `"{db}:{tid}:{collection}"` format
/// as the `timeseries` field's keys so the key parsers are shared.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Default,
)]
pub struct TsFlushedCollectionBlob {
    /// `"{database_id}:{tenant_id}:{collection}"` — the same scoped key format
    /// used throughout the timeseries snapshot fields.
    pub collection_key: String,
    /// One blob per flushed partition directory.
    pub partitions: Vec<TsFlushedPartitionBlob>,
}

/// Wire blob for one flushed partition directory.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Default,
)]
pub struct TsFlushedPartitionBlob {
    /// Directory name of the partition (e.g. `"ts-20240101-000000_20240102-000000"`).
    pub dir_name: String,
    /// Partition metadata — captured directly from `PartitionEntry::meta`.
    /// Embedded as a nested msgpack blob to avoid coupling `TsFlushedPartitionBlob`
    /// to `PartitionMeta`'s zerompk wire layout at the outer struct level.
    pub meta_bytes: Vec<u8>,
    /// All files in the partition directory: `(filename, raw_bytes)`.
    pub files: Vec<(String, Vec<u8>)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Backward-compat: a `TenantDataSnapshot` serialized WITHOUT the
    /// `columnar_engines` field (simulating a snapshot from before this field
    /// was added) must decode successfully with `columnar_engines` defaulting
    /// to `Vec::new()`.
    ///
    /// We simulate the "old" wire format by defining an 8-field map-encoded
    /// struct that matches the schema before `columnar_engines` was added,
    /// serialising it, then decoding as the new 9-field `TenantDataSnapshot`.
    /// zerompk's `#[msgpack(default)]` fills in the missing field with
    /// `Vec::new()`.
    #[test]
    fn backward_compat_missing_columnar_engines_defaults_to_empty() {
        #[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
        #[msgpack(map)]
        struct OldSnapshot {
            documents: Vec<(String, Vec<u8>)>,
            indexes: Vec<(String, Vec<u8>)>,
            edges: Vec<(String, Vec<u8>)>,
            vectors: Vec<(String, Vec<u8>)>,
            kv_tables: Vec<(String, Vec<u8>)>,
            timeseries: Vec<(String, Vec<u8>)>,
            flushed_ts_segments: Vec<TsFlushedCollectionBlob>,
        }

        let old = OldSnapshot {
            documents: vec![("k".to_string(), b"v".to_vec())],
            indexes: vec![],
            edges: vec![],
            vectors: vec![],
            kv_tables: vec![],
            timeseries: vec![("ts:c".to_string(), b"data".to_vec())],
            flushed_ts_segments: vec![],
        };
        let bytes = zerompk::to_msgpack_vec(&old).expect("encode old snapshot");

        // Decode as new schema — columnar_engines must default to empty.
        let decoded: TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).expect("decode old snapshot as new schema");
        assert_eq!(decoded.documents.len(), 1);
        assert_eq!(decoded.timeseries.len(), 1);
        assert!(
            decoded.columnar_engines.is_empty(),
            "expected columnar_engines to default to empty for old snapshot"
        );
        assert!(
            decoded.surrogate_pk.is_empty(),
            "expected surrogate_pk to default to empty for old snapshot"
        );
        assert!(
            decoded.tenant_edges.is_empty(),
            "expected tenant_edges to default to empty for old snapshot"
        );
        assert!(
            decoded.crdt_state.is_empty(),
            "expected crdt_state to default to empty for old snapshot"
        );
    }

    /// Round-trip + backward-compat for the `surrogate_pk` field: a snapshot
    /// carrying bindings survives encode→decode intact, and a snapshot
    /// serialized WITHOUT the field (the 9-field schema that existed before
    /// `surrogate_pk` was added) decodes with `surrogate_pk` defaulting to
    /// empty.
    #[test]
    fn surrogate_pk_round_trips_and_back_compat() {
        let snap = TenantDataSnapshot {
            surrogate_pk: vec![
                SurrogateBindEntry {
                    tenant_id: 7,
                    collection: "users".to_string(),
                    pk: b"row-0".to_vec(),
                    surrogate: 1,
                },
                SurrogateBindEntry {
                    tenant_id: 7,
                    collection: "users".to_string(),
                    pk: b"row-1".to_vec(),
                    surrogate: 2,
                },
            ],
            ..Default::default()
        };
        let bytes = zerompk::to_msgpack_vec(&snap).expect("encode snapshot with surrogate_pk");
        let decoded: TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).expect("decode snapshot with surrogate_pk");
        assert_eq!(decoded.surrogate_pk, snap.surrogate_pk);

        // Old 9-field schema (pre-surrogate_pk) must still decode.
        #[derive(zerompk::ToMessagePack)]
        #[msgpack(map)]
        struct OldSnapshot {
            documents: Vec<(String, Vec<u8>)>,
            indexes: Vec<(String, Vec<u8>)>,
            edges: Vec<(String, Vec<u8>)>,
            vectors: Vec<(String, Vec<u8>)>,
            kv_tables: Vec<(String, Vec<u8>)>,
            timeseries: Vec<(String, Vec<u8>)>,
            flushed_ts_segments: Vec<TsFlushedCollectionBlob>,
            columnar_engines: Vec<(String, Vec<u8>)>,
        }
        let old = OldSnapshot {
            documents: vec![("k".to_string(), b"v".to_vec())],
            indexes: vec![],
            edges: vec![],
            vectors: vec![],
            kv_tables: vec![],
            timeseries: vec![],
            flushed_ts_segments: vec![],
            columnar_engines: vec![],
        };
        let old_bytes = zerompk::to_msgpack_vec(&old).expect("encode old snapshot");
        let decoded_old: TenantDataSnapshot =
            zerompk::from_msgpack(&old_bytes).expect("decode old snapshot as new schema");
        assert_eq!(decoded_old.documents.len(), 1);
        assert!(
            decoded_old.surrogate_pk.is_empty(),
            "expected surrogate_pk to default to empty for old snapshot"
        );
        assert!(
            decoded_old.tenant_edges.is_empty(),
            "expected tenant_edges to default to empty for old snapshot"
        );
        assert!(
            decoded_old.crdt_state.is_empty(),
            "expected crdt_state to default to empty for old snapshot"
        );
    }

    /// Backward-compat: a `TenantDataSnapshot` serialized WITHOUT the
    /// `flushed_ts_segments` field (simulating a snapshot from before this
    /// field was added) must decode successfully with `flushed_ts_segments`
    /// defaulting to `Vec::new()`.
    ///
    /// We simulate the "old" wire format by defining a 7-field map-encoded
    /// struct that matches the original schema, serialising it, then decoding
    /// as the new 8-field `TenantDataSnapshot`. zerompk's `#[msgpack(default)]`
    /// fills in the missing `flushed_ts_segments` with `Vec::new()`.
    #[test]
    fn backward_compat_missing_flushed_ts_segments_defaults_to_empty() {
        /// Mirrors the original 7-field schema — no `flushed_ts_segments`.
        #[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
        #[msgpack(map)]
        struct OldSnapshot {
            documents: Vec<(String, Vec<u8>)>,
            indexes: Vec<(String, Vec<u8>)>,
            edges: Vec<(String, Vec<u8>)>,
            vectors: Vec<(String, Vec<u8>)>,
            kv_tables: Vec<(String, Vec<u8>)>,
            timeseries: Vec<(String, Vec<u8>)>,
        }

        let old = OldSnapshot {
            documents: vec![("k".to_string(), b"v".to_vec())],
            indexes: vec![],
            edges: vec![],
            vectors: vec![],
            kv_tables: vec![],
            timeseries: vec![("ts:c".to_string(), b"data".to_vec())],
        };
        let bytes = zerompk::to_msgpack_vec(&old).expect("encode old snapshot");

        // Decode as new schema — flushed_ts_segments must default to empty.
        let decoded: TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).expect("decode old snapshot as new schema");
        assert_eq!(decoded.documents.len(), 1);
        assert_eq!(decoded.timeseries.len(), 1);
        assert!(
            decoded.flushed_ts_segments.is_empty(),
            "expected flushed_ts_segments to default to empty for old snapshot"
        );
    }

    /// Blob round-trip: `TsFlushedCollectionBlob` with one partition containing
    /// fake files and meta_bytes survives msgpack encode → decode intact.
    #[test]
    fn flushed_collection_blob_round_trips() {
        let partition = TsFlushedPartitionBlob {
            dir_name: "ts-20240101-000000_20240102-000000".to_string(),
            meta_bytes: b"fake-meta".to_vec(),
            files: vec![
                ("schema.json".to_string(), b"{\"v\":1}".to_vec()),
                ("col_ts.col".to_string(), b"\x00\x01\x02".to_vec()),
            ],
        };
        let blob = TsFlushedCollectionBlob {
            collection_key: "1:42:metrics".to_string(),
            partitions: vec![partition],
        };

        let bytes = zerompk::to_msgpack_vec(&blob).expect("encode blob");
        let decoded: TsFlushedCollectionBlob = zerompk::from_msgpack(&bytes).expect("decode blob");

        assert_eq!(decoded.collection_key, "1:42:metrics");
        assert_eq!(decoded.partitions.len(), 1);
        let p = &decoded.partitions[0];
        assert_eq!(p.dir_name, "ts-20240101-000000_20240102-000000");
        assert_eq!(p.meta_bytes, b"fake-meta");
        assert_eq!(p.files.len(), 2);
        assert_eq!(p.files[0].0, "schema.json");
        assert_eq!(p.files[1].0, "col_ts.col");
        assert_eq!(p.files[1].1, b"\x00\x01\x02");
    }

    /// Round-trip: `vector_params` and `index_configs` survive msgpack
    /// encode → decode with non-default values.
    ///
    /// This test FAILS without the two new fields on `TenantDataSnapshot`
    /// because the fields simply do not exist and there is nothing to assert.
    /// With the fix, both sections are present, carry non-default values, and
    /// decode back intact. The test does NOT pre-seed params on a live CoreLoop
    /// so it exercises the struct wire format in isolation — the appropriate
    /// level since `TenantDataSnapshot` is the boundary type.
    #[test]
    fn vector_params_and_index_configs_round_trip() {
        use nodedb_types::hnsw::HnswParams;
        use nodedb_vector::index_config::{IndexConfig, IndexType};

        // Non-default HnswParams (m=32 vs default 16, ef_construction=400 vs 200).
        let non_default_params = HnswParams {
            m: 32,
            m0: 64,
            ef_construction: 400,
            metric: nodedb_types::vector_distance::DistanceMetric::Cosine,
            dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
        };
        let non_default_cfg = IndexConfig {
            hnsw: non_default_params.clone(),
            index_type: IndexType::HnswPq,
            pq_m: 16,
            ivf_cells: 512,
            ivf_nprobe: 32,
        };

        let params_bytes = zerompk::to_msgpack_vec(&non_default_params).expect("encode HnswParams");
        let cfg_bytes = zerompk::to_msgpack_vec(&non_default_cfg).expect("encode IndexConfig");

        let snap = TenantDataSnapshot {
            vector_params: vec![("1:42:embeddings".to_string(), params_bytes)],
            index_configs: vec![("1:42:embeddings".to_string(), cfg_bytes)],
            ..Default::default()
        };

        let encoded = zerompk::to_msgpack_vec(&snap).expect("encode snapshot");
        let decoded: TenantDataSnapshot = zerompk::from_msgpack(&encoded).expect("decode snapshot");

        assert_eq!(
            decoded.vector_params.len(),
            1,
            "vector_params must survive round-trip"
        );
        assert_eq!(decoded.vector_params[0].0, "1:42:embeddings");
        let decoded_params: HnswParams =
            zerompk::from_msgpack(&decoded.vector_params[0].1).expect("decode HnswParams");
        assert_eq!(decoded_params.m, 32, "non-default m must be preserved");
        assert_eq!(
            decoded_params.ef_construction, 400,
            "non-default ef_construction must be preserved"
        );

        assert_eq!(
            decoded.index_configs.len(),
            1,
            "index_configs must survive round-trip"
        );
        assert_eq!(decoded.index_configs[0].0, "1:42:embeddings");
        let decoded_cfg: IndexConfig =
            zerompk::from_msgpack(&decoded.index_configs[0].1).expect("decode IndexConfig");
        assert_eq!(
            decoded_cfg.index_type,
            IndexType::HnswPq,
            "non-default index_type must be preserved"
        );
        assert_eq!(decoded_cfg.pq_m, 16, "non-default pq_m must be preserved");
    }

    /// Round-trip: `crdt_constraints` survives msgpack encode → decode intact,
    /// and each inner blob decodes back into the exact `nodedb_crdt::Constraint`
    /// it was encoded from.
    #[test]
    fn crdt_constraints_round_trip() {
        let unique = nodedb_crdt::Constraint {
            name: "users_email_unique".to_string(),
            collection: "users".to_string(),
            field: "email".to_string(),
            kind: nodedb_crdt::ConstraintKind::Unique,
        };
        let not_null = nodedb_crdt::Constraint {
            name: "users_email_not_null".to_string(),
            collection: "users".to_string(),
            field: "email".to_string(),
            kind: nodedb_crdt::ConstraintKind::NotNull,
        };
        let unique_bytes = zerompk::to_msgpack_vec(&unique).expect("encode Unique constraint");
        let not_null_bytes = zerompk::to_msgpack_vec(&not_null).expect("encode NotNull constraint");

        let snap = TenantDataSnapshot {
            crdt_constraints: vec![(
                0,
                7,
                "users".to_string(),
                3,
                vec![unique_bytes, not_null_bytes],
            )],
            ..Default::default()
        };

        let bytes = zerompk::to_msgpack_vec(&snap).expect("encode snapshot with crdt_constraints");
        let decoded: TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).expect("decode snapshot with crdt_constraints");

        assert_eq!(decoded.crdt_constraints.len(), 1);
        let (database_id, tid, collection, version, encoded) = &decoded.crdt_constraints[0];
        assert_eq!(*database_id, 0);
        assert_eq!(*tid, 7);
        assert_eq!(collection, "users");
        assert_eq!(*version, 3);
        assert_eq!(encoded.len(), 2);

        let decoded_unique: nodedb_crdt::Constraint =
            zerompk::from_msgpack(&encoded[0]).expect("decode Unique constraint");
        assert_eq!(decoded_unique, unique);
        let decoded_not_null: nodedb_crdt::Constraint =
            zerompk::from_msgpack(&encoded[1]).expect("decode NotNull constraint");
        assert_eq!(decoded_not_null, not_null);
    }

    /// Backward-compat: a `TenantDataSnapshot` serialized WITHOUT the
    /// `crdt_constraints` field (the schema that existed before this field
    /// was added) must decode successfully with `crdt_constraints` defaulting
    /// to `Vec::new()`.
    #[test]
    fn backward_compat_missing_crdt_constraints_defaults_to_empty() {
        #[derive(zerompk::ToMessagePack)]
        #[msgpack(map)]
        struct OldSnapshot {
            documents: Vec<(String, Vec<u8>)>,
            indexes: Vec<(String, Vec<u8>)>,
            edges: Vec<(String, Vec<u8>)>,
            vectors: Vec<(String, Vec<u8>)>,
            kv_tables: Vec<(String, Vec<u8>)>,
            crdt_state: Vec<(u64, String, Vec<u8>)>,
            timeseries: Vec<(String, Vec<u8>)>,
            flushed_ts_segments: Vec<TsFlushedCollectionBlob>,
            columnar_engines: Vec<(String, Vec<u8>)>,
            vector_params: Vec<(String, Vec<u8>)>,
            index_configs: Vec<(String, Vec<u8>)>,
            surrogate_pk: Vec<SurrogateBindEntry>,
            tenant_edges: Vec<(u64, String, Vec<u8>)>,
        }
        let old = OldSnapshot {
            documents: vec![("k".to_string(), b"v".to_vec())],
            indexes: Vec::<(String, Vec<u8>)>::new(),
            edges: Vec::<(String, Vec<u8>)>::new(),
            vectors: Vec::<(String, Vec<u8>)>::new(),
            kv_tables: Vec::<(String, Vec<u8>)>::new(),
            crdt_state: Vec::<(u64, String, Vec<u8>)>::new(),
            timeseries: Vec::<(String, Vec<u8>)>::new(),
            flushed_ts_segments: Vec::<TsFlushedCollectionBlob>::new(),
            columnar_engines: Vec::<(String, Vec<u8>)>::new(),
            vector_params: Vec::<(String, Vec<u8>)>::new(),
            index_configs: Vec::<(String, Vec<u8>)>::new(),
            surrogate_pk: Vec::<SurrogateBindEntry>::new(),
            tenant_edges: Vec::<(u64, String, Vec<u8>)>::new(),
        };
        let bytes = zerompk::to_msgpack_vec(&old).expect("encode old snapshot");
        let decoded: TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).expect("decode old snapshot as new schema");
        assert_eq!(decoded.documents.len(), 1);
        assert!(
            decoded.crdt_constraints.is_empty(),
            "crdt_constraints must default to empty for old snapshot"
        );
    }

    /// Backward-compat: a snapshot written WITHOUT `vector_params` and
    /// `index_configs` (old wire format) must still decode with both fields
    /// defaulting to empty — matching the `#[msgpack(default)]` contract.
    #[test]
    fn backward_compat_missing_vector_params_and_index_configs_default_to_empty() {
        // Old schema: the 11-field struct that existed before the two new fields.
        #[derive(zerompk::ToMessagePack)]
        #[msgpack(map)]
        struct OldSnapshot {
            documents: Vec<(String, Vec<u8>)>,
            indexes: Vec<(String, Vec<u8>)>,
            edges: Vec<(String, Vec<u8>)>,
            vectors: Vec<(String, Vec<u8>)>,
            kv_tables: Vec<(String, Vec<u8>)>,
            crdt_state: Vec<(u64, String, Vec<u8>)>,
            timeseries: Vec<(String, Vec<u8>)>,
            flushed_ts_segments: Vec<TsFlushedCollectionBlob>,
            columnar_engines: Vec<(String, Vec<u8>)>,
            surrogate_pk: Vec<SurrogateBindEntry>,
            tenant_edges: Vec<(u64, String, Vec<u8>)>,
        }
        let old = OldSnapshot {
            documents: vec![("k".to_string(), b"v".to_vec())],
            indexes: Vec::<(String, Vec<u8>)>::new(),
            edges: Vec::<(String, Vec<u8>)>::new(),
            vectors: Vec::<(String, Vec<u8>)>::new(),
            kv_tables: Vec::<(String, Vec<u8>)>::new(),
            crdt_state: Vec::<(u64, String, Vec<u8>)>::new(),
            timeseries: Vec::<(String, Vec<u8>)>::new(),
            flushed_ts_segments: Vec::<TsFlushedCollectionBlob>::new(),
            columnar_engines: Vec::<(String, Vec<u8>)>::new(),
            surrogate_pk: Vec::<SurrogateBindEntry>::new(),
            tenant_edges: Vec::<(u64, String, Vec<u8>)>::new(),
        };
        let bytes = zerompk::to_msgpack_vec(&old).expect("encode old snapshot");
        let decoded: TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).expect("decode old snapshot as new schema");
        assert_eq!(decoded.documents.len(), 1);
        assert!(
            decoded.vector_params.is_empty(),
            "vector_params must default to empty for old snapshot"
        );
        assert!(
            decoded.index_configs.is_empty(),
            "index_configs must default to empty for old snapshot"
        );
    }
}
