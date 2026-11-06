// SPDX-License-Identifier: BUSL-1.1

/// Serializable snapshot of a tenant's Data Plane state.
///
/// Shared between Control Plane (backup/restore DDL) and Data Plane
/// (snapshot creation/restoration). Lives in `types` to avoid
/// cross-plane module visibility leaks.
#[derive(
    serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack, Default,
)]
#[msgpack(map)]
pub struct TenantDataSnapshot {
    /// Graph edges: `[("{collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:020}", value_bytes), ...]`
    ///
    /// The edge key does NOT carry the owning tenant. This (no-tenant) field is
    /// used by the per-tenant user RESTORE path, which dispatches with the
    /// correct tenant from context. For the multi-tenant merged Raft snapshot
    /// (which spans tenants and applies once with no per-tenant dispatch), use
    /// the tenant-aware companion [`Self::tenant_edges`] instead.
    pub edges: Vec<(String, Vec<u8>)>,
    /// Vector collections: `[("{db}:{tid}:{coll_key}", serialized_vectors_msgpack), ...]`
    /// Each value is a MessagePack-serialized list of `(vector_id, f32_data, doc_id)`.
    /// HNSW graph is NOT serialized — it's rebuilt on restore from raw vectors.
    pub vectors: Vec<(String, Vec<u8>)>,
    /// KV tables: `[("{db}:{tid}:{collection}", serialized_entries_msgpack), ...]`.
    /// `collection` is the database-qualified name the KV engine keys the table by.
    /// Each value is a MessagePack-serialized list of `(key_bytes, value_bytes, expire_at_ms)`.
    pub kv_tables: Vec<(String, Vec<u8>)>,
    /// CRDT state, one entry per `(tenant, collection)`:
    /// `[(tenant_id, collection, loro_export_bytes), ...]`. Each collection
    /// owns its own LoroDoc. `tenant_id` is carried explicitly because the
    /// merged multi-tenant Raft snapshot is applied with a dispatch tenant of 0.
    pub crdt_state: Vec<(u64, u64, String, Vec<u8>)>,
    /// Timeseries memtable data: `[("{db}:{tid}:{collection}", serialized_columns_msgpack), ...]`
    pub timeseries: Vec<(String, Vec<u8>)>,
    /// Flushed on-disk timeseries segments per collection.
    pub flushed_ts_segments: Vec<TsFlushedCollectionBlob>,
    /// Timeseries series catalogs: `[("{db}:{tid}:{collection}",
    /// series_catalog_file_bytes), ...]`. Every stored row carries its
    /// series id, so a snapshot carries the catalog that gave the ids: an
    /// installed collection gives every key the id its rows carry. The
    /// install puts each catalog in place before it restores any row.
    pub ts_series_catalogs: Vec<(String, Vec<u8>)>,

    /// The conversion record of each collection a conversion touched:
    /// `[("{db}:{tid}:{collection}", encoded_record), ...]`. It names the mode
    /// the snapshot's rows hold, so an install reads them with that mode.
    /// A backup leaves it out: its rows restore as new writes.
    pub storage_conversions: Vec<(String, Vec<u8>)>,

    /// Plain-columnar (and spatial) engine state per collection.
    ///
    /// Each entry is `(collection_key, msgpack_bytes)` where:
    /// - `collection_key` uses the same `"{database_id}:{tenant_id}:{collection}"`
    ///   format as every other scoped snapshot field.
    /// - `msgpack_bytes` is a zerompk-serialized `nodedb_columnar::ColumnarEngineSnapshot`
    ///   (stored as opaque bytes to keep this type decoupled from the columnar
    ///   wire layout and mirror the `vectors`/`kv_tables` encoding pattern).
    pub columnar_engines: Vec<(String, Vec<u8>)>,

    /// Per-collection HNSW parameters set via DDL, carried through snapshots
    /// so snapshot-installed followers rebuild vector collections with the
    /// original params instead of silently falling back to defaults.
    ///
    /// Key format: `"{db}:{tid}:{collection_key}"` (same as `vectors`).
    /// Value: msgpack-serialized `HnswParams`.
    pub vector_params: Vec<(String, Vec<u8>)>,

    /// Per-collection index config (index type, PQ/IVF params) carried through
    /// snapshots. Without this a snapshot-installed node gets wrong index routing
    /// (`IndexType::Hnsw` default instead of the configured type).
    ///
    /// Key format: `"{db}:{tid}:{collection_key}"` (same as `vectors`).
    /// Value: msgpack-serialized `IndexConfig`.
    pub index_configs: Vec<(String, Vec<u8>)>,

    /// PK → surrogate identity bindings for the snapshotted collections.
    ///
    /// The surrogate map (`surrogate_pk` / `surrogate_pk_rev` catalog
    /// tables) is DATA-derived per-node state: on the cluster apply path a
    /// follower binds it when it applies a replicated `PointInsert`/`PointPut`.
    /// The snapshot install path bypasses that apply path entirely (it installs
    /// doc blobs directly), so without carrying these bindings a
    /// snapshot-installed / restored node has documents but no PK→surrogate
    /// mapping — full scans work but PK point-lookups (`WHERE id=<pk>`) resolve
    /// to nothing. Carrying + rebinding these on the Control-Plane apply side
    /// closes that gap.
    ///
    /// The Data-Plane snapshot builder
    /// (`create.rs`) has no catalog access and leaves this empty; it is filled
    /// by the Control-Plane snapshot builder / backup orchestrator and consumed
    /// by the Control-Plane applier / restore orchestrator.
    pub surrogate_pk: Vec<SurrogateBindEntry>,

    /// Graph edges WITH their owning database and tenant, for the per-group
    /// Raft snapshot (the merged snapshot spans databases and tenants, and the
    /// edge key — `"{collection}\x00{src}\x00{label}\x00{dst}\x00{system:020}"` —
    /// carries neither). Each entry is `(database_id, tenant_id, edge_key,
    /// value_bytes)`. The `edges` field (no database, no tenant) is used by a
    /// restore dispatched in the snapshot's own database and tenant.
    pub tenant_edges: Vec<(u64, u64, String, Vec<u8>)>,

    /// Graph edge versions a TRUNCATE hides from a current read, keyed like
    /// `edges`. A read as of an earlier system time still reaches one, so a
    /// snapshot carries them with the cuts and applied ordinals. Empty in a
    /// backup: its RESTORE re-issues the versions a current read reaches.
    pub edge_hidden: Vec<(String, Vec<u8>)>,

    /// Every TRUNCATE cut of an edge collection, as `(collection, cut)`.
    /// Empty in a backup.
    pub edge_cuts: Vec<(String, i64)>,

    /// The applied ordinal of every edge version applied at another ordinal
    /// than its system time, as `(edge_key, applied)`. Empty in a backup.
    pub edge_applied: Vec<(String, i64)>,

    /// `edge_cuts` with each cut's database and tenant, for the per-group
    /// Raft snapshot, as `(database_id, tenant_id, collection, cut)`. The
    /// group's hidden edge versions travel in `tenant_edges`.
    pub tenant_edge_cuts: Vec<(u64, u64, String, i64)>,

    /// `edge_applied` with each entry's database and tenant, for the
    /// per-group Raft snapshot, as `(database_id, tenant_id, edge_key,
    /// applied)`.
    pub tenant_edge_applied: Vec<(u64, u64, String, i64)>,

    /// Every labeled node's current label set, as `(database_id, tenant_id,
    /// node, labels)`. A node's labels home on `VShardId::from_key(node)`.
    pub node_labels: Vec<NodeLabelsEntry>,

    /// Every node-label version, as `(database_id, tenant_id, node,
    /// system_from, labels)`. Each keeps its system time.
    pub node_label_versions: Vec<NodeLabelVersionEntry>,

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
    pub crdt_constraints: Vec<CrdtConstraintEntry>,

    /// The group's tenant write marks, for the per-group Raft snapshot:
    /// `[(tenant_id, commit_hlc, site_code, collection, restore_id), ...]`,
    /// with `restore_id` `0` for a user write's mark. A follower
    /// caught up by the snapshot never applies the entries it covers, so it
    /// takes their marks from here, and RESTORE's staleness guard reads the
    /// same marks on it as on every other replica. Empty in a backup: the
    /// guard reads the marks of the destination cluster.
    pub group_write_marks: Vec<(u64, u64, u8, String, u64)>,

    /// `(log_index, proposal_key)` of the group's committed entries at or
    /// below the Raft snapshot index, for the window a propose waiter can
    /// exist in. The follower that installs the snapshot never applies those
    /// entries. It takes their keys from here, so its duplicate check skips a
    /// later copy of the same proposal, and a waiter learns whether its own
    /// proposal committed. Empty in a backup.
    pub group_proposal_keys: Vec<(u64, u64)>,

    /// Lowest index from which `group_proposal_keys` is complete. `0` when
    /// every key in the window is present. Below it a missing key proves
    /// nothing, so a waiter there learns its outcome is unknown.
    pub group_proposal_keys_complete_from: u64,

    /// Every document version of every document collection:
    /// `[("{db}:{tid}:{collection}:{doc_id}\x00{system_from:020}", versioned_value), ...]`.
    /// Every version keeps its system time, tombstones included.
    pub documents_versioned: Vec<(String, Vec<u8>)>,

    /// Every versioned secondary-index entry of every document collection:
    /// `[("{db}:{tid}:{collection}:{field}:{value}:{doc_id}\x00{system_from:020}", tag), ...]`.
    pub indexes_versioned: Vec<(String, Vec<u8>)>,

    /// Multi-vector documents per vector index:
    /// `[("{db}:{tid}:{coll_key}", document_surrogates), ...]`, keyed like
    /// `vectors`. A `vectors` row carries no membership, and a one-vector
    /// multi-vector document is otherwise indistinguishable from a
    /// single-vector row.
    pub vector_multi_documents: Vec<(String, Vec<nodedb_types::Surrogate>)>,

    /// The system-time history of each vector index, keyed like `vectors`:
    /// `[("{db}:{tid}:{coll_key}", msgpack VectorIndexHistory), ...]`. A
    /// `vectors` row carries the live vector only.
    pub vector_versions: Vec<(String, Vec<u8>)>,

    /// Every sparse vector index with its history:
    /// `[("{db}:{tid}:{collection}", msgpack FieldIndexImage), ...]`.
    pub sparse_vectors: Vec<(String, Vec<u8>)>,

    /// Every spatial R-tree with its history and entry map:
    /// `[("{db}:{tid}:{collection}", msgpack FieldIndexImage), ...]`.
    pub spatial_indexes: Vec<(String, Vec<u8>)>,

    /// Array cell versions, one blob per `(array, vShard)` on each core.
    /// Every version keeps its system time, tombstones and erasures included.
    /// Only a snapshot requested with `arrays` carries them: a backup and a
    /// Raft group snapshot.
    pub arrays: Vec<ArrayCellsBlob>,

    /// The metadata group index the builder had applied when it captured a
    /// Raft group snapshot. Every row it carries belongs to a collection
    /// incarnation created at or below this index. The installing follower
    /// waits until its own metadata group applied this index. A new
    /// incarnation's storage clear then runs before the rows land, never
    /// after. `0` holds nothing: a backup and a per-core part carry `0`.
    pub metadata_floor: u64,

    /// The group's applied index a Raft group snapshot was captured at. The
    /// builder fences the group's apply for the capture, so every section,
    /// the write marks and the proposal keys hold exactly the entries at or
    /// below it. It is at or above the Raft snapshot index. The installing
    /// follower treats the entries between the two as covered and never
    /// applies them. `0` holds nothing: a backup and a per-core part carry
    /// `0`.
    pub group_cut_index: u64,

    /// The Event Plane lane state of a Raft group snapshot's vShards: the
    /// held trigger actions and committed messages, and their cursors. A
    /// follower caught up by the snapshot never applies the entries it
    /// covers, so it holds their events from here and can own the vShards'
    /// firing and delivery. Empty in a backup and a per-core part.
    pub group_event_lane: GroupEventLane,

    /// The Calvin cut of a Raft group snapshot: the sequencer index its
    /// Calvin state holds every input through, and each vShard's applied
    /// positions. `None` in a backup and a per-core part.
    pub group_calvin: Option<super::snapshot_calvin::GroupCalvinCut>,

    /// The open chunked redo streams of a Raft group snapshot's group. A
    /// follower caught up by the snapshot never applies the chunks it covers,
    /// so it holds them from here for each stream's final entry. Empty in a
    /// backup and a per-core part.
    pub group_redo_streams: Vec<crate::wal::CarriedRedoStream>,
}

/// One node's current label set: `(database_id, tenant_id, node, labels)`.
pub type NodeLabelsEntry = (u64, u64, String, Vec<String>);

/// One node-label version: `(database_id, tenant_id, node, system_from,
/// labels)`.
pub type NodeLabelVersionEntry = (u64, u64, String, i64, Vec<String>);

pub use super::snapshot_lane::{GroupEventLane, HeldAtPosition, PartitionCursor};

/// One array's cell versions whose tile keys route to one vShard.
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
pub struct ArrayCellsBlob {
    /// Database the array lives in.
    pub database_id: u64,
    /// Tenant owning the array.
    pub tenant_id: u64,
    /// Bare array name.
    pub array: String,
    /// The vShard the cells route to.
    pub vshard: u32,
    /// zerompk-encoded `Vec<crate::engine::array::export::ArrayCellVersion>`.
    pub cells: Vec<u8>,
}

/// One collection's CRDT constraint set plus its installed version, carried
/// through a snapshot so `restore_crdt_constraints` can reconstruct the
/// validator and the version together.
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
pub struct CrdtConstraintEntry {
    /// Database owning the collection.
    pub database_id: u64,
    /// Tenant owning the collection.
    pub tenant_id: u64,
    /// Collection the constraint set applies to.
    pub collection: String,
    /// Installed constraint version. The write gate rejects peer deltas whose
    /// `constraint_version_required` exceeds this.
    pub version: u64,
    /// Encoded constraint definitions.
    pub constraints: Vec<Vec<u8>>,
}

/// A single PK → surrogate identity binding carried in a snapshot/backup.
///
/// Mirrors one row of the `surrogate_pk` catalog table for one
/// `(database_id, tenant_id, collection)`. Rebound on the Control-Plane apply side via
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
    /// Database the collection lives in.
    pub database_id: u64,
    /// Owning tenant of the `(collection, pk)` binding.
    pub tenant_id: u64,
    /// Bare catalog name of the collection.
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
    /// The collection's continuous-aggregate image (`cagg.state`), or empty
    /// when no aggregate is rooted here. Its folded sets name partitions in
    /// `partitions` by directory name, and the restore keeps those names,
    /// so the image and the partitions stay in step. It carries the buckets
    /// folded from partitions raw retention has removed.
    pub cagg_image: Vec<u8>,
    /// The same aggregates for a copy that re-issues the collection's rows,
    /// or empty when no aggregate is rooted here. A re-issue keeps no partition
    /// name, so this image names none. A `Manual` or `Periodic` aggregate
    /// parks the rows it has not folded until its next cut. Any other holds
    /// them in its buckets.
    pub cagg_copy_image: Vec<u8>,
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

    /// Round-trip for the `surrogate_pk` field: a snapshot carrying bindings
    /// survives encode→decode intact.
    #[test]
    fn surrogate_pk_round_trips() {
        let snap = TenantDataSnapshot {
            surrogate_pk: vec![
                SurrogateBindEntry {
                    database_id: 0,
                    tenant_id: 7,
                    collection: "users".to_string(),
                    pk: b"row-0".to_vec(),
                    surrogate: 1,
                },
                SurrogateBindEntry {
                    database_id: 1025,
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
            cagg_image: vec![1, 2, 3],
            cagg_copy_image: vec![4, 5],
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
        assert_eq!(decoded.cagg_image, vec![1, 2, 3]);
    }

    /// Round-trip: `vector_params` and `index_configs` survive msgpack
    /// encode → decode with non-default values.
    ///
    /// The test seeds no params on a live CoreLoop. It exercises the struct
    /// wire format in isolation, because `TenantDataSnapshot` is the boundary
    /// type.
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
            declared_dim: 128,
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
            crdt_constraints: vec![CrdtConstraintEntry {
                database_id: 0,
                tenant_id: 7,
                collection: "users".to_string(),
                version: 3,
                constraints: vec![unique_bytes, not_null_bytes],
            }],
            ..Default::default()
        };

        let bytes = zerompk::to_msgpack_vec(&snap).expect("encode snapshot with crdt_constraints");
        let decoded: TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).expect("decode snapshot with crdt_constraints");

        assert_eq!(decoded.crdt_constraints.len(), 1);
        let entry = &decoded.crdt_constraints[0];
        let encoded = &entry.constraints;
        assert_eq!(entry.database_id, 0);
        assert_eq!(entry.tenant_id, 7);
        assert_eq!(entry.collection, "users");
        assert_eq!(entry.version, 3);
        assert_eq!(encoded.len(), 2);

        let decoded_unique: nodedb_crdt::Constraint =
            zerompk::from_msgpack(&encoded[0]).expect("decode Unique constraint");
        assert_eq!(decoded_unique, unique);
        let decoded_not_null: nodedb_crdt::Constraint =
            zerompk::from_msgpack(&encoded[1]).expect("decode NotNull constraint");
        assert_eq!(decoded_not_null, not_null);
    }
}
