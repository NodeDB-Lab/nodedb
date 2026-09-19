// SPDX-License-Identifier: Apache-2.0

//! Vector engine operations dispatched to the Data Plane.

use nodedb_types::{
    QualifiedCollection, Surrogate, SurrogateBitmap, vector_distance::DistanceMetric,
};

use crate::physical_plan::PhysicalPlan;
use crate::physical_plan::document::{ReturningSpec, UpdateValue};

use super::resolved_mutation::VectorResolvedMutation;
use super::write::VectorWriteTargets;

/// Vector engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum VectorOp {
    /// Vector similarity search.
    Search {
        collection: QualifiedCollection,
        query_vector: Vec<f32>,
        top_k: usize,
        /// Optional search beam width override. If 0, uses default `4 * top_k`.
        ef_search: usize,
        /// Per-query distance metric override. Overrides the collection-default
        /// metric configured at index creation time.
        metric: DistanceMetric,
        /// Pre-computed bitmap of eligible surrogates (from filter evaluation).
        filter_bitmap: Option<SurrogateBitmap>,
        /// Named vector field to search. Empty string = default field.
        field_name: String,
        /// RLS post-candidate filters (serialized `Vec<ScanFilter>`).
        /// Applied after HNSW returns candidates, before returning to client.
        /// Result count may be less than requested `top_k`.
        rls_filters: Vec<u8>,
        /// Optional sub-plan whose output rows materialize a `SurrogateBitmap`
        /// used as an additional prefilter (intersected with `filter_bitmap`).
        ///
        /// Set by the planner when fusing `Vector ORDER BY ... + JOIN
        /// ARRAY_SLICE(...) ON v.id = s.surrogate` into a single op:
        /// the array slice runs first, its surrogate column becomes the
        /// vector engine's pre-filter, then HNSW search runs against the
        /// reduced candidate set. Mirrors `HashJoin`'s `inline_*_bitmap`
        /// mechanism so the Data Plane composition is uniform.
        inline_prefilter_plan: Option<Box<PhysicalPlan>>,
        /// ANN tuning knobs from the SQL caller. Defaults to no overrides.
        ann_options: nodedb_types::VectorAnnOptions,
        /// When `true`, the SELECT projection contains only `id` and/or
        /// `vector_distance(...)` — no payload fields. The handler skips
        /// the document-body fetch. Ignored (treated as `false`) when RLS
        /// filters are active, as body fetch is required for RLS evaluation.
        skip_payload_fetch: bool,
        /// Optional payload bitmap pre-filter for vector-primary collections.
        /// Each atom is `Eq` / `In` / `Range` — the handler ANDs all atoms
        /// and intersects the resulting bitmap with the HNSW candidate set
        /// before walking. The planner only emits atoms whose field has a
        /// registered payload index of the matching kind. Empty means no
        /// payload pre-filter.
        payload_filters: Vec<nodedb_types::PayloadAtom>,
    },

    /// Insert a vector into the HNSW index (write path).
    Insert {
        collection: QualifiedCollection,
        vector: Vec<f32>,
        dim: usize,
        /// Named vector field. Empty string = default (unnamed) field.
        field_name: String,
        /// Global surrogate identifying this row. Allocated by the
        /// Control Plane via `SurrogateAssigner` before dispatch; the
        /// engine binds the HNSW node id to this surrogate.
        surrogate: Surrogate,
        /// User PK bytes (UTF-8 of the document id) when the insert
        /// originates from a PK-bearing path; `None` for headless
        /// inserts. Carried on the replication wire so followers bind
        /// the leader-assigned surrogate to this exact key rather than
        /// re-allocating a divergent one.
        #[serde(default)]
        pk_bytes: Option<Vec<u8>>,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    },

    /// Batch insert vectors into the HNSW index.
    BatchInsert {
        collection: QualifiedCollection,
        vectors: Vec<Vec<f32>>,
        dim: usize,
        /// One surrogate per inserted vector, parallel to `vectors`.
        /// Empty vector = headless batch (no PK binding).
        surrogates: Vec<Surrogate>,
    },

    /// Multi-vector search: query across all named vector fields, fuse via RRF.
    MultiSearch {
        collection: QualifiedCollection,
        query_vector: Vec<f32>,
        top_k: usize,
        ef_search: usize,
        filter_bitmap: Option<SurrogateBitmap>,
        /// RLS post-candidate filters.
        rls_filters: Vec<u8>,
    },

    /// Soft-delete a vector by internal node ID.
    Delete {
        collection: QualifiedCollection,
        vector_id: u32,
    },

    /// Soft-delete a vector by surrogate (sync inbound path).
    ///
    /// The Data Plane resolves `surrogate → HNSW node_id` via the
    /// `surrogate_to_local` map and tombstones the node.  If the surrogate
    /// is not found the op is a no-op (idempotent).
    DeleteBySurrogate {
        collection: QualifiedCollection,
        surrogate: nodedb_types::Surrogate,
        /// Named vector field; empty = default field.
        field_name: String,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    },

    /// Set vector index parameters for a collection.
    SetParams {
        collection: QualifiedCollection,
        /// Named vector field this config applies to. Empty string = the
        /// default (unnamed) vector field. Lets one collection carry
        /// multiple vector indexes, one per `VECTOR` / embedding column.
        field_name: String,
        /// Declared vector dimension. Enforced against every vector written
        /// to this field; `0` means "not declared" and leaves the index to
        /// adopt the width of whatever arrives first.
        #[serde(default)]
        dim: usize,
        m: usize,
        ef_construction: usize,
        metric: String,
        /// Index type: "hnsw" (default), "hnsw_pq", or "ivf_pq".
        index_type: String,
        /// PQ subvectors (for hnsw_pq and ivf_pq). Default: 8.
        pq_m: usize,
        /// IVF cells (for ivf_pq only). Default: 256.
        ivf_cells: usize,
        /// IVF probe count (for ivf_pq only). Default: 16.
        ivf_nprobe: usize,
    },

    /// Tear down one vector index: the materialized graph, its build
    /// parameters, its declared dimension, and its on-disk checkpoint.
    ///
    /// The counterpart of [`VectorOp::SetParams`]. Without it a dropped
    /// vector index keeps answering searches until the process restarts and
    /// keeps its `(collection, field)` slot occupied so no replacement index
    /// can be created.
    DropIndex {
        collection: QualifiedCollection,
        /// Named vector field the index covers. Empty string = the default
        /// (unnamed) vector field.
        field_name: String,
    },

    /// Query live vector index statistics. Returns `VectorIndexStats` as payload.
    QueryStats {
        collection: QualifiedCollection,
        /// Named vector field. Empty string = default (unnamed) field.
        field_name: String,
    },

    /// Force-seal the growing segment, triggering background HNSW build.
    Seal {
        collection: QualifiedCollection,
        /// Named vector field. Empty string = default field.
        field_name: String,
    },

    /// Force tombstone compaction on sealed segments.
    CompactIndex {
        collection: QualifiedCollection,
        /// Named vector field. Empty string = default field.
        field_name: String,
    },

    /// Rebuild sealed segments with new HNSW parameters.
    /// Old index serves queries until rebuild completes, then swaps atomically.
    Rebuild {
        collection: QualifiedCollection,
        /// Named vector field. Empty string = default field.
        field_name: String,
        /// New M parameter. 0 = keep current.
        m: usize,
        /// New M0 parameter. 0 = keep current.
        m0: usize,
        /// New ef_construction. 0 = keep current.
        ef_construction: usize,
    },

    /// Insert a sparse vector into the inverted index.
    SparseInsert {
        collection: QualifiedCollection,
        /// Named sparse vector field.
        field_name: String,
        /// Document ID to associate with this sparse vector.
        doc_id: String,
        /// Sparse vector entries as `(dimension, weight)` pairs.
        entries: Vec<(u32, f32)>,
    },

    /// Search the sparse inverted index via dot-product scoring.
    SparseSearch {
        collection: QualifiedCollection,
        /// Named sparse vector field.
        field_name: String,
        /// Query sparse vector entries.
        query_entries: Vec<(u32, f32)>,
        /// Maximum results to return.
        top_k: usize,
    },

    /// Delete a document from the sparse inverted index.
    SparseDelete {
        collection: QualifiedCollection,
        /// Named sparse vector field.
        field_name: String,
        /// Document ID to remove.
        doc_id: String,
    },

    /// Insert multiple vectors for a single document (ColBERT-style).
    /// All vectors are inserted as separate HNSW nodes sharing the
    /// same `document_surrogate`.
    MultiVectorInsert {
        collection: QualifiedCollection,
        /// Named vector field. Empty = default.
        field_name: String,
        /// Surrogate shared by all vectors of the document.
        document_surrogate: Surrogate,
        /// Flat vector data: count × dim f32 values.
        vectors: Vec<f32>,
        /// Number of vectors.
        count: usize,
        /// Dimensionality of each vector.
        dim: usize,
    },

    /// Delete all vectors for a document from the multi-vector index.
    MultiVectorDelete {
        collection: QualifiedCollection,
        /// Named vector field. Empty = default.
        field_name: String,
        /// Document surrogate whose vectors should be tombstoned.
        document_surrogate: Surrogate,
    },

    /// Search with multi-vector aggregated scoring (MaxSim, AvgSim, SumSim).
    /// Over-fetches from HNSW, groups by doc_id, aggregates, deduplicates.
    MultiVectorScoreSearch {
        collection: QualifiedCollection,
        /// Named vector field. Empty = default.
        field_name: String,
        /// Query vector.
        query_vector: Vec<f32>,
        /// Maximum documents to return.
        top_k: usize,
        /// HNSW ef_search override. 0 = auto.
        ef_search: usize,
        /// Aggregation mode: "max_sim", "avg_sim", "sum_sim".
        mode: String,
    },

    /// Direct vector upsert for vector-primary collections: an existing
    /// primary key is replaced whole, or patched by `on_conflict_updates`.
    ///
    /// Bypasses MessagePack document encoding — the Data Plane binds the
    /// vector to an HNSW node, updates the payload bitmap indexes from
    /// `payload`, and stores `payload` as the row's sidecar. No
    /// full-document blob is written.
    ///
    /// Ordering invariant (enforced by the handler):
    ///   1. Validate dim and storage dtype.
    ///   2. Decode `payload` bytes and probe the surrogate.
    ///   3. Remove the old node, bitmap entries, and sidecar when one exists.
    ///   4. Insert into HNSW (surrogate bound) and update the bitmap indexes.
    ///   5. Store the sidecar; a failed store rolls step 4 back.
    DirectUpsert {
        collection: QualifiedCollection,
        /// Vector column name. Used to compute the vector index key so the
        /// SELECT path (which keys by `(tid, collection, field)`) finds the
        /// same index this insert wrote into.
        field: String,
        /// Global surrogate the Control Plane bound to `pk_bytes`.
        surrogate: Surrogate,
        /// UTF-8 of the declared primary-key value. Followers bind the
        /// leader-assigned surrogate to this exact key.
        pk_bytes: Vec<u8>,
        /// FP32 vector values.
        vector: Vec<f32>,
        /// Pre-encoded MessagePack of every non-vector column the statement
        /// supplied. Empty when the row carried none.
        ///
        /// Only the fields named in `payload_indexes` become bitmap-indexed
        /// downstream; the rest travel here so the whole row image is available
        /// to whatever has to decide it — a row-level-security write policy
        /// among them.
        payload: Vec<u8>,
        /// Collection-level quantization. Applied via `set_quantization` on
        /// first insert so subsequent seals trigger codec-dispatch rebuilds
        /// against the configured codec (RaBitQ / BBQ).
        quantization: nodedb_types::VectorQuantization,
        /// Native storage dtype for vector values (F32 / F16 / BF16). The
        /// executor uses this to decide whether to transcode incoming f32
        /// components before writing them into the HNSW index backing store.
        storage_dtype: nodedb_types::VectorStorageDtype,
        /// Payload field bitmap indexes (name + kind). Registered via
        /// `payload.add_index` on the first insert into a new collection.
        payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
        /// When `Some`, return the STORED post-image of the upserted row — the
        /// metadata sidecar as it exists after the handler wrote it, decoded
        /// through the same normalizer the `SELECT` scan uses, so the returned
        /// row renders identically to a later read. Never the submitted values:
        /// the sidecar is `zerompk` TAGGED bytes and echoing the request would
        /// report a shape no read of the collection ever produces.
        ///
        /// The vector goes to HNSW, every other column to the sidecar, and
        /// there is no companion document write to carry the clause instead.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the row `returning` emits. A vector-primary
        /// write's policy admission happens Control-Plane-side against the
        /// `payload` image (`RlsCtx::admit_write_value_map_image`), which
        /// decides whether the write happens at all; this bounds what may be
        /// shown back, so a `RETURNING` row never exceeds a `SELECT` by the
        /// same principal.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// `ON CONFLICT (pk) DO UPDATE SET` assignments applied to the stored
        /// payload when the key exists. Empty means whole-row replace.
        #[serde(default)]
        on_conflict_updates: Vec<(String, UpdateValue)>,
        /// Write policy for the merged post-image an `on_conflict_updates`
        /// patch produces; the image exists only on the Data Plane. Decided
        /// Control-Plane-side when the patch is empty.
        rls_write_check: nodedb_types::RlsWriteCheck,
    },

    /// Vector-primary `INSERT`: an existing primary key raises
    /// `unique_violation`. Same fields as [`VectorOp::DirectUpsert`] minus
    /// the conflict patch.
    DirectInsert {
        collection: QualifiedCollection,
        field: String,
        surrogate: Surrogate,
        pk_bytes: Vec<u8>,
        vector: Vec<f32>,
        payload: Vec<u8>,
        quantization: nodedb_types::VectorQuantization,
        storage_dtype: nodedb_types::VectorStorageDtype,
        payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
        #[serde(default)]
        returning: Option<ReturningSpec>,
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Vector-primary `INSERT ... ON CONFLICT DO NOTHING`: an existing
    /// primary key leaves the stored row alone and reports zero rows.
    DirectInsertIfAbsent {
        collection: QualifiedCollection,
        field: String,
        surrogate: Surrogate,
        pk_bytes: Vec<u8>,
        vector: Vec<f32>,
        payload: Vec<u8>,
        quantization: nodedb_types::VectorQuantization,
        storage_dtype: nodedb_types::VectorStorageDtype,
        payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
        #[serde(default)]
        returning: Option<ReturningSpec>,
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Vector-primary `DELETE`. Removes the HNSW node, its payload bitmap
    /// entries, and the payload sidecar row of every targeted surrogate.
    /// Reports the number of rows that existed.
    DirectDelete {
        collection: QualifiedCollection,
        /// Vector column name; keys the HNSW index.
        field: String,
        targets: VectorWriteTargets,
        /// When `Some`, return the removed rows' sidecar images.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy decided against each removed row's sidecar image.
        rls_write_check: nodedb_types::RlsWriteCheck,
    },

    /// Vector-primary `UPDATE`. A `new_vector` rebuilds the HNSW node under
    /// the same surrogate; `payload_patch` merges into the sidecar and moves
    /// the payload bitmap entries. Reports the number of rows that existed.
    DirectUpdate {
        collection: QualifiedCollection,
        /// Vector column name; keys the HNSW index.
        field: String,
        targets: VectorWriteTargets,
        /// Replacement vector, when the statement assigns the vector column.
        new_vector: Option<Vec<f32>>,
        /// Assignments to non-vector columns, applied to the stored payload.
        payload_patch: Vec<(String, UpdateValue)>,
        quantization: nodedb_types::VectorQuantization,
        storage_dtype: nodedb_types::VectorStorageDtype,
        payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
        /// When `Some`, return the rows' stored post-images.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy decided against each row's post-image.
        rls_write_check: nodedb_types::RlsWriteCheck,
    },

    // ── Resolve-before-propose (governed vector-primary writes) ─────────
    /// Read-only: report every row mutation the wrapped vector-primary write
    /// would apply, and the response payload it would return, without
    /// applying anything.
    ///
    /// Wraps a [`VectorOp::DirectDelete`], [`VectorOp::DirectUpdate`], or
    /// [`VectorOp::DirectUpsert`] verbatim, live write predicate included —
    /// the handler resolves the targets, reads each row's sidecar, computes
    /// the post-image, and decides the policy here, where the writing
    /// identity is still available. A follower has none, so the predicate
    /// can never cross the Raft wire.
    ResolveDirectWrite(Box<VectorOp>),

    /// Apply exactly the mutations a [`VectorOp::ResolveDirectWrite`]
    /// reported, then return `response_payload` verbatim.
    ///
    /// No predicate is evaluated and no image is recomputed: the Control
    /// Plane already decided both, and `rls_write_check` carries the verdict
    /// (`RlsWriteCheck::DecidedEarlierInRequest`). Every mutation's pre-image
    /// is checked against the stored sidecar before the first one applies,
    /// so a resolution that drifted under a concurrent write applies nothing
    /// and asks for a retry.
    ResolvedDirectWrite {
        collection: QualifiedCollection,
        /// Vector column name; keys the HNSW index.
        field: String,
        /// Index settings a first write into a new index registers; an
        /// `Upsert` mutation into an empty collection creates the index.
        quantization: nodedb_types::VectorQuantization,
        storage_dtype: nodedb_types::VectorStorageDtype,
        payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
        mutations: Vec<VectorResolvedMutation>,
        /// The statement's reply, decided while resolving. Applying nodes
        /// return it unchanged, so leader and follower report the same thing.
        response_payload: Vec<u8>,
        rls_write_check: nodedb_types::RlsWriteCheck,
    },
}
