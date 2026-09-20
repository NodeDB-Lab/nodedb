// SPDX-License-Identifier: Apache-2.0

//! The CRDT operation enum — the wire shape and nothing else.

use nodedb_types::{QualifiedCollection, Surrogate};

use crate::physical_plan::document::ReturningSpec;

use super::write_verb::CrdtWriteVerb;

/// CRDT engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CrdtOp {
    /// CRDT state read for a document.
    Read {
        collection: QualifiedCollection,
        document_id: String,
    },

    /// CRDT delta application (write path).
    ///
    /// Binds the user-visible `document_id` to a stable cross-engine
    /// `Surrogate`. UPSERT-aware: if the document already has a surrogate,
    /// the assigner returns the existing one. `Surrogate::ZERO` only
    /// appears in test fixtures.
    Apply {
        collection: QualifiedCollection,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        /// Per-mutation unique ID for deduplication and compensation tracking.
        mutation_id: u64,
        /// Stable cross-engine identity for the document this delta targets.
        surrogate: Surrogate,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
        /// The collection's constraint-set version (`constraint_version`)
        /// that this delta was admitted against. Stamped at admission
        /// (Control Plane, which can read the catalog) so the apply-time
        /// write-gate can reject a delta that reaches a replica before that
        /// replica has installed the matching constraint version. `0` means
        /// "no fence" (gate open): no constraints installed, or the
        /// construction site does not admit against a catalog record.
        #[serde(default)]
        constraint_version_required: u64,
        /// Optional preview fence. `None` preserves legacy/replay behavior.
        #[serde(default)]
        expected_frontier_digest: Option<[u8; 32]>,
    },

    /// External sync delta carrying server-derived identity and catalog-owned
    /// signing enforcement. Kept distinct from trusted/internal `Apply` so a
    /// transport cannot accidentally omit the signing admission fields.
    ApplyAuthenticated {
        collection: QualifiedCollection,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        mutation_id: u64,
        surrogate: Surrogate,
        provenance: nodedb_types::sync::wire::SyncProvenance,
        constraint_version_required: u64,
        expected_frontier_digest: Option<[u8; 32]>,
        auth_user_id: u64,
        auth_device_id: u64,
        auth_seq_no: u64,
        delta_signature: [u8; 32],
        signing_required: bool,
    },

    /// Import a per-collection Loro snapshot into the tenant CRDT engine.
    ///
    /// Used by the durable RESTORE re-issue path: the snapshot is replicated
    /// to every Raft replica of a data group, and each replica calls
    /// `import_snapshot_bytes` (a monotonic, idempotent, commutative Loro
    /// merge — deterministic across replicas). Carries no surrogate or
    /// provenance: it is a collection-doc import, not a per-document op.
    ImportSnapshot {
        tenant_id: u64,
        collection: QualifiedCollection,
        bytes: Vec<u8>,
    },

    /// Install a collection's constraint set into every replica's CRDT
    /// validator. Carried opaquely as zerompk-encoded constraint blobs so this
    /// crate stays decoupled from the constraint wire layout (mirroring the
    /// opaque payload precedent of other replicated ops). Decoded into typed
    /// constraints inside the Data Plane handler. Applied deterministically on
    /// every replica from the per-vshard data Raft log.
    /// `constraint_version` fences the install: a replica applies only when it
    /// is `>=` the version last installed for the collection, so a stale set
    /// cannot clobber a newer one regardless of data-log apply order.
    SetConstraints {
        collection: QualifiedCollection,
        constraint_version: u64,
        constraints: Vec<Vec<u8>>,
    },

    /// Remove every constraint scoped to `collection` from the CRDT validator.
    /// `constraint_version` fences the drop identically to `SetConstraints`.
    DropConstraints {
        collection: QualifiedCollection,
        constraint_version: u64,
    },

    /// Read the constraint set currently installed in this replica's per-core
    /// CRDT validator for `collection`. Read-only observability op: returns the
    /// installed `Vec<Constraint>` zerompk-encoded in the response payload. It
    /// is never replicated or logged — it is constructed directly against a
    /// single node's data core to inspect that replica's validator state.
    ReadConstraints { collection: QualifiedCollection },

    /// Set conflict resolution policy for a CRDT collection (DDL).
    SetPolicy {
        collection: QualifiedCollection,
        /// JSON-serialized `CollectionPolicy` from nodedb-crdt.
        policy_json: String,
    },

    /// Read the current conflict resolution policy for a CRDT collection.
    /// Returns the JSON-serialized `CollectionPolicy`, falling back to the
    /// ephemeral default when no explicit policy has been registered.
    GetPolicy { collection: QualifiedCollection },

    /// Read a document at a specific historical version.
    /// Returns the document state as JSON bytes.
    ReadAtVersion {
        collection: QualifiedCollection,
        document_id: String,
        /// JSON-serialized `HashMap<String, i64>` of {peer_id_hex: counter}.
        version_vector_json: String,
    },

    /// Get the current oplog version vector for a tenant's CRDT state.
    /// Returns version vector as JSON string.
    GetVersionVector { collection: QualifiedCollection },

    /// Export oplog delta from a version to current.
    /// Returns raw Loro delta bytes.
    ExportDelta {
        collection: QualifiedCollection,
        /// JSON-serialized version vector to start from.
        from_version_json: String,
    },

    /// Restore a document to a historical version (forward mutation).
    /// Returns the delta bytes for the restore operation.
    RestoreToVersion {
        collection: QualifiedCollection,
        document_id: String,
        /// JSON-serialized version vector of the target version.
        target_version_json: String,
        /// Stable cross-engine identity for the document being restored.
        surrogate: Surrogate,
    },

    /// Compact history at a specific version.
    CompactAtVersion {
        collection: QualifiedCollection,
        /// JSON-serialized version vector. Oplog before this is discarded.
        target_version_json: String,
    },

    // ─── Block Document (LoroList) Operations ───────────────────────
    /// Insert a block (LoroMap) into a document's block list.
    /// `fields_json` contains the block's fields as a JSON object.
    ///
    /// The inserted list element is a sub-document of the existing CRDT
    /// document — it does not allocate a new top-level surrogate. The
    /// `surrogate` field carries the parent document's surrogate.
    ListInsert {
        collection: QualifiedCollection,
        document_id: String,
        list_path: String,
        index: usize,
        fields_json: String,
        /// Surrogate of the parent document hosting this block list.
        surrogate: Surrogate,
    },

    /// Delete a block from a document's block list.
    ListDelete {
        collection: QualifiedCollection,
        document_id: String,
        list_path: String,
        index: usize,
        /// Surrogate of the parent document hosting this block list.
        surrogate: Surrogate,
    },

    /// Move a block within a document's block list (reorder).
    ListMove {
        collection: QualifiedCollection,
        document_id: String,
        list_path: String,
        from_index: usize,
        to_index: usize,
        /// Surrogate of the parent document hosting this block list.
        surrogate: Surrogate,
    },

    // ─── Document row (top-level LoroMap) field-carrying ops ─────────
    /// Insert-or-replace / partial-update a document row's scalar fields,
    /// server-built from `fields_json` (a JSON object). Mirrors the
    /// `ListInsert` intent-carrying contract: the Control Plane has no
    /// `LoroDoc`, so the Data Plane handler builds the Loro mutation.
    ///
    /// `partial = false`: INSERT / full replace — scalar keys absent from
    /// `fields_json` are pruned (LWW full-projection replace).
    /// `partial = true`: UPDATE SET — only the provided fields are written
    /// (LWW-per-field), untouched keys survive.
    ///
    /// `surrogate` is this row's OWN stable top-level cross-engine identity.
    DocUpsert {
        collection: QualifiedCollection,
        document_id: String,
        fields_json: String,
        surrogate: Surrogate,
        partial: bool,
        /// The SQL statement that produced this write; decides the command tag.
        verb: CrdtWriteVerb,
        /// When `Some`, return the STORED post-image of the upserted row —
        /// projected per spec. Carried across replication so a replay
        /// re-executes this write for the originating request, not just for
        /// the follower's own state.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// RLS read filters gating the rows `returning` emits. The write is
        /// governed by the write policy; this bounds what the statement may
        /// show back, so a `RETURNING` row set never exceeds what a `SELECT`
        /// by the same principal would return. Response-only, like
        /// `returning` — empty means no policy applies.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    /// Delete a document row: tombstone in the collection's Loro doc + remove
    /// from the sparse document store.
    DocDelete {
        collection: QualifiedCollection,
        document_id: String,
        surrogate: Surrogate,
        /// When `Some`, return the STORED pre-image of the deleted row —
        /// projected per spec. Carried across replication — see
        /// `DocUpsert::returning`.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// RLS read filters gating the rows `returning` emits — same contract
        /// as `DocUpsert::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Bounded, read-only preview of a CRDT delta against its target state.
    ///
    /// Appended to preserve every existing enum discriminator in durable WAL
    /// and replication records.
    PreviewApply {
        collection: QualifiedCollection,
        document_id: String,
        delta: Vec<u8>,
    },
}
