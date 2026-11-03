// SPDX-License-Identifier: Apache-2.0

//! The `NodeDb` trait: one query interface for Origin and Lite.
//!
//! Application code writes against this trait once. Lite runs each call on
//! in-process engines. The native and pgwire clients send it to Origin. All
//! methods are `async`: Tokio on native targets, `wasm-bindgen-futures` on
//! WASM.
//!
//! `NodeDb` holds every operation but the graph ones, which live in its
//! supertrait [`NodeDbGraph`]. An implementation is one `impl` block per
//! trait. Long provided-method bodies live in `default_impls`.
//!
//! Each method states its contract first. The `On Lite` / `On Remote` /
//! `On Native` lines name what each backend runs. `Remote` is the pgwire
//! client. `Native` is the MessagePack client, the canonical typed
//! transport. "Key column" is a collection's declared primary key, else `id`.

use std::collections::HashSet;

use async_trait::async_trait;

use nodedb_types::{
    document::Document, dropped_collection::DroppedCollection, error::NodeDbResult,
    filter::MetadataFilter, protocol::ArrayCellWrite, protocol::Limits, result::QueryResult,
    result::SearchResult, text_search::TextSearchParams, value::Value,
};

use super::default_impls;
use super::graph_def::NodeDbGraph;
use crate::traits::document::CollectionPurgedHandler;

/// Unified database interface for NodeDB.
///
/// Implementations:
/// - `NodeDbLite`: in-process HNSW/CSR/Loro engines on the edge device.
///   Writes produce CRDT deltas synced to Origin in the background.
/// - `NativeClient`: the MessagePack protocol to Origin.
/// - `NodeDbRemote`: SQL over pgwire to Origin.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait NodeDb: NodeDbGraph {
    /// Search for the `k` nearest vectors to `query` in `collection`.
    ///
    /// Results are ordered by ascending distance. `filter` constrains which
    /// vectors are considered. `Some(allowed_ids)` applies before ranking on
    /// every implementation: the top-k comes from the allowed set even when
    /// nearer vectors lie outside it. An empty set returns no result.
    ///
    /// On Lite: in-memory HNSW search with the id prefilter pushed into
    /// traversal.
    /// On Remote: `SELECT id, vector_distance(..) AS distance FROM <collection>
    /// [WHERE <filter>] [AND <key column> IN (..)] ORDER BY vector_distance(..)
    /// LIMIT <k>`. The server lowers the key restriction to a candidate bitmap
    /// the index search honors.
    /// On Native: the ids travel in the request. The server lowers them to
    /// the same candidate bitmap.
    async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
        allowed_ids: Option<&HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>>;

    /// Insert a vector with optional metadata into `collection`.
    ///
    /// On Lite: HNSW insert + CRDT delta + persist to local storage.
    /// On Remote: `INSERT INTO collection (id, embedding, metadata) VALUES (...)`.
    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()>;

    /// Delete a vector by ID from `collection`.
    ///
    /// On Lite: marks deleted in HNSW + emits CRDT tombstone.
    /// On Remote: `DELETE FROM collection WHERE id = $1`.
    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()>;

    /// Get a document by ID from `collection`.
    ///
    /// A missing document is `Ok(None)`. An error means the read failed.
    /// Every field reads back with its stored type, on every implementation.
    ///
    /// On Lite: direct Loro state read.
    /// On Remote: `SELECT to_jsonb(*) AS document FROM <collection> WHERE
    /// <key column> = '<id>'` on the simple-query protocol. The row object
    /// decodes field by field to the values the native client returns.
    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>>;

    /// Put (insert or replace) a document into `collection`.
    ///
    /// The document's `id` is the key. An existing document with that id is
    /// replaced whole (last-writer-wins locally, CRDT merge on sync).
    ///
    /// Each field is a top-level column. The key column holds the document
    /// id. A field under the key column must equal the document id. A
    /// declared key column reads back as a field. The implicit `id` does not.
    ///
    /// On Lite: Loro apply + CRDT delta + local storage.
    /// On Remote: `DELETE` then `INSERT INTO <collection> (<key column>,
    /// <fields>) VALUES (...)`, every value a bound parameter. The pair is
    /// atomic. Inside the caller's transaction block it runs under a
    /// savepoint and never ends that block. A failed put rolls back to the
    /// savepoint and leaves the block usable. Outside a block it runs in its
    /// own transaction.
    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()>;

    /// Delete a document by ID from `collection`.
    ///
    /// On Lite: Loro delete + CRDT tombstone.
    /// On Remote: `DELETE FROM <collection> WHERE <key column> = $1`.
    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()>;

    /// Put a document and insert its embedding under one CRDT lock acquisition.
    ///
    /// Equivalent to `document_put` then `vector_insert` into
    /// `vector_collection`. An empty `embedding` makes it `document_put`.
    /// The default makes the two calls. Implementations that can batch both
    /// under one lock override it.
    async fn document_put_with_vector(
        &self,
        doc_collection: &str,
        doc: Document,
        vector_collection: &str,
        id: &str,
        embedding: &[f32],
    ) -> NodeDbResult<()> {
        default_impls::document_put_with_vector_default(
            self,
            doc_collection,
            doc,
            vector_collection,
            id,
            embedding,
        )
        .await
    }

    /// Read a document as of a system time, optionally at a valid time.
    ///
    /// Only valid on collections created `WITH (bitemporal=true)`; a plain
    /// collection returns an error. `as_of_ms: None` reads the live version.
    /// `Some(t)` reads the version visible at system time `t`.
    /// `valid_time_ms: Some(vt)` also requires
    /// `valid_from_ms <= vt < valid_until_ms`.
    ///
    /// Implementations without bitemporal reads return `Err`.
    async fn document_get_as_of(
        &self,
        collection: &str,
        id: &str,
        as_of_ms: Option<i64>,
        valid_time_ms: Option<i64>,
    ) -> NodeDbResult<Option<Document>> {
        default_impls::document_get_as_of_default(collection, id, as_of_ms, valid_time_ms)
    }

    /// Put a document with explicit valid-time bounds.
    ///
    /// Only valid on collections created `WITH (bitemporal=true)`.
    /// `valid_from_ms` defaults to the system time, `valid_until_ms` to
    /// open-ended. Implementations without bitemporal writes return `Err`.
    async fn document_put_with_valid_time(
        &self,
        collection: &str,
        doc: Document,
        valid_from_ms: Option<i64>,
        valid_until_ms: Option<i64>,
    ) -> NodeDbResult<()> {
        default_impls::document_put_with_valid_time_default(
            collection,
            doc,
            valid_from_ms,
            valid_until_ms,
        )
    }

    /// Insert a new block into a document's movable list at `index`.
    ///
    /// `list_path` addresses the list (dot-path for nested containers).
    /// `fields` must be `Value::Object(..)`. Each key becomes a field of the
    /// new map block. `index` is required: a defaulted position corrupts
    /// list order.
    ///
    /// On Lite: `nodedb_crdt::list_ops::list_insert_container` + CRDT delta.
    /// On Native: `OpCode::CrdtListInsert` with `list_path` / `list_index` /
    /// `list_fields_json`.
    async fn list_insert(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
        fields: &Value,
    ) -> NodeDbResult<()>;

    /// Delete the block at `index` from a document's movable list.
    ///
    /// On Lite: removes the local entry + CRDT tombstone delta.
    /// On Native: `OpCode::CrdtListDelete` with `list_path` / `list_index`.
    async fn list_delete(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> NodeDbResult<()>;

    /// Move the block at `from_index` to `to_index` in a document's movable list.
    ///
    /// The two indexes are distinct wire fields. Swapping them reorders the
    /// list to the wrong shape.
    ///
    /// On Lite: repositions the local entry + CRDT delta.
    /// On Native: `OpCode::CrdtListMove` with `list_path` /
    /// `list_from_index` / `list_to_index`.
    async fn list_move(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> NodeDbResult<()>;

    /// Insert a vector into the named field `field_name` of `collection`.
    ///
    /// Each named field has its own HNSW index, so a collection holds several
    /// embeddings (e.g. `title_embedding`, `body_embedding`). The default
    /// returns `Err`: delegating to `vector_insert` would drop `field_name`
    /// and land the vector in the wrong field.
    async fn vector_insert_field(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        default_impls::vector_insert_field_default(collection, field_name, id, embedding, metadata)
    }

    /// Search the named vector field `field_name`.
    ///
    /// The default returns `Err`: delegating to `vector_search` would drop
    /// `field_name` and search the wrong field.
    async fn vector_search_field(
        &self,
        collection: &str,
        field_name: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        default_impls::vector_search_field_default(collection, field_name, query, k, filter)
    }

    /// Full-text BM25 search of the indexed `field` of `collection`.
    ///
    /// Every BM25 index covers one declared field, so the caller names it.
    /// An empty `field` searches the whole-document index. Returns ids with
    /// scores, best first, at most `top_k`. `Some(allowed_ids)` returns only
    /// those ids.
    ///
    /// `params` means the same on every implementation. `mode` combines the
    /// terms (`Or`: any, `And`: all). `fuzzy`, or the collection's fuzzy
    /// default, lets a term with no exact match fall back to a Levenshtein
    /// match. [`TextSearchParams::default`] is `{ mode: Or, fuzzy: false }`.
    ///
    /// On Lite: honors every `params` value, and applies `allowed_ids` after
    /// an over-fetch.
    /// On Remote and Native: one shared statement, `SELECT id, bm25_score(..,
    /// opts) AS score FROM <collection> WHERE text_match(.., opts) [AND <key
    /// column> IN (..)] ORDER BY score DESC LIMIT <k>`, with `opts` the
    /// non-default `mode => '..'` and `fuzzy => ..`. Both return the same ids
    /// and scores. A double-quoted query is a phrase query, which takes only
    /// the default `params`.
    async fn text_search(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        top_k: usize,
        params: TextSearchParams,
        allowed_ids: Option<&HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>>;

    /// Batch insert vectors — amortizes CRDT delta export to O(1) per batch.
    async fn batch_vector_insert(
        &self,
        collection: &str,
        vectors: &[(&str, &[f32])],
    ) -> NodeDbResult<()> {
        default_impls::batch_vector_insert_default(self, collection, vectors).await
    }

    /// Load `cells` into `array` through the bulk path. Returns the count.
    /// A cell holds one coordinate per dim and one value per attr, in schema
    /// order. Outside a block a bad cell stops the load, earlier chunks kept.
    /// On Native: `OpCode::ArrayPutBatch` requests, each under one frame.
    /// The default returns `Err`: the pgwire client does not implement it.
    async fn array_put_cells(&self, array: &str, cells: Vec<ArrayCellWrite>) -> NodeDbResult<u64> {
        default_impls::array_put_cells_default(array, cells)
    }

    /// The protocol version negotiated during the connection handshake.
    /// `0` for an implementation that performs no handshake.
    fn proto_version(&self) -> u16 {
        0
    }

    /// The raw capability bitfield advertised by the server. `0` when no
    /// handshake ran. `Capabilities::from_raw` gives named predicates.
    fn capabilities(&self) -> u64 {
        0
    }

    /// The server version string from `HelloAckFrame` (e.g. `"0.1.0-dev"`).
    /// Empty when no handshake ran.
    fn server_version(&self) -> String {
        String::new()
    }

    /// Per-operation limits announced by the server. Every field is `None`
    /// when no handshake ran. `None` means no server-side cap.
    fn limits(&self) -> Limits {
        Limits::default()
    }

    /// Execute a raw SQL query with parameters.
    ///
    /// On Lite: requires the `sql` feature. Without it, returns
    /// `NodeDbError::SqlNotEnabled`.
    /// On Remote: pass-through to Origin via pgwire.
    ///
    /// The typed methods above cover most agent workloads. Use this for BI
    /// tools, ORMs, and ad-hoc queries.
    async fn execute_sql(&self, query: &str, params: &[Value]) -> NodeDbResult<QueryResult>;

    /// Restore a soft-deleted collection within its retention window.
    ///
    /// Runs `UNDROP COLLECTION <name>` through `execute_sql`. Fails with
    /// 42P01 once the retention window has elapsed, or with 42501 when the
    /// caller is neither the preserved owner nor an admin.
    async fn undrop_collection(&self, name: &str) -> NodeDbResult<()> {
        default_impls::undrop_collection_default(self, name).await
    }

    /// Hard-delete a collection, skipping soft-delete and retention.
    ///
    /// Runs `DROP COLLECTION <name> PURGE`. Admin-only: the server rejects
    /// other callers with 42501. The data is unrecoverable.
    async fn drop_collection_purge(&self, name: &str) -> NodeDbResult<()> {
        default_impls::drop_collection_purge_default(self, name).await
    }

    /// List the current tenant's soft-deleted collections still within their
    /// retention window. Empty when there are none.
    ///
    /// Reads `tenant_id, name, owner, deactivated_at_ns,
    /// retention_expires_at_ns` from `_system.dropped_collections`.
    async fn list_dropped_collections(&self) -> NodeDbResult<Vec<DroppedCollection>> {
        default_impls::list_dropped_collections_default(self).await
    }

    /// Register a handler fired when a collection the caller synced is purged
    /// on Origin and the local copy is removed.
    ///
    /// The default returns `NodeDbError::storage` ("not supported").
    /// Implementations with a sync client (Lite) override it. A stateless
    /// pgwire client has nothing to push, so the default is correct there.
    async fn on_collection_purged(&self, _handler: CollectionPurgedHandler) -> NodeDbResult<()> {
        default_impls::on_collection_purged_default()
    }
}
