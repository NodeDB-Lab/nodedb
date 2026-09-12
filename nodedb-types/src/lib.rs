// SPDX-License-Identifier: Apache-2.0

//! Shared NodeDB type definitions: strong IDs (TenantId, VShardId, RequestId,
//! Lsn, Surrogate), error types (NodeDbError + ErrorCode + ErrorDetails), wire
//! types for cross-plane and cross-node messages, value types, configuration
//! schemas, and the approximate-aggregate sketches.
//!
//! Every NodeDB workspace crate depends on this. Items here form the stable
//! cross-crate vocabulary; types specific to one engine live in that engine's
//! crate.

pub mod audit_dml;
pub mod config;
pub mod quota;

pub mod approx;
pub mod array_cell;
pub mod ascii;
pub mod backup_envelope;
pub mod bbox;
pub mod calvin;
pub mod clone;
pub mod collection;
pub mod collection_config;
pub mod columnar;
pub mod conversion;
pub mod crdt_preview;
pub mod datetime;
pub mod decode_bounds;
pub mod diagnostic;
pub mod document;
pub mod dropped_collection;
pub mod error;
pub mod fail_point;
pub mod filter;
pub mod geometry;
pub mod graph;
pub mod hlc;
pub mod hnsw;
pub mod id;
pub mod id_gen;
pub mod identity;
pub mod json_msgpack;
pub mod kv;
pub mod kv_parsing;
pub mod lsn;
pub mod mirror;
pub mod multi_vector;
pub mod namespace;
pub mod path_component;
pub mod pg_compat;
pub mod protocol;
pub mod result;
pub mod rls_write_check;
pub mod row_identity;
pub mod sparse_vector;
pub mod sql_quote;
pub mod surrogate;
pub mod surrogate_bitmap;
pub mod sync;
pub mod temporal;
pub mod text_search;
pub mod timeseries;
pub mod trace;
pub mod typeguard;
pub mod value;
pub mod vector_ann;
pub mod vector_distance;
pub mod vector_dtype;
pub mod vector_index_params;
pub mod vector_index_stats;
pub mod vector_model;
pub mod volatility;
pub mod wire_version;

pub use approx::{CountMinSketch, HyperLogLog, SpaceSaving, TDigest};
pub use array_cell::ArrayCell;
pub use ascii::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from, rfind_ascii_case_insensitive,
    starts_with_ascii_case_insensitive, strip_prefix_ascii_case_insensitive,
};
pub use audit_dml::AuditDmlMode;
pub use bbox::{BoundingBox, geometry_bbox};
pub use clone::{CloneOrigin, CloneStatus, MAX_CLONE_DEPTH};
pub use collection::{CollectionType, CollectionTypeParseError};
pub use collection_config::{
    KeySpec, PartitionStrategy, PayloadAtom, PayloadIndexKind, PrimaryEngine, VectorPrimaryConfig,
};
pub use columnar::{
    ColumnDef, ColumnType, ColumnarProfile, ColumnarSchema, DocumentMode, SchemaError, StrictSchema,
};
pub use config::TuningConfig;
pub use crdt_preview::CrdtPreviewResult;
pub use datetime::{NdbDateTime, NdbDateTimeError, NdbDuration};
pub use document::Document;
pub use dropped_collection::DroppedCollection;
pub use error::NodeDbError;
pub use filter::{EdgeFilter, MetadataFilter};
pub use graph::{Direction, GraphStats};
pub use hlc::{ClockSkew, Hlc, HlcClock, MAX_CLOCK_SKEW_NS};
pub use hnsw::{HnswCheckpoint, HnswNodeSnapshot, HnswParams};
pub use id::{
    CollectionId, DatabaseId, DocumentId, EdgeId, EdgeIdParseError, IdError, IdType, NodeId,
    QualifiedCollection, ShapeId, TenantId,
};
pub use identity::KeyRepr;
pub use json_msgpack::{
    JsonValue, MsgpackError, MsgpackResult, json_from_msgpack, json_to_msgpack,
    json_to_msgpack_or_empty, msgpack_to_json_string, value_from_msgpack, value_to_msgpack,
};
pub use kv::{KV_DEFAULT_INLINE_THRESHOLD, KvConfig, KvTtlPolicy, is_valid_kv_key_type};
pub use lsn::Lsn;
pub use mirror::{MirrorLagRecord, MirrorMode, MirrorOrigin, MirrorStatus};
pub use multi_vector::{MultiVector, MultiVectorError, MultiVectorScoreMode};
pub use namespace::Namespace;
pub use path_component::is_plain_path_component;
pub use quota::{
    PriorityClass, PriorityClassParseError, QuotaRecord, QuotaSpec, QuotaValidationError,
};
pub use result::{QueryResult, SearchResult, SubGraph};
pub use rls_write_check::{RlsWriteCheck, WriteGateDecision};
pub use row_identity::{
    DEFAULT_IDENTITY_COLUMN, HEADLESS_SENTINEL_PREFIX, RowIdentity, StorageKey,
    doc_id_to_surrogate, extract_pk_value, identity_of, surrogate_to_doc_id, value_to_pk_string,
};
pub use sparse_vector::{SparseVector, SparseVectorError};
pub use sql_quote::{quote_ident, quote_literal};
pub use surrogate::Surrogate;
pub use surrogate_bitmap::SurrogateBitmap;
pub use sync::compensation::CompensationHint;
pub use sync::shape::{ShapeDefinition, ShapeType};
pub use sync::violation::ViolationType;
pub use sync::wire::{SyncFrame, SyncMessageType};
pub use temporal::{
    BitemporalFilter, BitemporalInterval, LsnMapError, LsnMsAnchor, LsnMsMap, NANOS_PER_MS,
    OPEN_UPPER, OrdinalClock, SystemTimeScope, ValidTimePredicate, lsn_to_ms, ms_to_ordinal_upper,
    ordinal_to_ms,
};
pub use text_search::{Bm25Params, QueryMode, TextSearchParams};
pub use trace::{SpanId, TraceId};
pub use typeguard::TypeGuardFieldDef;
pub use value::Value;
pub use vector_ann::{VectorAnnOptions, VectorQuantization};
pub use vector_dtype::VectorStorageDtype;
pub use vector_index_params::StoredVectorIndexParams;
pub use vector_index_stats::{VectorIndexQuantization, VectorIndexStats, VectorIndexType};
pub use vector_model::{VectorModelEntry, VectorModelMetadata};
pub use volatility::Volatility;
