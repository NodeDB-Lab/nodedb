// SPDX-License-Identifier: Apache-2.0

// Library code propagates errors; tests may unwrap.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

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
pub mod array_agg;
pub mod array_cell;
pub mod array_route;
pub mod ascii;
pub mod backup_envelope;
pub mod bbox;
pub mod bytea_text;
pub mod calvin;
pub mod calvin_passive;
pub mod clone;
pub mod collection;
pub mod collection_config;
pub mod columnar;
pub mod conversion;
pub mod crdt_preview;
pub mod datetime;
pub mod decimal_float;
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
pub mod msgpack_span;
pub mod multi_vector;
pub mod namespace;
pub mod numeric_cmp;
pub mod object_msgpack;
pub mod path_component;
pub mod pg_compat;
pub mod protocol;
pub mod result;
pub mod returning;
pub mod rls_write_check;
pub mod row_identity;
pub mod row_limits;
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
pub mod wall_clock;
pub mod wire_bytes;
pub mod wire_version;

pub use approx::{CountMinSketch, HyperLogLog, SpaceSaving, TDigest};
pub use array_cell::ArrayCell;
pub use array_route::ShardBucket;
pub use ascii::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from, rfind_ascii_case_insensitive,
    starts_with_ascii_case_insensitive, strip_prefix_ascii_case_insensitive,
};
pub use audit_dml::AuditDmlMode;
pub use bbox::{BboxSpace, BoundingBox, geometry_bbox, geometry_bbox_in};
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
    CollectionId, CollectionKey, CollectionKeyError, DatabaseId, DocumentId, EdgeId,
    EdgeIdParseError, IdError, IdType, NodeId, QualifiedCollection, ShapeId, TenantId,
};
pub use identity::KeyRepr;
pub use json_msgpack::{
    InstantKind, JsonValue, MsgpackError, MsgpackResult, NATIVE_CELL_CONSTANT,
    NATIVE_CELL_GROWTH_DEN, NATIVE_CELL_GROWTH_NUM, NativeCell, NonStringKey,
    find_body_non_string_key, find_non_string_key, json_from_msgpack, json_to_msgpack,
    json_to_msgpack_or_empty, msgpack_to_json_string, native_cell_bound, ordered_map_to_msgpack,
    plain_msgpack_keeps_type, read_instant, value_from_msgpack, value_to_document_body,
    value_to_msgpack, value_to_typed_msgpack, write_instant,
};
pub use kv::{KV_DEFAULT_INLINE_THRESHOLD, KvConfig, KvTtlPolicy, is_valid_kv_key_type};
pub use lsn::Lsn;
pub use mirror::{
    MirrorCutAdvance, MirrorCutMark, MirrorCutState, MirrorEntryStamp, MirrorEpochRange,
    MirrorFailure, MirrorGroupPosition, MirrorMode, MirrorOrigin, MirrorPin, MirrorPinState,
    MirrorStatus, MirrorStatusStep,
};
pub use multi_vector::{MultiVector, MultiVectorError, MultiVectorScoreMode};
pub use namespace::Namespace;
pub use object_msgpack::{ObjectFormat, ObjectView, decode_object};
pub use path_component::{hashed_file_stem, is_plain_path_component};
pub use quota::{
    PriorityClass, PriorityClassParseError, QuotaRecord, QuotaSpec, QuotaValidationError,
};
pub use result::{QueryResult, SearchResult, SubGraph};
pub use returning::{ReturningColumns, ReturningItem, ReturningSpec};
pub use rls_write_check::{RlsWriteCheck, WriteGateDecision};
pub use row_identity::{
    DEFAULT_IDENTITY_COLUMN, HEADLESS_SENTINEL_PREFIX, ROWID_COLUMN, RowIdentity, StorageKey,
    declared_key, extract_pk_value, value_to_pk_string,
};
pub use row_limits::{
    MAX_ADDED_COLUMN_IMAGE_BYTES, MAX_COLLECTION_NAME_BYTES, MAX_OLD_EVENT_IMAGE_BYTES,
    MAX_ROW_IMAGE_BYTES, MAX_SCHEMA_WITNESS_BYTES, added_column_image_bytes,
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
    BitemporalFilter, BitemporalInterval, LsnTimeAnchor, LsnTimeError, LsnTimeMap,
    MAX_POSITIONS_PER_EPOCH, NANOS_PER_MS, OPEN_UPPER, OrdinalClock, SystemTimeScope,
    ValidTimePredicate, calvin_txn_ordinal, ms_to_ordinal_upper, ordinal_to_ms,
};
pub use text_search::{Bm25Params, QueryMode, TextColumnFault, TextSearchParams};
pub use trace::{SpanId, TraceId};
pub use typeguard::TypeGuardFieldDef;
pub use value::{NotScalar, Value, scalar_to_raw_bytes};
pub use vector_ann::{VectorAnnOptions, VectorQuantization};
pub use vector_dtype::VectorStorageDtype;
pub use vector_index_params::StoredVectorIndexParams;
pub use vector_index_stats::{
    VectorIndexQuantization, VectorIndexStats, VectorIndexType, VectorIvfStats,
};
pub use vector_model::{VectorModelEntry, VectorModelMetadata};
pub use volatility::Volatility;
pub use wall_clock::{checked_wall_now_ms, wall_now_ns, wall_now_ns_i64};
pub use wire_bytes::WireBytes;
