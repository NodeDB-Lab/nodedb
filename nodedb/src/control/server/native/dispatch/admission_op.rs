// SPDX-License-Identifier: BUSL-1.1

//! Maps a native protocol [`OpCode`] to the `operation` string the
//! request-admission rate limiter's cost table
//! (`control::security::ratelimit::config::default_endpoint_costs`) keys on.
//!
//! Every opcode that has a natural counterpart in the cost table maps to
//! that key so its configured cost multiplier applies. Opcodes with no
//! natural counterpart map to the closest-shaped existing key (point write,
//! scan, or search) rather than an unrecognized string, which would silently
//! fall back to the default cost of 1 — mapping to an existing key keeps the
//! fallback intentional instead of accidental.

use nodedb_types::protocol::OpCode;

/// Resolve the rate-limiter `operation` string for a native opcode.
pub(crate) fn admission_operation(op: OpCode) -> &'static str {
    match op {
        // Point reads.
        OpCode::PointGet
        | OpCode::CrdtRead
        | OpCode::KvGetTtl
        | OpCode::KvBatchGet
        | OpCode::KvFieldGet
        | OpCode::KvGetSet => "point_get",

        // Point writes.
        OpCode::PointPut
        | OpCode::PointDelete
        | OpCode::CrdtApply
        | OpCode::CrdtListInsert
        | OpCode::CrdtListDelete
        | OpCode::CrdtListMove
        | OpCode::AlterCollectionPolicy
        | OpCode::EdgePut
        | OpCode::EdgeDelete
        | OpCode::TimeseriesIngest
        | OpCode::KvExpire
        | OpCode::KvPersist
        | OpCode::KvBatchPut
        | OpCode::KvFieldSet
        | OpCode::KvIncr
        | OpCode::KvIncrFloat
        | OpCode::KvCas
        | OpCode::KvRegisterIndex
        | OpCode::KvDropIndex
        | OpCode::KvTruncate
        | OpCode::KvRegisterSortedIndex
        | OpCode::KvDropSortedIndex
        | OpCode::VectorInsert
        | OpCode::VectorSetParams
        | OpCode::VectorDelete
        | OpCode::VectorBatchInsert
        | OpCode::DocumentUpdate
        | OpCode::DocumentUpsert
        | OpCode::DocumentRegister
        | OpCode::DocumentDropIndex
        | OpCode::DocumentBatchInsert
        | OpCode::ColumnarInsert
        | OpCode::ArrayPutBatch
        | OpCode::EdgePutBatch
        | OpCode::EdgeDeleteBatch => "point_put",

        // Scans.
        OpCode::RangeScan
        | OpCode::DocumentScan
        | OpCode::DocumentBulkUpdate
        | OpCode::DocumentBulkDelete
        | OpCode::DocumentTruncate
        | OpCode::DocumentInsertSelect
        | OpCode::SpatialScan
        | OpCode::TimeseriesScan
        | OpCode::ColumnarScan
        | OpCode::RecursiveScan => "document_scan",

        // KV scans.
        OpCode::KvScan
        | OpCode::KvSortedIndexRank
        | OpCode::KvSortedIndexTopK
        | OpCode::KvSortedIndexRange
        | OpCode::KvSortedIndexCount
        | OpCode::KvSortedIndexScore => "kv_scan",

        // Vector search.
        OpCode::VectorSearch | OpCode::VectorMultiSearch => "vector_search",

        // Text search.
        OpCode::TextSearch => "text_search",

        // Hybrid / fused search.
        OpCode::HybridSearch | OpCode::GraphRagFusion => "hybrid_search",

        // Graph traversal.
        OpCode::GraphHop | OpCode::GraphNeighbors | OpCode::GraphMatch => "graph_hop",
        OpCode::GraphPath | OpCode::GraphSubgraph => "graph_path",
        OpCode::GraphAlgo => "aggregate",

        OpCode::DocumentEstimateCount => "aggregate",

        // SQL / DDL / session control have no natural cost-table counterpart;
        // the default cost of 1 applies.
        OpCode::Sql
        | OpCode::Ddl
        | OpCode::Set
        | OpCode::Show
        | OpCode::Reset
        | OpCode::Begin
        | OpCode::Commit
        | OpCode::Rollback
        | OpCode::Explain
        | OpCode::CopyFrom => "sql",

        // Auth/Ping/Status never reach this call site (handled earlier in
        // `handle_request`). `OpCode` is `#[non_exhaustive]`, so a future
        // opcode falls back to the default cost via an unrecognized key.
        _ => "native_op",
    }
}
