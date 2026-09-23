// SPDX-License-Identifier: BUSL-1.1

//! WAL append logic for write operations.
//!
//! Serializes write plans as MessagePack and appends to the appropriate
//! WAL record type. Read operations are no-ops.

mod array;
mod columnar;
mod core;
mod crdt;
mod document;
mod graph;
mod graph_labels;
mod spatial;
mod stamp;
mod text;
mod timeseries;
mod vector;
mod write_set_redo;

pub use core::{
    WalAppendOutcome, WalAppendRequest, wal_append, wal_append_if_write,
    wal_append_if_write_with_creds,
};
pub use stamp::stamp_minted_lsn;
pub use timeseries::{ColumnarWalAppendArgs, wal_append_columnar};
pub(crate) use timeseries::{TimeseriesWalAppendContext, wal_append_timeseries};
pub use vector::{
    VectorDeleteWalArgs, VectorPutWalArgs, wal_append_vector_delete_by_surrogate,
    wal_append_vector_put,
};
pub use write_set_redo::{append_write_set_redo, mint_dispatch_local_redo, plan_post_apply_redo};

// Payload encoders shared by the autocommit WAL path and transaction resolve, so
// each engine's record shape lives in exactly one place.
pub(crate) use crdt::encode_crdt_op_record;
pub(crate) use graph_labels::encode_graph_node_label_payload;
pub(crate) use text::encode_text_op_record;
#[cfg(test)]
pub(crate) use timeseries::{TimeseriesIngestRecord, encode_timeseries_ingest_payload};
pub(crate) use timeseries::{
    encode_columnar_truncate_payload, encode_timeseries_batch_payload_with_format,
};
pub(crate) use vector::{
    VectorDirectDeleteRecord, VectorDirectTruncateRecord, VectorDirectUpdatePayload,
    VectorDirectUpdateRecord, VectorDirectUpsertPayload, VectorDirectUpsertRecord,
    VectorResolvedDirectWritePayload, VectorResolvedDirectWriteRecord,
    encode_multi_vector_delete_payload, encode_multi_vector_put_payload,
    encode_sparse_vector_delete_payload, encode_sparse_vector_put_payload,
    encode_vector_batch_put_payload, encode_vector_delete_by_surrogate_payload,
    encode_vector_delete_payload, encode_vector_direct_delete_payload,
    encode_vector_direct_truncate_payload, encode_vector_direct_update_payload,
    encode_vector_direct_upsert_payload, encode_vector_put_payload,
    encode_vector_resolved_direct_write_payload,
};

pub(crate) use super::wal_dispatch_fts_spatial::{
    encode_spatial_delete_payload, encode_spatial_put_payload,
};
pub use super::wal_dispatch_fts_spatial::{
    wal_append_fts_delete, wal_append_fts_index, wal_append_spatial_delete, wal_append_spatial_put,
};
