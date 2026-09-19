// SPDX-License-Identifier: BUSL-1.1

//! Vector-engine WAL dispatch: one payload encoder per record shape, and the
//! append path that chooses the record for a physical op.

pub mod append;
pub mod encode;

pub(crate) use append::wal_append_vector_op;
pub use append::{
    VectorDeleteWalArgs, VectorPutWalArgs, wal_append_vector_delete_by_surrogate,
    wal_append_vector_put,
};
pub(crate) use encode::{
    VectorDirectDeleteRecord, VectorDirectUpdatePayload, VectorDirectUpdateRecord,
    VectorDirectUpsertPayload, VectorDirectUpsertRecord, encode_multi_vector_delete_payload,
    encode_multi_vector_put_payload, encode_sparse_vector_delete_payload,
    encode_sparse_vector_put_payload, encode_vector_batch_put_payload,
    encode_vector_delete_by_surrogate_payload, encode_vector_delete_payload,
    encode_vector_direct_delete_payload, encode_vector_direct_update_payload,
    encode_vector_direct_upsert_payload, encode_vector_put_payload,
};
pub(crate) use encode::{
    VectorResolvedDirectWritePayload, VectorResolvedDirectWriteRecord,
    encode_vector_resolved_direct_write_payload,
};
