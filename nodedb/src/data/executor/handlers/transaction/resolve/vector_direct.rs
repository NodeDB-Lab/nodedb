// SPDX-License-Identifier: BUSL-1.1

//! Vector-primary direct writes in transaction resolve, by staging source.
//!
//! A session transaction stages every direct write into its overlay, so its
//! redo carries the staged rows (`vector_primary`): each op here only
//! registers its collection. A Calvin transaction stages no vector-primary
//! write, so its redo carries each op as the autocommit record shape and
//! replay re-runs it through the live handler in the epoch's deterministic
//! order.

use nodedb_physical::physical_plan::{UpdateValue, VectorDirectWriteIntent, VectorWriteTargets};
use nodedb_types::Surrogate;
use nodedb_wal::record::RecordType;

use super::vector_primary::{VectorPrimaryCollections, VectorPrimarySpec, note_direct_write};
use crate::control::server::wal_dispatch::{
    VectorDirectUpdatePayload, VectorDirectUpsertPayload, encode_vector_direct_delete_payload,
    encode_vector_direct_truncate_payload, encode_vector_direct_update_payload,
    encode_vector_direct_upsert_payload,
};
use crate::wal::RedoSubRecord;

/// Where a transaction's vector-primary direct writes resolve from.
pub(super) enum DirectWrites<'a> {
    /// The overlay holds the staged rows; ops register their collection.
    Staged(&'a mut VectorPrimaryCollections),
    /// Nothing is staged; each op serializes from its plan node.
    Plan,
}

/// One insert-family direct write (`DirectInsert` / `DirectInsertIfAbsent` /
/// `DirectUpsert`).
pub(super) struct DirectInsert<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub surrogate: Surrogate,
    pub pk_bytes: &'a [u8],
    pub vector: &'a [f32],
    pub payload: &'a [u8],
    pub spec: VectorPrimarySpec,
    pub intent: VectorDirectWriteIntent,
    pub on_conflict_updates: &'a [(String, UpdateValue)],
}

/// One `DirectUpdate`.
pub(super) struct DirectUpdate<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub targets: &'a VectorWriteTargets,
    pub new_vector: Option<&'a [f32]>,
    pub payload_patch: &'a [(String, UpdateValue)],
    pub spec: VectorPrimarySpec,
}

impl DirectWrites<'_> {
    pub(super) fn insert(
        &mut self,
        write: DirectInsert<'_>,
        ops: &mut Vec<RedoSubRecord>,
    ) -> crate::Result<()> {
        match self {
            Self::Staged(collections) => {
                note_direct_write(
                    collections,
                    write.collection,
                    write.field,
                    Some(write.spec),
                    Some((write.surrogate, write.pk_bytes)),
                );
                Ok(())
            }
            Self::Plan => {
                let payload = encode_vector_direct_upsert_payload(VectorDirectUpsertPayload {
                    collection: write.collection,
                    field: write.field,
                    surrogate: write.surrogate,
                    pk_bytes: write.pk_bytes,
                    vector: write.vector,
                    payload: write.payload,
                    quantization: write.spec.quantization,
                    storage_dtype: write.spec.storage_dtype,
                    payload_indexes: &write.spec.payload_indexes,
                    intent: write.intent,
                    on_conflict_updates: write.on_conflict_updates,
                })?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::VectorDirectUpsert as u32,
                    payload,
                });
                Ok(())
            }
        }
    }

    pub(super) fn update(
        &mut self,
        write: DirectUpdate<'_>,
        ops: &mut Vec<RedoSubRecord>,
    ) -> crate::Result<()> {
        match self {
            Self::Staged(collections) => {
                note_direct_write(
                    collections,
                    write.collection,
                    write.field,
                    Some(write.spec),
                    None,
                );
                Ok(())
            }
            Self::Plan => {
                let payload = encode_vector_direct_update_payload(VectorDirectUpdatePayload {
                    collection: write.collection,
                    field: write.field,
                    targets: write.targets,
                    new_vector: write.new_vector,
                    payload_patch: write.payload_patch,
                    quantization: write.spec.quantization,
                    storage_dtype: write.spec.storage_dtype,
                    payload_indexes: &write.spec.payload_indexes,
                })?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::VectorDirectUpdate as u32,
                    payload,
                });
                Ok(())
            }
        }
    }

    pub(super) fn delete(
        &mut self,
        collection: &str,
        field: &str,
        targets: &VectorWriteTargets,
        ops: &mut Vec<RedoSubRecord>,
    ) -> crate::Result<()> {
        match self {
            Self::Staged(collections) => {
                note_direct_write(collections, collection, field, None, None);
                Ok(())
            }
            Self::Plan => {
                ops.push(RedoSubRecord {
                    record_type: RecordType::VectorDirectDelete as u32,
                    payload: encode_vector_direct_delete_payload(collection, field, targets)?,
                });
                Ok(())
            }
        }
    }

    pub(super) fn truncate(
        &mut self,
        collection: &str,
        field: &str,
        ops: &mut Vec<RedoSubRecord>,
    ) -> crate::Result<()> {
        match self {
            Self::Staged(collections) => {
                note_direct_write(collections, collection, field, None, None);
                Ok(())
            }
            Self::Plan => {
                ops.push(RedoSubRecord {
                    record_type: RecordType::VectorDirectTruncate as u32,
                    payload: encode_vector_direct_truncate_payload(collection, field)?,
                });
                Ok(())
            }
        }
    }
}
