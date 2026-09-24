// SPDX-License-Identifier: BUSL-1.1

//! Vector-primary serializer for transaction resolve. Overlay-driven.
//!
//! A vector-primary direct write (`DirectInsert`, `DirectInsertIfAbsent`,
//! `DirectUpsert`, `DirectUpdate`, `DirectDelete`, `DirectTruncate`) stages
//! the row it leaves behind per surrogate: a [`StagedVectorRow`] holding the
//! vector and the exact sidecar bytes, or a tombstone. An `ON CONFLICT DO
//! UPDATE` stages the merged sidecar and an UPDATE stages the patched one.
//! That staged row is what the transaction's own reads showed, so the redo
//! carries it verbatim as one `VectorResolvedDirectWrite` record per
//! collection, the record the governed autocommit path already ships. Replay
//! installs each row through the same apply and never re-runs a merge or a
//! patch against the replaying node's state.
//!
//! * A staged row → `VectorResolvedMutation::Upsert` with the staged vector
//!   and sidecar. `old_payload` is the base sidecar the row replaces, `None`
//!   when no base row is bound.
//! * A staged tombstone of a bound base row → `VectorResolvedMutation::Delete`.
//!   A tombstone of a row the transaction itself inserted emits nothing.
//! * A staged TRUNCATE → the `VectorDirectTruncate` record ahead of the rows.
//!
//! Rows are emitted in surrogate order, so two resolves of one transaction
//! produce byte-identical records.
//!
//! Session and Calvin transactions stage every direct write, so both
//! resolve here.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::VectorResolvedMutation;
use nodedb_types::{PayloadIndexKind, Surrogate, VectorQuantization, VectorStorageDtype};
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::{
    VectorResolvedDirectWritePayload, encode_vector_direct_truncate_payload,
    encode_vector_resolved_direct_write_payload,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{Staged, StagedVectorRow, TxnOverlay};
use crate::types::{DatabaseId, TenantId};
use crate::wal::RedoSubRecord;

/// The index settings a direct insert-family or update plan carries.
#[derive(Clone)]
pub(super) struct VectorPrimarySpec {
    pub quantization: VectorQuantization,
    pub storage_dtype: VectorStorageDtype,
    pub payload_indexes: Vec<(String, PayloadIndexKind)>,
}

/// What the plans say about one vector-primary collection the transaction
/// wrote.
#[derive(Default)]
pub(super) struct VectorPrimaryWrites {
    pub field: String,
    /// `None` when only DELETE / TRUNCATE plans named the collection.
    pub spec: Option<VectorPrimarySpec>,
    /// The declared primary-key bytes each inserted surrogate carried.
    pub pk_by_surrogate: BTreeMap<u32, Vec<u8>>,
}

/// Vector-primary collections a transaction wrote, keyed by collection.
pub(super) type VectorPrimaryCollections = BTreeMap<String, VectorPrimaryWrites>;

/// Record one vector-primary direct write against its collection. `spec` is
/// the index settings an insert-family or update plan carries. `pk` is the
/// surrogate and declared primary-key bytes an insert-family plan carries.
pub(super) fn note_direct_write(
    collections: &mut VectorPrimaryCollections,
    collection: &str,
    field: &str,
    spec: Option<VectorPrimarySpec>,
    pk: Option<(Surrogate, &[u8])>,
) {
    let writes = collections.entry(collection.to_string()).or_default();
    if writes.field.is_empty() {
        writes.field = field.to_string();
    }
    if writes.spec.is_none() {
        writes.spec = spec;
    }
    if let Some((surrogate, pk_bytes)) = pk {
        writes
            .pk_by_surrogate
            .entry(surrogate.as_u32())
            .or_insert_with(|| pk_bytes.to_vec());
    }
}

impl CoreLoop {
    /// Append the truncate record (when the transaction truncated the
    /// collection) and the collection's `VectorResolvedDirectWrite` record
    /// to `ops`. Reads base by `&` only.
    pub(super) fn serialize_vector_primary_collection(
        &self,
        overlay: &TxnOverlay,
        coll_key: &(DatabaseId, TenantId, String),
        writes: &VectorPrimaryWrites,
        ops: &mut Vec<RedoSubRecord>,
    ) -> crate::Result<()> {
        let (database_id, tid, collection) =
            (coll_key.0.as_u64(), coll_key.1.as_u64(), &coll_key.2);
        let truncated = overlay.is_truncated(coll_key);
        if truncated {
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDirectTruncate as u32,
                payload: encode_vector_direct_truncate_payload(collection, &writes.field)?,
            });
        }

        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, &writes.field);
        let entries: BTreeMap<u32, &Staged> = overlay.iter_for_collection(coll_key).collect();
        let mut mutations = Vec::with_capacity(entries.len());
        for (surrogate_u32, staged) in entries {
            let surrogate = Surrogate::new(surrogate_u32);
            let base = if !truncated && self.vector_direct_node(&index_key, surrogate).is_some() {
                Some(
                    self.vector_sidecar_bytes(database_id, tid, collection, surrogate)
                        .map_err(|e| crate::Error::Internal {
                            detail: format!(
                                "vector-primary resolve of '{collection}': base sidecar of \
                                 surrogate {surrogate_u32}: {e:?}"
                            ),
                        })?
                        .unwrap_or_default(),
                )
            } else {
                None
            };
            match staged {
                Staged::Put(body) => {
                    let row = StagedVectorRow::from_bytes(body)?;
                    mutations.push(VectorResolvedMutation::Upsert {
                        surrogate,
                        pk_bytes: writes
                            .pk_by_surrogate
                            .get(&surrogate_u32)
                            .cloned()
                            .unwrap_or_default(),
                        vector: row.vector,
                        payload: row.sidecar,
                        old_payload: base,
                    });
                }
                Staged::Tombstone => {
                    if let Some(old_payload) = base {
                        mutations.push(VectorResolvedMutation::Delete {
                            surrogate,
                            old_payload,
                        });
                    }
                }
            }
        }
        if mutations.is_empty() {
            return Ok(());
        }

        let stores_rows = mutations
            .iter()
            .any(|m| matches!(m, VectorResolvedMutation::Upsert { .. }));
        let spec = match (&writes.spec, stores_rows) {
            (Some(spec), _) => spec.clone(),
            (None, false) => VectorPrimarySpec {
                quantization: VectorQuantization::default(),
                storage_dtype: VectorStorageDtype::default(),
                payload_indexes: Vec::new(),
            },
            (None, true) => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "vector-primary resolve of '{collection}': a staged row has no \
                         insert or update plan carrying its index settings"
                    ),
                });
            }
        };
        let payload =
            encode_vector_resolved_direct_write_payload(VectorResolvedDirectWritePayload {
                collection,
                field: &writes.field,
                quantization: spec.quantization,
                storage_dtype: spec.storage_dtype,
                payload_indexes: &spec.payload_indexes,
                mutations: &mutations,
            })?;
        ops.push(RedoSubRecord {
            record_type: RecordType::VectorResolvedDirectWrite as u32,
            payload,
        });
        Ok(())
    }
}
