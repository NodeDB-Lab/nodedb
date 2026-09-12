// SPDX-License-Identifier: BUSL-1.1

//! The read-modify-write that moves one materialized-sum balance.
//!
//! One implementation, two callers, deliberately:
//!
//! * the **co-resident** path — [`super::apply`] — runs it inside the SOURCE
//!   write's transaction, because one core owns both rows;
//! * the **cross-shard** path — the `ApplyBalanceDelta` handler — runs it in a
//!   transaction of its own on the TARGET's core, because the source's
//!   transaction cannot reach rows this core does not own.
//!
//! Those are two different transactions, but they must produce byte-identical
//! rows: the balance the cross-shard task writes has to be the balance the
//! co-resident path would have written, or the same statement would total
//! differently depending on where two collections happened to hash. So the
//! arithmetic, the encoding decisions and the refusals all live here, and each
//! caller supplies only the transaction it owns.
//!
//! The write goes through [`CoreLoop::apply_point_put`], not a bare
//! `sparse.put`: the target row gets WAL-consistent transaction membership,
//! inverted-index maintenance, secondary and versioned index maintenance, column
//! statistics, document-cache population and aggregate-cache invalidation —
//! everything any other write of that row would get.

use redb::WriteTransaction;
use rust_decimal::Decimal;

use nodedb_types::Surrogate;

use super::apply::TargetWrite;
use super::delta::json_to_decimal;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::types::{DatabaseId, Lsn, TenantId};

/// Everything one balance move needs, independent of which transaction it lands
/// in.
pub(in crate::data::executor) struct BalanceRmw<'a> {
    pub database_id: u64,
    pub tid: u64,
    /// TARGET collection — never the source.
    pub target_collection: &'a str,
    /// The balance column being moved.
    pub target_column: &'a str,
    /// Target row's identity. Resolved on the Control Plane; never derived from
    /// a store probe, because a join-key VALUE is not a storage key.
    pub surrogate: Surrogate,
    /// Signed amount to add.
    pub delta: Decimal,
    /// Binding's join column, for the typed not-found error.
    pub join_column: &'a str,
    /// Join value that resolved to `surrogate`, for the typed not-found error.
    pub join_value: &'a str,
    pub wal_lsn: Option<Lsn>,
}

impl BalanceRmw<'_> {
    /// The typed error a missing target row fails with.
    ///
    /// Skipping instead would leave the stored total short of the `SUM(...)`
    /// that `VERIFY_BALANCE` recomputes over every source row — the feature
    /// would report itself broken.
    ///
    /// This one IS about the user's data, unlike
    /// [`MaterializedSumResolutionMissing`](crate::Error::MaterializedSumResolutionMissing):
    /// identity resolved, and the row that identity names is not in storage —
    /// the target was deleted while its binding survived. A replica applying a
    /// write the leader accepted cannot reach it: the target row's own write
    /// precedes this one in the same log, so by the time this delta applies the
    /// row is present on every replica that has applied that far.
    fn target_not_found(&self) -> crate::Error {
        crate::Error::MaterializedSumTargetNotFound {
            target_collection: self.target_collection.to_string(),
            join_column: self.join_column.to_string(),
            join_value: self.join_value.to_string(),
        }
    }
}

impl CoreLoop {
    /// Read the target row, add `delta` to its balance column, and write it back
    /// through the full document write path inside `txn`.
    pub(in crate::data::executor) fn apply_balance_delta(
        &mut self,
        txn: &WriteTransaction,
        params: &BalanceRmw<'_>,
    ) -> crate::Result<TargetWrite> {
        let storage_key = nodedb_types::StorageKey::for_surrogate(params.surrogate);
        // Rendered once, for the not-an-object error message below.
        let document_id = storage_key.to_string();

        // The TARGET collection's encoding is resolved from `doc_configs`, not
        // assumed: the target is a different collection from the source and may
        // be strict (Binary Tuples), which the schemaless decoder cannot read.
        let format = self.sparse_body_format(
            DatabaseId::new(params.database_id),
            TenantId::new(params.tid),
            params.target_collection,
        );
        if matches!(format, SparseBodyFormat::VectorSidecar) {
            // A vector-primary collection's rows are TAGGED `zerompk` sidecars
            // written by the vector upsert handler, not document bodies. The
            // document write path below would store an untagged map over them,
            // which reads back as tag arrays. Refusing is the only outcome that
            // does not corrupt the row.
            return Err(crate::Error::Storage {
                engine: "materialized_sum".into(),
                detail: format!(
                    "target collection '{}' is vector-primary; its rows are metadata \
                     sidecars and cannot carry a materialized sum",
                    params.target_collection
                ),
            });
        }

        let Some(old_bytes) = self.read_balance_row(txn, params, &storage_key)? else {
            return Err(params.target_not_found());
        };

        let mut target_doc = decode_scanned_document(&old_bytes, format.as_format_ref())?;
        let current = target_doc
            .get(params.target_column)
            .and_then(json_to_decimal)
            .unwrap_or(Decimal::ZERO);
        let new_balance = current + params.delta;

        // Always stored as a string: `f64` is lossy past 15 significant digits,
        // and a balance is exactly the column where that shows up.
        let Some(object) = target_doc.as_object_mut() else {
            return Err(crate::Error::Storage {
                engine: "materialized_sum".into(),
                detail: format!(
                    "target row {}/{document_id} is not an object",
                    params.target_collection
                ),
            });
        };
        object.insert(
            params.target_column.to_string(),
            serde_json::Value::String(new_balance.to_string()),
        );

        // `apply_point_put` takes an incoming BODY and encodes it into whatever
        // the target collection stores — a Binary Tuple for a strict target —
        // so the body handed to it is MessagePack for every storage mode. The
        // decode above still has to be format-aware, because the bytes read back
        // out of the store are in the collection's own encoding.
        let body = doc_format::encode_to_msgpack(&target_doc);

        let put = self.apply_point_put(
            txn,
            PointPutParams {
                database_id: params.database_id,
                tid: params.tid,
                collection: params.target_collection,
                storage_key,
                surrogate: params.surrogate,
                value: &body,
                index_text: true,
                // A derived write carries no user intent and no user roles: its
                // admission was decided when the SOURCE row was admitted. Running
                // the target's own PUT admission (append-only, period lock,
                // role-gated state transitions) against it would refuse a write
                // the user never issued, on a row whose only changed column is
                // one the engine maintains.
                user_roles: &[],
                enforce: false,
                wal_lsn: params.wal_lsn,
                resolved_targets: &[],
            },
        );
        let outcome = match put {
            Ok(outcome) => outcome,
            Err(e) => {
                // A rejection late in `apply_point_put` lands after it has already
                // cached the row it wrote. The caller drops `txn`, so that cache
                // entry would outlive a balance update that never committed.
                self.doc_cache.invalidate(
                    params.database_id,
                    params.tid,
                    params.target_collection,
                    &storage_key,
                );
                return Err(e);
            }
        };

        Ok(TargetWrite {
            collection: params.target_collection.to_string(),
            surrogate: params.surrogate,
            body,
            outcome,
        })
    }

    /// Read the target row's current stored bytes.
    ///
    /// The plain read goes through the CALLER'S write transaction so a second
    /// delta against the same row in the same transaction sees the first one's
    /// result. A bitemporal target reads its current version the same way
    /// `apply_point_put` reads its own pre-image.
    fn read_balance_row(
        &self,
        txn: &WriteTransaction,
        params: &BalanceRmw<'_>,
        storage_key: &nodedb_types::StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        if self.is_bitemporal(params.database_id, params.tid, params.target_collection) {
            self.sparse.versioned_get_current(
                params.database_id,
                params.tid,
                params.target_collection,
                storage_key,
            )
        } else {
            self.sparse.get_in_txn(
                txn,
                params.database_id,
                params.tid,
                params.target_collection,
                storage_key,
            )
        }
    }
}
