// SPDX-License-Identifier: BUSL-1.1

//! Passive-read helpers for the Calvin dependent-read path.
//!
//! [`CoreLoop::execute_calvin_execute_passive`] reads each declared key from
//! base storage to build the `Vec<(PassiveReadKeyId, Value)>` payload the
//! Control Plane scheduler proposes as a `CalvinReadResult` Raft entry.
//!
//! A row's value is its stored bytes as `Value::Bytes`, `Value::Null` when
//! the row is absent. Stored bytes compare exactly, so an active participant
//! matches them against the bytes its coordinator read.
//!
//! `EngineKeySet.collection` arrives database-qualified: the Calvin key
//! extraction reads it off the physical plan's qualified collection field.
//! The engines key their tables by that same string.

use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
use nodedb_types::{DatabaseId, QualifiedCollection, StorageKey, Surrogate, Value};

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::kv::current_ms;
use crate::types::TenantId;

impl CoreLoop {
    /// Read every row of `engine_key` from base storage.
    ///
    /// A passive participant reads document rows by surrogate and key-value
    /// rows by key. Every other key set names no row a passive read returns,
    /// so it is refused with `Unsupported`.
    pub(super) fn read_passive_key(
        &self,
        database_id: u64,
        tenant_id: &TenantId,
        engine_key: &nodedb_cluster::calvin::types::EngineKeySet,
    ) -> Result<Vec<(PassiveReadKeyId, Value)>, ErrorCode> {
        use nodedb_cluster::calvin::types::EngineKeySet;

        let tid = tenant_id.as_u64();
        match engine_key {
            EngineKeySet::Document {
                collection,
                surrogates,
            } => surrogates
                .iter()
                .map(|&surrogate| {
                    // The row at its current version, so a bitemporal
                    // collection reads too.
                    let stored = self
                        .current_row_body(
                            database_id,
                            tid,
                            collection,
                            &StorageKey::for_surrogate(Surrogate::new(surrogate)),
                        )
                        .map_err(|e| ErrorCode::Internal {
                            detail: format!(
                                "calvin passive read of {collection} row {surrogate}: {e}"
                            ),
                        })?;
                    let qualified = passive_collection(database_id, collection)?;
                    Ok((
                        PassiveReadKeyId::surrogate(qualified, surrogate),
                        stored_value(stored),
                    ))
                })
                .collect(),
            EngineKeySet::Kv { collection, keys } => {
                let now_ms = current_ms();
                keys.iter()
                    .map(|key| {
                        let stored = self
                            .kv_engine
                            .get(database_id, tid, collection, key, now_ms);
                        let qualified = passive_collection(database_id, collection)?;
                        Ok((
                            PassiveReadKeyId::kv(qualified, key.clone()),
                            stored_value(stored),
                        ))
                    })
                    .collect()
            }
            EngineKeySet::Vector { collection, .. }
            | EngineKeySet::Edge { collection, .. }
            | EngineKeySet::Array { collection, .. }
            | EngineKeySet::Collection { collection, .. }
            | EngineKeySet::Unique { collection, .. } => Err(ErrorCode::Unsupported {
                detail: format!(
                    "a passive Calvin read names rows of {collection} by a key set that is \
                     neither document surrogates nor key-value keys"
                ),
            }),
        }
    }
}

/// The stored collection a passive read names, checked against its database.
fn passive_collection(
    database_id: u64,
    collection: &str,
) -> Result<QualifiedCollection, ErrorCode> {
    QualifiedCollection::from_stored(DatabaseId::new(database_id), collection.to_owned())
        .map_err(|e| ErrorCode::from(crate::Error::from(e)))
}

/// The value a passive read reports for a row: its stored bytes, or `Null`
/// for an absent row.
fn stored_value(stored: Option<Vec<u8>>) -> Value {
    match stored {
        Some(bytes) => Value::Bytes(bytes),
        None => Value::Null,
    }
}
