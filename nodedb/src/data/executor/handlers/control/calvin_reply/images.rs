// SPDX-License-Identifier: BUSL-1.1

//! Stored images of the rows a Calvin plan touched, and the `RETURNING`
//! rows they render.
//!
//! Each engine renders through the converter its live write handler uses,
//! so a row reads the same whichever path wrote it.

use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_types::{RowIdentity, StorageKey, Surrogate};

use super::target::RowEngine;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_rows::{
    build_stored_rows_payload, kv_stored_rows_payload, vector_stored_rows_payload,
};
use crate::data::executor::handlers::transaction::overlay::staged_vector_sidecar;
use crate::data::executor::handlers::transaction::stage_write::unhex_key;

/// One row's identity and the stored bytes of the image `RETURNING`
/// reports. A vector-primary row's bytes are its payload sidecar.
pub(super) struct StoredRow {
    pub identity: RowIdentity,
    pub surrogate: Surrogate,
    pub bytes: Vec<u8>,
}

/// Where a row lives, for a base read.
pub(super) struct RowLocation<'a> {
    pub engine: RowEngine,
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
}

/// The stored bytes of a row staged as `body`. A staged vector-primary row
/// carries its vector beside its sidecar, and only the sidecar is rendered.
pub(super) fn staged_row_bytes(engine: RowEngine, body: &[u8]) -> Result<Vec<u8>, ErrorCode> {
    match engine {
        RowEngine::Vector => staged_vector_sidecar(body).map_err(ErrorCode::from),
        RowEngine::Document
        | RowEngine::Crdt
        | RowEngine::Kv
        | RowEngine::Columnar
        | RowEngine::Timeseries => Ok(body.to_vec()),
    }
}

/// The raw key of a KV row, from its overlay identity.
fn kv_raw_key(identity: &RowIdentity) -> Result<Vec<u8>, ErrorCode> {
    unhex_key(identity.as_str()).ok_or_else(|| ErrorCode::Internal {
        detail: format!(
            "calvin RETURNING: KV row identity '{}' is not a hex-encoded key",
            identity.as_str()
        ),
    })
}

impl CoreLoop {
    /// The row's body in base storage, or `None` when base holds no such row.
    /// A bitemporal document row reads its current version.
    pub(super) fn calvin_base_row(
        &self,
        at: &RowLocation<'_>,
        identity: &RowIdentity,
        surrogate: Surrogate,
    ) -> Result<Option<Vec<u8>>, ErrorCode> {
        let RowLocation {
            engine,
            database_id,
            tid,
            collection,
        } = *at;
        match engine {
            RowEngine::Document | RowEngine::Crdt => {
                let key = StorageKey::for_surrogate(surrogate);
                let read = if self.is_bitemporal(database_id, tid, collection) {
                    self.sparse
                        .versioned_get_current(database_id, tid, collection, &key)
                } else {
                    self.sparse.get(database_id, tid, collection, &key)
                };
                read.map_err(ErrorCode::from)
            }
            RowEngine::Kv => {
                let key = kv_raw_key(identity)?;
                Ok(self
                    .kv_engine
                    .get(database_id, tid, collection, &key, self.kv_read_now_ms()))
            }
            RowEngine::Vector => self.vector_sidecar_bytes(database_id, tid, collection, surrogate),
            // Neither engine keys a base row by surrogate. Their `RETURNING`
            // rows are always the images the plan staged.
            RowEngine::Columnar | RowEngine::Timeseries => Err(ErrorCode::Internal {
                detail: format!(
                    "calvin RETURNING: a {engine:?} row of '{collection}' has no keyed base read"
                ),
            }),
        }
    }

    /// Render `rows` as the `RETURNING` row set of `at.engine`.
    pub(super) fn calvin_render_rows(
        &self,
        at: &RowLocation<'_>,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        rows: &[StoredRow],
    ) -> Result<Vec<u8>, ErrorCode> {
        match at.engine {
            RowEngine::Document | RowEngine::Crdt => {
                // A CRDT row is MessagePack in either storage mode.
                let strict_schema = match at.engine {
                    RowEngine::Document => {
                        self.resolve_strict_schema(at.database_id, at.tid, at.collection)
                    }
                    RowEngine::Crdt
                    | RowEngine::Kv
                    | RowEngine::Vector
                    | RowEngine::Columnar
                    | RowEngine::Timeseries => None,
                };
                let stored: Vec<(&RowIdentity, &[u8])> = rows
                    .iter()
                    .map(|row| (&row.identity, row.bytes.as_slice()))
                    .collect();
                build_stored_rows_payload(spec, rls_filters, strict_schema.as_ref(), &stored)
                    .map_err(ErrorCode::from)
            }
            RowEngine::Kv => {
                let keys = rows
                    .iter()
                    .map(|row| kv_raw_key(&row.identity))
                    .collect::<Result<Vec<_>, _>>()?;
                let stored: Vec<(&[u8], &[u8])> = keys
                    .iter()
                    .zip(rows)
                    .map(|(key, row)| (key.as_slice(), row.bytes.as_slice()))
                    .collect();
                kv_stored_rows_payload(spec, rls_filters, &stored).map_err(ErrorCode::from)
            }
            RowEngine::Vector => {
                let keys: Vec<StorageKey> = rows
                    .iter()
                    .map(|row| StorageKey::for_surrogate(row.surrogate))
                    .collect();
                let stored: Vec<(&StorageKey, &[u8])> = keys
                    .iter()
                    .zip(rows)
                    .map(|(key, row)| (key, row.bytes.as_slice()))
                    .collect();
                vector_stored_rows_payload(spec, rls_filters, &stored).map_err(ErrorCode::from)
            }
            RowEngine::Columnar | RowEngine::Timeseries => Err(ErrorCode::Internal {
                detail: format!(
                    "calvin RETURNING: {:?} rows of '{}' render from their staged values",
                    at.engine, at.collection
                ),
            }),
        }
    }
}
