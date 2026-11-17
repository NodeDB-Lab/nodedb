// SPDX-License-Identifier: BUSL-1.1

//! The index section of a tenant backup.
//!
//! Every index kind is identified by its catalog record. The record is what
//! `SHOW INDEXES` lists and `DROP INDEX` resolves, and a full-text record
//! declares the fields a collection's text index holds. The section carries
//! each record with its owner, and a vector index's build parameters from
//! `_system.vector_index_params`.

use nodedb_types::backup_envelope::IndexRecordBlob;

use crate::Error;
use crate::control::security::catalog::IndexKind;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::metadata::{TenantDatabase, encode_section_part};

/// One blob per index record of `databases`, in database then key order.
pub(super) fn index_blobs(
    state: &SharedState,
    tenant: TenantId,
    databases: &[TenantDatabase],
) -> Result<Vec<IndexRecordBlob>, Error> {
    let catalog = state.credentials.catalog();
    let mut blobs = Vec::new();
    for database in databases {
        let database_id = database.id().as_u64();
        for record in &database.indexes {
            let owner = state.permissions.get_owner(
                record.kind.owner_object_type(),
                database_id,
                tenant,
                &record.name,
            );
            let vector_params = match record.kind {
                IndexKind::Vector => catalog
                    .get_committed_vector_index_params(
                        database_id,
                        tenant.as_u64(),
                        &record.collection,
                        record.primary_field(),
                    )?
                    .map(|params| encode_section_part("vector index params", &params))
                    .transpose()?,
                IndexKind::Secondary
                | IndexKind::FullText
                | IndexKind::Spatial
                | IndexKind::Sparse
                | IndexKind::Sorted => None,
            };
            blobs.push(IndexRecordBlob {
                database_id,
                name: record.name.clone(),
                record: encode_section_part("index record", record)?,
                owner,
                vector_params,
            });
        }
    }
    Ok(blobs)
}
