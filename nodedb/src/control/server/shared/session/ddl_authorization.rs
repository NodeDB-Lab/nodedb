// SPDX-License-Identifier: BUSL-1.1

//! Hold a transactional DDL commit until its authorization changes bind
//! every node.

use nodedb_cluster::{GroupCoverage, METADATA_GROUP_ID, MetadataEntry, PendingDdlObject};

use crate::control::catalog_entry;
use crate::control::state::SharedState;

/// Whether any of `objects` changes authorization state.
pub(super) fn objects_bear_authorization(objects: &[PendingDdlObject]) -> crate::Result<bool> {
    for object in objects {
        let entry = match object {
            PendingDdlObject::Create { entry } | PendingDdlObject::Alter { entry, .. } => entry,
        };
        let payload = match entry.as_ref() {
            MetadataEntry::CatalogDdl { payload }
            | MetadataEntry::CatalogDdlAudited { payload, .. } => payload,
            other => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "transactional DDL: pending object wire shape is not CatalogDdl: {other:?}"
                    ),
                });
            }
        };
        if catalog_entry::decode(payload)?.bears_authorization() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Run the authorization barrier on the metadata entry committed at
/// `log_index`.
pub(super) fn barrier_at(state: &SharedState, log_index: u64) -> crate::Result<()> {
    crate::control::security::auth_lease::block_on_barrier(
        state,
        vec![GroupCoverage {
            group_id: METADATA_GROUP_ID,
            through: log_index,
        }],
    )
}
