// SPDX-License-Identifier: BUSL-1.1

//! Assign a fresh, catalog-registered surrogate for a row written into a
//! target collection on behalf of another operation.

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use super::pk::{TargetPk, extract_pk_value};
use crate::control::state::SharedState;

/// Assign a fresh, registered surrogate for one written row on the TARGET's
/// primary key.
pub(crate) fn assign_target_surrogate(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    target_collection: &str,
    target_pk: &TargetPk,
    body: &[u8],
) -> crate::Result<Surrogate> {
    match target_pk {
        TargetPk::AutoRowId => {
            let (surrogate, _) = state.surrogate_assigner.assign_fresh(
                database_id,
                tenant_id,
                target_collection,
                nodedb_physical::FreshSurrogateKind::AutoRowId,
            )?;
            Ok(surrogate)
        }
        TargetPk::Field { name, declared } => match extract_pk_value(body, name) {
            // The empty string is a key like any other. Minting a fresh
            // surrogate for it would let two rows share it.
            Some(pk) => state.surrogate_assigner.assign(
                database_id,
                tenant_id,
                target_collection,
                pk.as_bytes(),
            ),
            // No usable key value on a DDL-declared PRIMARY KEY: NOT NULL is
            // implied, so refuse rather than mint a surrogate for a row that
            // plain INSERT would already reject.
            None if *declared => Err(crate::Error::RejectedConstraint {
                collection: target_collection.to_string(),
                constraint: "not_null".to_string(),
                detail: format!("primary key '{name}' cannot be NULL or omitted"),
            }),
            // Undeclared `id`-by-convention field: mint a fresh unique
            // surrogate rather than collapsing every keyless row onto one
            // binding. The row's identity is its document storage key, so
            // the allocator binds the hex form.
            _ => {
                let (surrogate, _) = state.surrogate_assigner.assign_fresh(
                    database_id,
                    tenant_id,
                    target_collection,
                    nodedb_physical::FreshSurrogateKind::DocumentStorageKey,
                )?;
                Ok(surrogate)
            }
        },
    }
}
