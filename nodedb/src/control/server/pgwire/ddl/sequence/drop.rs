//! `DROP SEQUENCE` handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::parse::parse_drop_target;

pub fn drop_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    let (name, if_exists) = parse_drop_target(parts, 2);

    if !state.sequence_registry.exists(tenant_id, &name) {
        if if_exists {
            return Ok(vec![Response::Execution(Tag::new("DROP SEQUENCE"))]);
        }
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P01".to_owned(),
            format!("sequence \"{name}\" does not exist"),
        ))));
    }

    // Propose the delete through the metadata raft group. Every
    // node's applier removes the record from local redb and from
    // its in-memory `sequence_registry`.
    let entry = crate::control::catalog_entry::CatalogEntry::DeleteSequence {
        tenant_id,
        name: name.clone(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                e.to_string(),
            )))
        })?;
    if log_index == 0 {
        // Single-node / no-cluster fallback.
        if let Some(catalog) = state.credentials.catalog() {
            let _ = catalog.delete_sequence(tenant_id, &name);
        }
        let _ = state.sequence_registry.remove(tenant_id, &name);
    }

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("DROP SEQUENCE"))])
}
