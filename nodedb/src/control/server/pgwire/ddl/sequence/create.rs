//! `CREATE SEQUENCE` handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::parse::parse_create_sequence;

pub fn create_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let mut def = parse_create_sequence(sql, tenant_id, &identity.username)?;

    def.created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    def.validate().map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P17".to_owned(),
            e,
        )))
    })?;

    if state.sequence_registry.exists(tenant_id, &def.name) {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P07".to_owned(),
            format!("sequence \"{}\" already exists", def.name),
        ))));
    }

    // Propose through the metadata raft group. On every node the
    // applier decodes `CatalogEntry::PutSequence`, writes the
    // record to local `SystemCatalog` redb, and syncs the
    // in-memory `sequence_registry` so `NEXTVAL` / `CURRVAL` on
    // followers see the replicated definition immediately.
    let entry = crate::control::catalog_entry::CatalogEntry::PutSequence(Box::new(def.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                e.to_string(),
            )))
        })?;
    if log_index == 0 {
        // Single-node / no-cluster fallback: write directly.
        if let Some(catalog) = state.credentials.catalog() {
            catalog.put_sequence(&def).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("failed to persist sequence: {e}"),
                )))
            })?;
        }
        state.sequence_registry.create(def).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                e.to_string(),
            )))
        })?;
    }

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("CREATE SEQUENCE"))])
}
