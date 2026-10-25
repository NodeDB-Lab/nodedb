// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE CREDENTIAL`, `DROP CREDENTIAL` and
//! `SHOW CREDENTIALS`.
//!
//! A credential holds the secret a remote `COPY` source authenticates with.
//! Every statement needs a superuser. `CREATE` seals the secret under the
//! `[credential_encryption]` key before it proposes the catalog entry, so
//! the metadata log, the catalog and every snapshot hold ciphertext only.
//! `SHOW CREDENTIALS` lists names, types and owners, never a secret. Each
//! statement records an audit event, and so does each refusal for lack of
//! privilege.

use serde_json::{Map, Value as JsonValue};

use nodedb_sql::ddl_ast::statement::CredentialSpec;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::source_credential::{
    CredentialKind, CredentialSecret, StoredCredential, seal_secret,
};
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// SQLSTATE of a credential name that is taken.
const DUPLICATE_OBJECT: &str = "42710";
/// SQLSTATE of a drop of a credential that does not exist.
const UNDEFINED_OBJECT: &str = "42704";
/// SQLSTATE of a server with no credential key.
const NOT_CONFIGURED: &str = "55000";

/// Handle `CREATE CREDENTIAL <name> (TYPE ..., ...)`.
pub async fn create_credential(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    spec: &CredentialSpec,
) -> Result<Vec<DdlResult>, DdlError> {
    const COMMAND: &str = "CREATE CREDENTIAL";
    require_superuser(state, identity, COMMAND)?;
    let kek = state
        .remote_sources
        .credential_kek
        .as_ref()
        .ok_or_else(|| {
            DdlError::new(
                NOT_CONFIGURED,
                "CREATE CREDENTIAL needs a credential key: set [credential_encryption] key_path \
                 in the server config",
            )
        })?;
    let tenant_id = identity.tenant_id.as_u64();
    let catalog = state.credentials.catalog();
    if catalog.get_credential(tenant_id, name)?.is_some() {
        return Err(exists(name));
    }
    let (kind, secret) = secret_of(state, spec)?;
    let sealed = seal_secret(kek, tenant_id, name, &secret)
        .map_err(|e| DdlError::internal(format!("{COMMAND} {name}: {e}")))?;
    let row = StoredCredential {
        tenant_id,
        name: name.to_string(),
        kind,
        sealed: sealed.clone(),
        owner: identity.username.clone(),
        created_at_ms: now_ms(),
    };
    // The proposal carries the statement's audit context into the metadata
    // log. `AuditCtx::for_statement` installed its redacted text.
    let entry = CatalogEntry::PutCredential(Box::new(row));
    let outcome = crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    if let ProposeOutcome::Replicated { .. } = outcome {
        // A racing CREATE of the same name commits first when the log
        // orders it first, and the apply keeps its row.
        match catalog.get_credential(tenant_id, name)? {
            Some(stored) if stored.sealed == sealed => {}
            Some(_) => return Err(exists(name)),
            None => {
                return Err(DdlError::internal(format!(
                    "{COMMAND} {name}: the committed entry wrote no row"
                )));
            }
        }
    }
    audit(
        state,
        identity,
        &format!("{COMMAND} {name} (TYPE '{}')", kind.type_name()),
    );
    Ok(status(COMMAND))
}

/// Handle `DROP CREDENTIAL [IF EXISTS] <name>`.
pub async fn drop_credential(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    if_exists: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    const COMMAND: &str = "DROP CREDENTIAL";
    require_superuser(state, identity, COMMAND)?;
    let tenant_id = identity.tenant_id.as_u64();
    if state
        .credentials
        .catalog()
        .get_credential(tenant_id, name)?
        .is_none()
    {
        if if_exists {
            return Ok(status(COMMAND));
        }
        return Err(DdlError::new(
            UNDEFINED_OBJECT,
            format!("credential '{name}' does not exist"),
        ));
    }
    let entry = CatalogEntry::DropCredential {
        tenant_id,
        name: name.to_string(),
    };
    crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    audit(state, identity, &format!("{COMMAND} {name}"));
    Ok(status(COMMAND))
}

/// Handle `SHOW CREDENTIALS`: name, type, owner and creation time.
pub fn show_credentials(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(state, identity, "SHOW CREDENTIALS")?;
    let rows = state
        .credentials
        .catalog()
        .list_credentials(identity.tenant_id.as_u64())?;
    let columns = ["name", "type", "owner", "created_at_ms"]
        .map(str::to_string)
        .to_vec();
    let rows = rows
        .iter()
        .map(|row| {
            let mut out = Map::new();
            out.insert("name".into(), JsonValue::String(row.name.clone()));
            out.insert(
                "type".into(),
                JsonValue::String(row.kind.type_name().to_string()),
            );
            out.insert("owner".into(), JsonValue::String(row.owner.clone()));
            out.insert(
                "created_at_ms".into(),
                JsonValue::String(row.created_at_ms.to_string()),
            );
            out
        })
        .collect();
    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// The kind and secret `spec` describes. An S3 endpoint must pass the
/// egress policy.
fn secret_of(
    state: &SharedState,
    spec: &CredentialSpec,
) -> Result<(CredentialKind, CredentialSecret), DdlError> {
    Ok(match spec {
        CredentialSpec::S3 {
            access_key_id,
            secret_access_key,
            region,
            session_token,
            endpoint,
        } => {
            if let Some(url) = endpoint {
                state
                    .remote_sources
                    .egress()
                    .check_url(url)
                    .map_err(|e| DdlError::new("22023", format!("ENDPOINT: {e}")))?;
            }
            (
                CredentialKind::S3,
                CredentialSecret::S3 {
                    access_key_id: access_key_id.expose().to_string(),
                    secret_access_key: secret_access_key.expose().to_string(),
                    region: region.clone(),
                    session_token: session_token.as_ref().map(|t| t.expose().to_string()),
                    endpoint: endpoint.clone(),
                },
            )
        }
        CredentialSpec::Bearer { token } => (
            CredentialKind::Bearer,
            CredentialSecret::Bearer {
                token: token.expose().to_string(),
            },
        ),
        CredentialSpec::Sas { token } => (
            CredentialKind::Sas,
            CredentialSecret::Sas {
                token: token.expose().to_string(),
            },
        ),
    })
}

fn exists(name: &str) -> DdlError {
    DdlError::new(
        DUPLICATE_OBJECT,
        format!("credential '{name}' already exists; DROP CREDENTIAL {name} first"),
    )
}

fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

fn audit(state: &SharedState, identity: &AuthenticatedIdentity, detail: &str) {
    state.audit_record_with_db(
        AuditEvent::DdlChange,
        Some(identity.tenant_id),
        None,
        &identity.username,
        detail,
    );
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Superuser gate: on denial it records `AuditEvent::PermissionDenied` and
/// returns SQLSTATE 42501.
fn require_superuser(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser {
        return Ok(());
    }
    state.audit_record_with_db(
        AuditEvent::PermissionDenied,
        Some(identity.tenant_id),
        None,
        &identity.username,
        action,
    );
    Err(DdlError::new(
        "42501",
        format!("permission denied: {action} requires superuser"),
    ))
}
