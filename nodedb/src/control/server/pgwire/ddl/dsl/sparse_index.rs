//! `CREATE SPARSE INDEX` DSL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;

/// CREATE SPARSE INDEX [name] ON <collection> (<field>)
pub fn create_sparse_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE SPARSE INDEX [name] ON <collection> (<field>)",
        ));
    }

    let (index_name, on_idx) = if parts[3].eq_ignore_ascii_case("ON") {
        ("_auto_sparse".to_string(), 3)
    } else {
        if parts.len() < 7 || !parts[4].eq_ignore_ascii_case("ON") {
            return Err(sqlstate_error("42601", "expected ON after index name"));
        }
        (parts[3].to_string(), 4)
    };

    let collection = parts
        .get(on_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "expected collection name after ON"))?;

    let field = parts
        .get(on_idx + 2)
        .map(|s| s.trim_matches(|c| c == '(' || c == ')'))
        .unwrap_or("_sparse");

    let tenant_id = identity.tenant_id;

    super::super::owner_propose::propose_owner(
        state,
        "sparse_index",
        tenant_id,
        &index_name,
        &identity.username,
    )?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created sparse index '{index_name}' on '{collection}' ({field})"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE SPARSE INDEX"))])
}
