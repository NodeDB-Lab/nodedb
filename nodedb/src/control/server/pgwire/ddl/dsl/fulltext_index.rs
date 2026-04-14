//! `CREATE FULLTEXT INDEX` DSL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;

/// CREATE FULLTEXT INDEX <name> ON <collection> (<field>)
pub fn create_fulltext_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 7 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE FULLTEXT INDEX <name> ON <collection> (<field>)",
        ));
    }

    let index_name = parts[3];
    if !parts[4].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after index name"));
    }
    let collection = parts[5];
    let field = parts[6].trim_matches(|c| c == '(' || c == ')');
    let tenant_id = identity.tenant_id;

    super::super::owner_propose::propose_owner(
        state,
        "fulltext_index",
        tenant_id,
        index_name,
        &identity.username,
    )?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created fulltext index '{index_name}' on '{collection}' ({field})"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE FULLTEXT INDEX"))])
}
