//! `CREATE SEARCH INDEX` DSL handler (higher-level alias for fulltext).

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;

/// CREATE SEARCH INDEX ON <collection> FIELDS <field1>[, <field2>...] [ANALYZER '<name>'] [FUZZY true|false]
pub fn create_search_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    let on_pos = upper.find(" ON ").ok_or_else(|| {
        sqlstate_error(
            "42601",
            "syntax: CREATE SEARCH INDEX ON <collection> FIELDS <field> [ANALYZER 'name'] [FUZZY true]",
        )
    })?;
    let after_on = sql[on_pos + 4..].trim_start();
    let fields_pos = upper.find(" FIELDS ").ok_or_else(|| {
        sqlstate_error(
            "42601",
            "syntax: CREATE SEARCH INDEX ON <collection> FIELDS <field> [ANALYZER 'name'] [FUZZY true]",
        )
    })?;

    let collection = after_on[..fields_pos - on_pos - 4].trim().to_lowercase();
    if collection.is_empty() {
        return Err(sqlstate_error("42601", "missing collection name"));
    }

    let after_fields = &sql[fields_pos + 8..];
    let fields_end = upper[fields_pos + 8..]
        .find(" ANALYZER ")
        .or_else(|| upper[fields_pos + 8..].find(" FUZZY "))
        .unwrap_or(after_fields.len());
    let fields_str = after_fields[..fields_end].trim();
    let fields: Vec<&str> = fields_str.split(',').map(|s| s.trim()).collect();

    if fields.is_empty() || fields[0].is_empty() {
        return Err(sqlstate_error("42601", "missing field list"));
    }

    let tenant_id = identity.tenant_id;

    for field in &fields {
        let index_name = format!("fts_{}_{}", collection, field);

        super::super::owner_propose::propose_owner(
            state,
            "fulltext_index",
            tenant_id,
            &index_name,
            &identity.username,
        )?;

        state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!("created search index '{index_name}' on '{collection}' ({field})"),
        );
    }

    Ok(vec![Response::Execution(Tag::new("CREATE SEARCH INDEX"))])
}
