//! Index DDL: CREATE INDEX, DROP INDEX, SHOW INDEXES.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{sqlstate_error, text_field};

/// CREATE [UNIQUE] INDEX <name> ON <collection> (<field>) [WHERE condition]
///
/// Creates an index owned by the collection's owner.
/// UNIQUE enforces uniqueness on the indexed field. WHERE makes it conditional.
pub fn create_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    // Detect UNIQUE modifier.
    let is_unique = upper.contains("UNIQUE INDEX");
    let idx_offset = if is_unique { 3 } else { 2 }; // skip "CREATE UNIQUE INDEX" vs "CREATE INDEX"

    if parts.len() < idx_offset + 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE [UNIQUE] INDEX <name> ON <collection> (<field>) [WHERE ...]",
        ));
    }

    let index_name = parts[idx_offset];
    if !parts[idx_offset + 1].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after index name"));
    }

    // Handle both "collection (field)" and "collection(field)" formats.
    let (collection, field) = {
        let raw = parts[idx_offset + 2];
        if let Some(paren_pos) = raw.find('(') {
            // Combined: "indexed_col(role)"
            let coll = &raw[..paren_pos];
            let fld = raw[paren_pos..].trim_matches(|c| c == '(' || c == ')');
            (coll.to_string(), fld.to_string())
        } else if parts.len() > idx_offset + 3 {
            // Separate: "indexed_col" "(role)"
            let fld = parts[idx_offset + 3].trim_matches(|c| c == '(' || c == ')');
            (raw.to_string(), fld.to_string())
        } else {
            return Err(sqlstate_error(
                "42601",
                "syntax: CREATE [UNIQUE] INDEX <name> ON <collection> (<field>) [WHERE ...]",
            ));
        }
    };

    // Parse optional WHERE condition for conditional indexes.
    let where_condition = upper
        .find(" WHERE ")
        .map(|pos| sql[pos + 7..].trim().to_string());

    // Parse optional COLLATE NOCASE for case-insensitive indexes.
    let case_insensitive = upper.contains("COLLATE NOCASE") || upper.contains("COLLATE CI");
    let tenant_id = identity.tenant_id;

    // Verify collection exists and user has CREATE permission.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), &collection) {
            Ok(Some(coll)) if coll.is_active => {
                // Check: must be collection owner, superuser, or tenant_admin.
                let is_owner = coll.owner == identity.username;
                if !is_owner
                    && !identity.is_superuser
                    && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
                {
                    return Err(sqlstate_error(
                        "42501",
                        "permission denied: must be collection owner or admin to create indexes",
                    ));
                }
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{collection}' does not exist"),
                ));
            }
        }
    }

    // Index ownership inherits from the collection owner.
    let catalog = state.credentials.catalog();
    let index_owner = if let Some(cat) = catalog {
        cat.get_collection(tenant_id.as_u32(), &collection)
            .ok()
            .flatten()
            .map(|c| c.owner)
            .unwrap_or_else(|| identity.username.clone())
    } else {
        identity.username.clone()
    };

    state
        .permissions
        .set_owner(
            "index",
            tenant_id,
            index_name,
            &index_owner,
            catalog.as_ref(),
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let kind = if is_unique { "unique index" } else { "index" };
    let ci = if case_insensitive {
        " COLLATE NOCASE"
    } else {
        ""
    };
    let cond = where_condition
        .as_deref()
        .map(|c| format!(" WHERE {c}"))
        .unwrap_or_default();
    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created {kind} '{index_name}' on '{collection}' ({field}){ci}{cond}"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE INDEX"))])
}

/// DROP INDEX <name>
pub fn drop_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP INDEX <name>"));
    }

    let index_name = parts[2];
    let tenant_id = identity.tenant_id;

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner("index", tenant_id, index_name)
        .as_deref()
        == Some(&identity.username);

    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: must be index owner or admin",
        ));
    }

    // Remove ownership record.
    let catalog = state.credentials.catalog();
    state
        .permissions
        .remove_owner("index", tenant_id, index_name, catalog.as_ref())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("dropped index '{index_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP INDEX"))])
}

/// SHOW INDEXES [ON <collection>]
///
/// Lists indexes for the current tenant (optionally filtered by collection).
pub fn show_indexes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    // Parse optional ON <collection> filter.
    let filter_collection = if parts.len() >= 4
        && parts[1].eq_ignore_ascii_case("INDEXES")
        && parts[2].eq_ignore_ascii_case("ON")
    {
        Some(parts[3])
    } else {
        None
    };

    let schema = Arc::new(vec![
        text_field("index_name"),
        text_field("type"),
        text_field("owner"),
    ]);

    // List all index types for this tenant.
    let index_types = [
        ("index", "btree"),
        ("vector_index", "vector"),
        ("fulltext_index", "fulltext"),
        ("spatial_index", "spatial"),
    ];

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    for (owner_type, display_type) in &index_types {
        let indexes = state.permissions.list_owners(owner_type, tenant_id);
        for (index_name, owner) in &indexes {
            if let Some(coll) = filter_collection
                && !index_name.starts_with(coll)
            {
                continue;
            }

            encoder
                .encode_field(index_name)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(display_type)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(owner)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
