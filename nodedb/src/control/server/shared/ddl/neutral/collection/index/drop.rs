// SPDX-License-Identifier: BUSL-1.1

//! `DROP [<KIND>] INDEX [IF EXISTS] <name>`.
//!
//! The name is resolved in the catalog index registry, which every
//! `CREATE ... INDEX` path registers into, so an index that `SHOW INDEXES`
//! lists is an index this statement can drop — whatever engine backs it.
//! Three outcomes, and no others:
//!
//! - the name resolves: the kind's own teardown runs, and only if it
//!   succeeds are the registry and ownership rows removed;
//! - the name does not resolve: `42704`, or a no-op when `IF EXISTS` is
//!   given;
//! - the name resolves to a different kind than a qualified statement asked
//!   for: `42809`, naming the kind it actually is.
//!
//! Reporting success without removing anything is not one of them, for
//! every kind including `Secondary`.

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::{IndexKind, StoredIndexRecord};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::ddl_buffer;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::super::result::{DdlError, DdlResult};
use super::commit::err;

/// Parsed `DROP INDEX` request.
#[derive(Clone, Copy)]
pub struct DropIndexRequest<'a> {
    pub index_name: &'a str,
    pub if_exists: bool,
    /// Kind qualifier from `DROP VECTOR|FULLTEXT|SPATIAL|SPARSE INDEX`.
    /// `None` for the unqualified `DROP INDEX`, which accepts any kind.
    pub kind: Option<IndexKind>,
    pub database_id: DatabaseId,
}

/// The command tag reported for a request, matching the spelling used.
fn command_tag(kind: Option<IndexKind>) -> String {
    match kind.and_then(|k| k.drop_keyword()) {
        Some(keyword) => format!("DROP {keyword} INDEX"),
        None => "DROP INDEX".to_string(),
    }
}

/// DROP [VECTOR|FULLTEXT|SPATIAL|SPARSE] INDEX [IF EXISTS] <name>
pub async fn drop_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &DropIndexRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let DropIndexRequest {
        index_name,
        if_exists,
        kind,
        database_id,
    } = *req;
    if index_name.is_empty() {
        return Err(err("42601", "syntax: DROP INDEX [IF EXISTS] <name>"));
    }
    let tenant_id = identity.tenant_id;
    let tag = command_tag(kind);

    let record = state
        .credentials
        .catalog()
        .get_index_record(database_id.as_u64(), tenant_id.as_u64(), index_name)
        .map_err(|e| err("XX000", e.to_string()))?
        // An index whose collection is soft-deleted is not listed and cannot
        // be dropped on its own: the collection owns its lifecycle, and
        // UNDROP must bring it back intact.
        .filter(StoredIndexRecord::is_visible);

    let Some(record) = record else {
        if if_exists {
            return Ok(vec![DdlResult::Status {
                command: tag,
                rows_affected: None,
            }]);
        }
        return Err(err("42704", format!("index '{index_name}' does not exist")));
    };

    if let Some(requested) = kind
        && requested != record.kind
    {
        return Err(err(
            "42809",
            format!(
                "index '{index_name}' is a {} index, not a {} index",
                record.kind.display_type(),
                requested.display_type()
            ),
        ));
    }

    // Ownership check against the row this index's kind files under.
    let is_owner = state
        .permissions
        .get_owner(
            record.kind.owner_object_type(),
            database_id.as_u64(),
            tenant_id,
            index_name,
        )
        .as_deref()
        == Some(&identity.username);

    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(err(
            "42501",
            "permission denied: must be index owner or admin",
        ));
    }

    // Autocommit: engine and kind-specific catalog state first. If any of it
    // survives, the identity record survives with it so the drop can be
    // retried. Inside an explicit transaction every entry is buffered and
    // the engine teardown waits for COMMIT, so the records are buffered
    // first and the teardown's deferred effects ride on this statement's
    // own entries: a ROLLBACK TO SAVEPOINT then discards them together.
    let in_transaction = ddl_buffer::is_active();
    if !in_transaction {
        super::teardown::teardown(state, &record, database_id, tenant_id).await?;
    }

    super::super::super::super::index_registry::propose_delete_index_record(
        state,
        database_id,
        tenant_id,
        index_name,
        &record.collection,
    )?;

    crate::control::server::shared::ddl::owner::propose_delete_owner(
        state,
        record.kind.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        index_name,
    )?;

    if in_transaction {
        super::teardown::teardown(state, &record, database_id, tenant_id).await?;
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "dropped {} index '{index_name}' on '{}'",
            record.kind.display_type(),
            record.collection
        ),
    );

    Ok(vec![DdlResult::Status {
        command: tag,
        rows_affected: None,
    }])
}
