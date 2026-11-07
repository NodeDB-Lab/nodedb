// SPDX-License-Identifier: BUSL-1.1

//! The durable record of a statement's catalog change, written before the change commits.
//!
//! The applier writes the `DdlChange` row after the commit, so a failed row cannot undo it.
//! This `ActionIntent` row runs first, so a failed record refuses the statement.
//! A row with no later `DdlChange` row names an action that did not commit.
//! An entry with no statement behind it, such as a background purge, writes no intent.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::audit::{AuditAuth, AuditEvent, AuditRecordInput};
use crate::control::server::shared::session::audit_context::AuditCtx;
use crate::control::state::SharedState;
use crate::error::Error;

/// Record each statement's intent for `entries`, with one sync.
///
/// Returns once every row is durable. An error means the caller must not propose.
pub(crate) fn record_ddl_intents(
    shared: &SharedState,
    entries: &[(&CatalogEntry, Option<&AuditCtx>)],
) -> Result<(), Error> {
    let rows: Vec<(AuditAuth, String, &str)> = entries
        .iter()
        .filter(|(entry, _)| !entry.is_progress_mark())
        .filter_map(|(entry, ctx)| {
            let ctx = (*ctx)?;
            let auth = AuditAuth {
                user_id: ctx.auth_user_id.clone(),
                user_name: ctx.auth_user_name.clone(),
                session_id: String::new(),
            };
            let detail = format!("DDL intent: {} {}", entry.kind(), ctx.sql_text);
            Some((auth, detail, ctx.auth_user_name.as_str()))
        })
        .collect();
    if rows.is_empty() {
        return Ok(());
    }
    shared.audit_record_batch_strict(rows.iter().map(|(auth, detail, source)| AuditRecordInput {
        event: AuditEvent::ActionIntent,
        tenant_id: None,
        database_id: None,
        source,
        detail,
        auth,
    }))
}
