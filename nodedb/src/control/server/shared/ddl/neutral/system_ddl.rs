// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `ALTER SYSTEM SET <field> = <value>` handler.
//!
//! System-level settings that change at runtime without a node
//! restart. The live cell lives on `SharedState::retention_settings`;
//! the collection-GC sweeper reads it on every tick.
//!
//! Currently supported fields:
//! - `deactivated_collection_retention_days` (u32)
//!
//! The superuser gate, token parsing, retention-settings write, and audit side
//! effect run here. The result is the protocol-neutral [`DdlResult`] with its
//! SQLSTATE codes and messages.

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`] from a SQLSTATE code and a message.
fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

pub fn alter_system(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err(
            "42501",
            "permission denied: only superuser can ALTER SYSTEM",
        ));
    }

    // Expected token shape:
    //   [0]=ALTER  [1]=SYSTEM  [2]=SET  [3]=<field>  [4]==  [5]=<value>
    // or without the `=` token:
    //   [0]=ALTER  [1]=SYSTEM  [2]=SET  [3]=<field>  [4]=<value>
    if parts.len() < 5 || !parts[2].eq_ignore_ascii_case("SET") {
        return Err(err("42601", "syntax: ALTER SYSTEM SET <field> = <value>"));
    }

    let field = parts[3].trim_end_matches(';').to_lowercase();
    let value_idx = if parts.len() > 5 && parts[4] == "=" {
        5
    } else {
        4
    };
    if value_idx >= parts.len() {
        return Err(err("42601", "expected value after field name"));
    }
    let raw_value = parts[value_idx].trim_end_matches(';');

    match field.as_str() {
        "deactivated_collection_retention_days" => {
            let days: u32 = raw_value.parse().map_err(|_| {
                err(
                    "42601",
                    "deactivated_collection_retention_days must be a non-negative integer",
                )
            })?;
            super::audit_intent::record_action_intent(
                state,
                identity,
                None,
                &format!("ALTER SYSTEM SET {field} = {days}"),
            )?;
            let mut w = state
                .retention_settings
                .write()
                .map_err(|_| err("58000", "retention settings lock poisoned"))?;
            w.deactivated_collection_retention_days = days;
            drop(w);
            state.audit_record(
                AuditEvent::AdminAction,
                None,
                &identity.username,
                &format!("altered system: set {field} = {days}"),
            );
            Ok(vec![DdlResult::Status {
                command: "ALTER SYSTEM".to_string(),
                rows_affected: None,
            }])
        }
        other => Err(err(
            "42601",
            format!(
                "unknown ALTER SYSTEM field: {other}. Valid: deactivated_collection_retention_days"
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    //! Integration-style coverage lives in
    //! `tests/collection_gc_retention.rs` — the ALTER SYSTEM path is
    //! exercised end-to-end there (propose → sweeper picks up new
    //! window on the next tick). The in-file suite would need a full
    //! `SharedState` fixture which is heavier than necessary for the
    //! parsing logic.
}
