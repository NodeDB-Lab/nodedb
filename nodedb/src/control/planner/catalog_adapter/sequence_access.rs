// SPDX-License-Identifier: BUSL-1.1

//! Sequence accessor bodies behind `OriginCatalog`'s `SqlCatalog` methods.
//!
//! `nextval` advances the node counter AND records the value in the calling
//! session's map. `currval` reads only that session map, so one session never
//! reports another session's allocation.

use nodedb_sql::SqlError;

use crate::control::sequence::{SequenceError, SequenceRegistry};

use super::adapter::OriginCatalog;

impl OriginCatalog {
    /// Advance `name` and record the value as this session's `currval`.
    pub(super) fn advance_sequence(&self, name: &str) -> Result<i64, SqlError> {
        let registry = self.require_sequence_registry()?;
        let database_id = self.database_id.as_u64();
        let value = registry
            .nextval(database_id, self.tenant_id, name)
            .map_err(|e| map_sequence_error(name, e))?;
        if let Some(session) = &self.session_sequences {
            session.record(database_id, self.tenant_id, name, value);
        }
        Ok(value)
    }

    /// The last value THIS SESSION obtained from `nextval` on `name`.
    pub(super) fn session_sequence_value(&self, name: &str) -> Result<i64, SqlError> {
        // The sequence must exist before its absence from the session map can
        // mean "not yet called in this session" rather than "no such object".
        let registry = self.require_sequence_registry()?;
        if !registry.exists(self.database_id.as_u64(), self.tenant_id, name) {
            return Err(undefined_sequence(name));
        }
        self.session_sequences
            .as_ref()
            .and_then(|session| session.last(self.database_id.as_u64(), self.tenant_id, name))
            .ok_or_else(|| SqlError::ObjectNotInPrerequisiteState {
                object: name.to_string(),
                detail: format!(
                    "currval of sequence \"{name}\" is not yet defined in this session"
                ),
            })
    }

    /// Position `name` so the next `nextval` returns `value + increment`.
    pub(super) fn position_sequence(&self, name: &str, value: i64) -> Result<i64, SqlError> {
        let registry = self.require_sequence_registry()?;
        registry
            .setval(self.database_id.as_u64(), self.tenant_id, name, value)
            .map_err(|e| map_sequence_error(name, e))
    }

    fn require_sequence_registry(&self) -> Result<&SequenceRegistry, SqlError> {
        self.sequence_registry
            .as_deref()
            .ok_or_else(|| SqlError::ObjectNotInPrerequisiteState {
                object: "sequence".into(),
                detail: "sequence access unavailable: this planner holds no sequence registry"
                    .into(),
            })
    }
}

/// The function exists, the object does not — SQLSTATE `42704`, never `42883`.
fn undefined_sequence(name: &str) -> SqlError {
    SqlError::UndefinedObject {
        kind: "sequence",
        name: name.to_string(),
    }
}

/// Map a registry error onto its planner equivalent.
fn map_sequence_error(name: &str, error: SequenceError) -> SqlError {
    match error {
        SequenceError::NotFound { .. } => undefined_sequence(name),
        other => SqlError::ObjectNotInPrerequisiteState {
            object: name.to_string(),
            detail: other.to_string(),
        },
    }
}
