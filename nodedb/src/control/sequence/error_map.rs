// SPDX-License-Identifier: BUSL-1.1

//! Maps a [`SequenceError`] onto the planner's `SqlError` and the crate's
//! `Error`, with one SQLSTATE per cause on both surfaces: a missing sequence
//! is `42704` (`undefined_object`) and every other registry refusal is
//! `55000` (`object_not_in_prerequisite_state`).

use nodedb_sql::SqlError;

use super::types::SequenceError;

/// The function exists, the object does not — SQLSTATE `42704`, never `42883`.
pub(crate) fn undefined_sequence(name: &str) -> SqlError {
    SqlError::UndefinedObject {
        kind: "sequence",
        name: name.to_string(),
    }
}

/// Map a registry error onto its planner equivalent.
pub(crate) fn map_sequence_error(name: &str, error: SequenceError) -> SqlError {
    match error {
        SequenceError::NotFound { .. } => undefined_sequence(name),
        other => SqlError::ObjectNotInPrerequisiteState {
            object: name.to_string(),
            detail: other.to_string(),
        },
    }
}

/// Map a registry error onto the crate error the response shaper returns.
///
/// Mirrors [`map_sequence_error`] variant for variant, the way
/// `plan_error_map` carries `SqlError::UndefinedObject` and
/// `SqlError::ObjectNotInPrerequisiteState` into `crate::Error`, so a
/// per-row accessor error renders the same SQLSTATE a plan-time one does.
pub(crate) fn sequence_error_to_error(name: &str, error: SequenceError) -> crate::Error {
    match error {
        SequenceError::NotFound { .. } => crate::Error::UndefinedObject {
            kind: "sequence",
            name: name.to_string(),
        },
        other => crate::Error::ObjectNotInPrerequisiteState {
            object: name.to_string(),
            detail: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_is_undefined_object_on_both_surfaces() {
        let sql = map_sequence_error("s", SequenceError::NotFound { name: "s".into() });
        assert!(matches!(
            sql,
            SqlError::UndefinedObject {
                kind: "sequence",
                ..
            }
        ));
        let err = sequence_error_to_error("s", SequenceError::NotFound { name: "s".into() });
        assert!(
            matches!(err, crate::Error::UndefinedObject { kind: "sequence", ref name } if name == "s")
        );
    }

    #[test]
    fn exhausted_is_prerequisite_state_on_both_surfaces() {
        let sql = map_sequence_error("s", SequenceError::Exhausted { name: "s".into() });
        assert!(matches!(sql, SqlError::ObjectNotInPrerequisiteState { .. }));
        let err = sequence_error_to_error("s", SequenceError::Exhausted { name: "s".into() });
        assert!(matches!(
            err,
            crate::Error::ObjectNotInPrerequisiteState { ref object, .. } if object == "s"
        ));
    }
}
