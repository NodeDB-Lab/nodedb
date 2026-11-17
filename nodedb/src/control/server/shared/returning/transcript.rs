// SPDX-License-Identifier: BUSL-1.1

//! Ordered sequence results retained by one source attempt.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use crate::control::sequence::SequenceAccess;

use super::precommit::PreparedReturningRow;

/// The evaluated arguments of one sequence invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AccessorInvocation {
    Nextval(String),
    Currval(String),
    Setval(String, i64),
}

pub(crate) type AccessorResult = Result<i64, AccessorError>;

/// The error one sequence invocation raised, kept so a replay raises the
/// error the first call raised, under the same SQLSTATE.
#[derive(Debug, Clone)]
pub(crate) enum AccessorError {
    /// The sequence does not exist: `42704`.
    Undefined { kind: &'static str, name: String },
    /// `currval` before the session obtained a value: `55000`.
    NotYetDefined { object: String, detail: String },
    /// Any other error, in its public form.
    Other(nodedb_types::NodeDbError),
}

impl AccessorError {
    fn of(error: crate::Error) -> Self {
        match error {
            crate::Error::UndefinedObject { kind, name } => Self::Undefined { kind, name },
            crate::Error::ObjectNotInPrerequisiteState { object, detail } => {
                Self::NotYetDefined { object, detail }
            }
            other => Self::Other(nodedb_types::NodeDbError::from(other)),
        }
    }

    fn to_error(&self) -> crate::Error {
        match self {
            Self::Undefined { kind, name } => crate::Error::UndefinedObject {
                kind,
                name: name.clone(),
            },
            Self::NotYetDefined { object, detail } => crate::Error::ObjectNotInPrerequisiteState {
                object: object.clone(),
                detail: detail.clone(),
            },
            Self::Other(error) => crate::Error::from(error.clone()),
        }
    }
}

/// Position of an expression within the bound prepared input.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AccessorOccurrence {
    pub row: usize,
    pub expression: usize,
}

pub(crate) type TranscriptEntry = (AccessorOccurrence, AccessorInvocation, AccessorResult);

/// One sequence change an evaluation made: a `nextval` or a `setval`, and
/// the value it returned.
pub(crate) type SequenceEffect = (AccessorInvocation, i64);

/// Exact prepared input and expression occurrences for one source attempt.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct EvaluationBinding {
    pub rows: Vec<PreparedReturningRow>,
    pub expressions: Vec<(String, nodedb_query::expr::SqlExpr)>,
    pub masked_rows: Vec<crate::control::server::response_shape::types::ShapedRow>,
    pub policy_version: u64,
    pub tenant_id: nodedb_types::TenantId,
    pub collection: String,
    pub roles: Vec<String>,
}

/// Sequence results in invocation order.
/// Retain this value across retries of the same source attempt.
#[derive(Debug, Default)]
pub(crate) struct AccessorTranscript {
    entries: Mutex<Vec<TranscriptEntry>>,
    started: AtomicBool,
    binding: Mutex<Option<EvaluationBinding>>,
}

impl AccessorTranscript {
    /// Check complete input membership and expression order before sequence access.
    pub(super) fn bind(&self, binding: EvaluationBinding) -> crate::Result<()> {
        let mut previous = self
            .binding
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match previous.as_ref() {
            Some(previous) if previous != &binding => {
                Err(crate::Error::CalvinSerializationConflict)
            }
            Some(_) => Ok(()),
            None => {
                *previous = Some(binding);
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn entries(&self) -> Vec<TranscriptEntry> {
        self.lock_entries().clone()
    }

    /// Every `nextval` and `setval` that returned a value, in invocation
    /// order. A `currval` changes no sequence, and an error changed none.
    pub(crate) fn sequence_effects(&self) -> Vec<SequenceEffect> {
        self.lock_entries()
            .iter()
            .filter_map(|(_, invocation, result)| match (invocation, result) {
                (AccessorInvocation::Currval(_), _) | (_, Err(_)) => None,
                (invocation, Ok(value)) => Some((invocation.clone(), *value)),
            })
            .collect()
    }

    fn lock_entries(&self) -> MutexGuard<'_, Vec<TranscriptEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(super) fn begin<'a>(&'a self, access: &'a dyn SequenceAccess) -> TranscriptAccess<'a> {
        TranscriptAccess {
            transcript: self,
            access,
            replay: self.started.swap(true, Ordering::AcqRel),
            cursor: Cell::new(0),
            occurrence: Cell::new(AccessorOccurrence::default()),
        }
    }
}

pub(super) struct TranscriptAccess<'a> {
    transcript: &'a AccessorTranscript,
    access: &'a dyn SequenceAccess,
    replay: bool,
    cursor: Cell<usize>,
    occurrence: Cell<AccessorOccurrence>,
}

impl TranscriptAccess<'_> {
    pub(super) fn set_occurrence(&self, row: usize, expression: usize) {
        self.occurrence.set(AccessorOccurrence { row, expression });
    }

    fn invoke(&self, invocation: AccessorInvocation) -> crate::Result<i64> {
        let index = self.cursor.get();
        if self.replay {
            let entries = self.transcript.lock_entries();
            let Some((occurrence, previous, result)) = entries.get(index) else {
                return Err(crate::Error::CalvinSerializationConflict);
            };
            if previous != &invocation || *occurrence != self.occurrence.get() {
                return Err(crate::Error::CalvinSerializationConflict);
            }
            self.cursor.set(index + 1);
            return result.clone().map_err(|error| error.to_error());
        }
        let result = match &invocation {
            AccessorInvocation::Nextval(name) => self.access.nextval(name),
            AccessorInvocation::Currval(name) => self.access.currval(name),
            AccessorInvocation::Setval(name, value) => self.access.setval(name, *value),
        };
        let result = result.map_err(AccessorError::of);
        self.transcript
            .lock_entries()
            .push((self.occurrence.get(), invocation, result.clone()));
        self.cursor.set(index + 1);
        result.map_err(|error| error.to_error())
    }

    pub(super) fn finish(&self) -> crate::Result<()> {
        if self.cursor.get() != self.transcript.lock_entries().len() {
            return Err(crate::Error::CalvinSerializationConflict);
        }
        Ok(())
    }
}

impl SequenceAccess for TranscriptAccess<'_> {
    fn nextval(&self, name: &str) -> crate::Result<i64> {
        self.invoke(AccessorInvocation::Nextval(name.to_owned()))
    }

    fn currval(&self, name: &str) -> crate::Result<i64> {
        self.invoke(AccessorInvocation::Currval(name.to_owned()))
    }

    fn setval(&self, name: &str, value: i64) -> crate::Result<i64> {
        self.invoke(AccessorInvocation::Setval(name.to_owned(), value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Access(Cell<i64>);
    impl SequenceAccess for Access {
        fn nextval(&self, _: &str) -> crate::Result<i64> {
            self.0.set(self.0.get() + 1);
            Ok(self.0.get())
        }
        fn currval(&self, _: &str) -> crate::Result<i64> {
            Ok(self.0.get())
        }
        fn setval(&self, _: &str, value: i64) -> crate::Result<i64> {
            self.0.set(value);
            Ok(value)
        }
    }

    #[test]
    fn identical_replay_reuses_results_without_setval_or_nextval() {
        let access = Access(Cell::new(0));
        let transcript = AccessorTranscript::default();
        let first = transcript.begin(&access);
        assert_eq!(first.nextval("s").expect("nextval"), 1);
        assert_eq!(first.setval("s", 40).expect("setval"), 40);
        first.finish().expect("finish");
        access.0.set(100);
        let replay = transcript.begin(&access);
        assert_eq!(replay.nextval("s").expect("replay nextval"), 1);
        assert_eq!(replay.setval("s", 40).expect("replay setval"), 40);
        replay.finish().expect("finish replay");
        assert_eq!(access.0.get(), 100);
        assert_eq!(transcript.entries().len(), 2);
        assert_eq!(
            transcript.sequence_effects(),
            vec![
                (AccessorInvocation::Nextval("s".into()), 1),
                (AccessorInvocation::Setval("s".into(), 40), 40),
            ]
        );
    }

    struct Fresh;
    impl SequenceAccess for Fresh {
        fn nextval(&self, name: &str) -> crate::Result<i64> {
            Err(crate::Error::UndefinedObject {
                kind: "sequence",
                name: name.into(),
            })
        }
        fn currval(&self, name: &str) -> crate::Result<i64> {
            Err(crate::Error::ObjectNotInPrerequisiteState {
                object: name.into(),
                detail: format!(
                    "currval of sequence \"{name}\" is not yet defined in this session"
                ),
            })
        }
        fn setval(&self, _: &str, value: i64) -> crate::Result<i64> {
            Ok(value)
        }
    }

    /// An accessor error keeps its type, on the first call and on a
    /// replay, so a refusal keeps SQLSTATE 55000 or 42704.
    #[test]
    fn accessor_errors_keep_their_type_on_replay() {
        let transcript = AccessorTranscript::default();
        for _ in 0..2 {
            let access = transcript.begin(&Fresh);
            assert!(matches!(
                access.currval("s"),
                Err(crate::Error::ObjectNotInPrerequisiteState { .. })
            ));
            assert!(matches!(
                access.nextval("gone"),
                Err(crate::Error::UndefinedObject {
                    kind: "sequence",
                    ..
                })
            ));
        }
    }

    #[test]
    fn a_currval_is_no_sequence_effect() {
        let access = Access(Cell::new(3));
        let transcript = AccessorTranscript::default();
        let first = transcript.begin(&access);
        assert_eq!(first.currval("s").expect("currval"), 3);
        first.finish().expect("finish");
        assert!(transcript.sequence_effects().is_empty());
    }

    #[test]
    fn changed_kind_arguments_or_length_conflict_without_new_calls() {
        let access = Access(Cell::new(0));
        let transcript = AccessorTranscript::default();
        transcript.begin(&access).setval("s", 5).expect("setval");
        assert!(matches!(
            transcript.begin(&access).setval("s", 6),
            Err(crate::Error::CalvinSerializationConflict)
        ));
        assert!(matches!(
            transcript.begin(&access).setval("other", 5),
            Err(crate::Error::CalvinSerializationConflict)
        ));
        assert!(matches!(
            transcript.begin(&access).currval("s"),
            Err(crate::Error::CalvinSerializationConflict)
        ));
        assert!(matches!(
            transcript.begin(&access).finish(),
            Err(crate::Error::CalvinSerializationConflict)
        ));
        let replay = transcript.begin(&access);
        replay.setval("s", 5).expect("replay");
        assert!(matches!(
            replay.nextval("s"),
            Err(crate::Error::CalvinSerializationConflict)
        ));
        assert_eq!(access.0.get(), 5);
    }
    #[test]
    fn identical_arguments_at_another_expression_occurrence_conflict_without_calls() {
        let access = Access(Cell::new(0));
        let transcript = AccessorTranscript::default();
        let first = transcript.begin(&access);
        first.set_occurrence(0, 1);
        first.nextval("s").expect("first occurrence");
        for (row, expression) in [(0, 0), (1, 1)] {
            let replay = transcript.begin(&access);
            replay.set_occurrence(row, expression);
            assert!(matches!(
                replay.nextval("s"),
                Err(crate::Error::CalvinSerializationConflict)
            ));
        }
        assert_eq!(access.0.get(), 1);
    }
}
