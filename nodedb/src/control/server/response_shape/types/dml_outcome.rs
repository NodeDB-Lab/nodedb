// SPDX-License-Identifier: BUSL-1.1

//! The count-bearing result of one write task, and the fold of every task's
//! result into the ONE command tag a statement answers with.
//!
//! Carries no pgwire wire types, so every protocol renders it its own way.

/// The count-bearing result of one write task, before any protocol renders it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DmlOutcome {
    /// The command verb, exactly as the tag names it (`INSERT`, `UPDATE`, ...).
    pub verb: &'static str,
    /// Rows this task affected.
    pub affected: u64,
}

/// Two tasks of one statement reported verbs that cannot share one tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DmlFoldError {
    #[error("one statement reported two command verbs: {first} then {second}")]
    VerbMismatch {
        first: &'static str,
        second: &'static str,
    },
}

/// The one command tag a statement answers with, folded over its tasks.
///
/// Fold rules:
/// - Same verb: `affected` sums.
/// - `INSERT` mixed with `UPDATE`: verb `INSERT`, `affected` sums. This is a
///   multi-row `INSERT ... ON CONFLICT DO UPDATE` whose rows resolved
///   differently (`PlanKind::DmlResultByOp`); Postgres tags it `INSERT 0 n`.
/// - Any other verb mix: [`DmlFoldError::VerbMismatch`]. Never a silent pick.
/// - An opaque task (`PlanKind::Execution`) adds nothing when a DML outcome
///   is folded before or after it. Only-opaque folds render as `OK`.
#[derive(Debug, Default)]
pub struct StatementTag {
    dml: Option<DmlOutcome>,
    opaque: bool,
}

/// What a statement's fold produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldedTag {
    Dml(DmlOutcome),
    /// Only opaque tasks were folded. Renders as the `OK` tag.
    Opaque,
}

impl StatementTag {
    /// Fold one task's count-bearing outcome into the statement's tag.
    pub fn fold(&mut self, outcome: DmlOutcome) -> Result<(), DmlFoldError> {
        let Some(current) = self.dml else {
            self.dml = Some(outcome);
            return Ok(());
        };
        let verb = merged_verb(current.verb, outcome.verb).ok_or(DmlFoldError::VerbMismatch {
            first: current.verb,
            second: outcome.verb,
        })?;
        self.dml = Some(DmlOutcome {
            verb,
            affected: current.affected.saturating_add(outcome.affected),
        });
        Ok(())
    }

    /// Fold one opaque task: no count, no verb.
    pub fn fold_opaque(&mut self) {
        self.opaque = true;
    }

    /// The statement's tag. `None` when nothing was folded.
    pub fn finish(self) -> Option<FoldedTag> {
        match (self.dml, self.opaque) {
            (Some(outcome), _) => Some(FoldedTag::Dml(outcome)),
            (None, true) => Some(FoldedTag::Opaque),
            (None, false) => None,
        }
    }
}

/// The verb two folded outcomes share, `None` when they cannot share one.
fn merged_verb(first: &'static str, second: &'static str) -> Option<&'static str> {
    if first == second {
        return Some(first);
    }
    match (first, second) {
        ("INSERT", "UPDATE") | ("UPDATE", "INSERT") => Some("INSERT"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(verb: &'static str, affected: u64) -> DmlOutcome {
        DmlOutcome { verb, affected }
    }

    #[test]
    fn empty_fold_finishes_to_none() {
        assert_eq!(StatementTag::default().finish(), None);
    }

    #[test]
    fn same_verb_sums_affected() {
        let mut tag = StatementTag::default();
        tag.fold(outcome("INSERT", 1)).expect("first fold");
        tag.fold(outcome("INSERT", 0)).expect("second fold");
        tag.fold(outcome("INSERT", 1)).expect("third fold");
        assert_eq!(tag.finish(), Some(FoldedTag::Dml(outcome("INSERT", 2))));
    }

    #[test]
    fn insert_and_update_fold_to_insert_in_either_order() {
        let mut tag = StatementTag::default();
        tag.fold(outcome("INSERT", 1)).expect("insert");
        tag.fold(outcome("UPDATE", 2)).expect("update after insert");
        assert_eq!(tag.finish(), Some(FoldedTag::Dml(outcome("INSERT", 3))));

        let mut tag = StatementTag::default();
        tag.fold(outcome("UPDATE", 2)).expect("update");
        tag.fold(outcome("INSERT", 1)).expect("insert after update");
        assert_eq!(tag.finish(), Some(FoldedTag::Dml(outcome("INSERT", 3))));
    }

    #[test]
    fn other_verb_mix_is_an_error() {
        let mut tag = StatementTag::default();
        tag.fold(outcome("INSERT", 1)).expect("insert");
        assert_eq!(
            tag.fold(outcome("DELETE", 1)),
            Err(DmlFoldError::VerbMismatch {
                first: "INSERT",
                second: "DELETE",
            })
        );
    }

    #[test]
    fn opaque_never_changes_a_dml_tag() {
        let mut tag = StatementTag::default();
        tag.fold_opaque();
        tag.fold(outcome("DELETE", 4)).expect("delete");
        tag.fold_opaque();
        assert_eq!(tag.finish(), Some(FoldedTag::Dml(outcome("DELETE", 4))));
    }

    #[test]
    fn only_opaque_finishes_to_opaque() {
        let mut tag = StatementTag::default();
        tag.fold_opaque();
        tag.fold_opaque();
        assert_eq!(tag.finish(), Some(FoldedTag::Opaque));
    }
}
