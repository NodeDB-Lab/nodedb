// SPDX-License-Identifier: BUSL-1.1

//! The count-bearing result of one write task, and the fold of every task's
//! result into the ONE command tag a statement answers with.
//!
//! Carries no pgwire wire types, so every protocol renders it its own way.
//! The payload-to-outcome readers here are the one place a Data Plane
//! count payload becomes a [`DmlOutcome`]; pgwire and native both call them.

use crate::control::server::shared::sql::staging_predicates::{
    StagedTagKind, extract_kv_conflict_op, require_affected_count,
};

use super::PlanKind;

/// The count-bearing result of one write task, before any protocol renders it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DmlOutcome {
    /// The command verb, exactly as the tag names it (`INSERT`, `UPDATE`, ...).
    pub verb: &'static str,
    /// Rows this task affected.
    pub affected: u64,
}

impl DmlOutcome {
    /// Whether this outcome reports a row count. See [`Self::verb_carries_count`].
    pub fn carries_count(&self) -> bool {
        Self::verb_carries_count(self.verb)
    }

    /// Whether `verb` reports a row count. A SQL `TRUNCATE` never does:
    /// Postgres tags it bare, and native leaves `rows_affected` unset. Every
    /// other verb carries `affected`. Both protocols read this one rule.
    pub fn verb_carries_count(verb: &str) -> bool {
        verb != "TRUNCATE"
    }
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

/// The count-bearing outcome of a staged write, from the neutral
/// [`StagedTagKind`] the staging gate decided. Verb mapping: `INSERT` /
/// `UPDATE` / `DELETE` by kind, and for a KV `InsertOnConflictUpdate` the
/// verb the stage handler resolved to.
pub(crate) fn staged_dml_outcome(kind: StagedTagKind, affected: usize) -> DmlOutcome {
    let verb = match kind {
        StagedTagKind::Insert => "INSERT",
        StagedTagKind::Update => "UPDATE",
        StagedTagKind::Delete => "DELETE",
        StagedTagKind::KvUpsert { updated: true } => "UPDATE",
        StagedTagKind::KvUpsert { updated: false } => "INSERT",
        // Matches the autocommit `DocumentOp::Upsert` / `KvOp::Put` tag
        // exactly: always the literal `UPSERT` command, regardless of
        // insert-vs-update outcome (see `describe_plan`'s `DmlResult("UPSERT")`
        // arms).
        StagedTagKind::Upsert => "UPSERT",
        // Statement-time in-transaction MERGE: the Postgres command tag for a
        // MERGE is `MERGE <total-rows-affected>` across all arms.
        StagedTagKind::Merge => "MERGE",
        // Statement-time in-transaction `UPDATE ... FROM`: an UPDATE reports the
        // Postgres `UPDATE <n>` command tag over the matched target rows.
        StagedTagKind::UpdateFromJoin => "UPDATE",
        // KV `Incr` / `IncrFloat` / `Cas` / `GetSet` never reach a tag: their
        // sole SQL surface (`SELECT KV_INCR(..)` and friends, in
        // `ddl/neutral/kv_atomic/`) reads `StagedWriteOutcome::payload`
        // directly, and both dispatch loops fold `RawPayload` as opaque. This
        // arm exists only so the match stays exhaustive against a new
        // `PhysicalPlan::Kv` caller; it names the tag a function-call
        // `SELECT` renders.
        StagedTagKind::RawPayload => "SELECT",
        // Staged `TRUNCATE`: `carries_count` is false for this verb, so the
        // wire answer is the bare `TRUNCATE` autocommit answers with.
        StagedTagKind::Truncate => "TRUNCATE",
    };
    DmlOutcome {
        verb,
        affected: affected as u64,
    }
}

/// The count-bearing outcome a `DmlResult(verb)` payload reports.
///
/// The count comes from the write, always. There is no "point operations
/// affected exactly 1 row" shortcut: a point delete or a conflicting
/// `ON CONFLICT DO NOTHING` insert is the same plan whether it touched a row
/// or not, so assuming 1 here reported rows that were never there.
pub(crate) fn dml_outcome_from_payload(
    payload: &[u8],
    verb: &'static str,
) -> crate::Result<DmlOutcome> {
    let affected = require_affected_count(payload).map_err(|e| crate::Error::Internal {
        detail: format!("{verb} response is missing its affected count: {e}"),
    })?;
    Ok(DmlOutcome { verb, affected })
}

/// The count-bearing outcome a `DmlResultByOp` payload reports.
///
/// The handler decides insert-vs-update at apply time and reports it as
/// `op`. A missing or unknown verb is a handler bug, never a default tag.
pub(crate) fn dml_outcome_by_op(payload: &[u8]) -> crate::Result<DmlOutcome> {
    let affected = require_affected_count(payload).map_err(|e| crate::Error::Internal {
        detail: format!("DmlResultByOp response is missing its affected count: {e}"),
    })?;
    let verb = match extract_kv_conflict_op(payload).as_deref() {
        Some("insert") => "INSERT",
        Some("update") => "UPDATE",
        other => {
            return Err(crate::Error::Internal {
                detail: format!(
                    "DmlResultByOp response carries no usable `op` verb \
                     (got {other:?}); the handler must report `insert` or `update`"
                ),
            });
        }
    };
    Ok(DmlOutcome { verb, affected })
}

/// The count-bearing outcome of a passthrough payload: `Some` for the
/// count-bearing kinds, `None` for an opaque `Execution`, an error for a
/// row-shaped kind (those never reach a tag).
pub(crate) fn payload_to_dml_outcome(
    payload: &[u8],
    kind: PlanKind,
) -> crate::Result<Option<DmlOutcome>> {
    match kind {
        PlanKind::Execution => Ok(None),
        PlanKind::DmlResult(verb) => dml_outcome_from_payload(payload, verb).map(Some),
        PlanKind::DmlResultByOp => dml_outcome_by_op(payload).map(Some),
        PlanKind::ArraySlice
        | PlanKind::ReturningRows
        | PlanKind::SingleDocument
        | PlanKind::MultiRow => Err(crate::Error::Internal {
            detail: format!("payload_to_dml_outcome cannot handle plan kind {kind:?}"),
        }),
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

    #[test]
    fn truncate_is_the_only_count_less_verb() {
        assert!(!outcome("TRUNCATE", 0).carries_count());
        for verb in ["INSERT", "UPDATE", "DELETE", "UPSERT", "MERGE"] {
            assert!(outcome(verb, 1).carries_count(), "{verb}");
        }
    }

    #[test]
    fn passthrough_rejects_precomposed_shapes() {
        assert!(payload_to_dml_outcome(&[], PlanKind::ArraySlice).is_err());
        assert!(payload_to_dml_outcome(&[], PlanKind::ReturningRows).is_err());
        assert!(payload_to_dml_outcome(&[], PlanKind::SingleDocument).is_err());
    }

    /// `KvOp::InsertOnConflictUpdate` reports the verb it resolved to; the
    /// outcome follows it, and a payload with no verb is refused rather than
    /// defaulted.
    #[test]
    fn dml_result_by_op_follows_the_reported_verb() {
        let update = nodedb_types::json_to_msgpack(&serde_json::json!({
            "affected": 1,
            "op": "update"
        }))
        .expect("encode payload");
        assert_eq!(
            payload_to_dml_outcome(&update, PlanKind::DmlResultByOp).expect("update outcome"),
            Some(outcome("UPDATE", 1))
        );

        let insert = nodedb_types::json_to_msgpack(&serde_json::json!({
            "affected": 1,
            "op": "insert"
        }))
        .expect("encode payload");
        assert_eq!(
            payload_to_dml_outcome(&insert, PlanKind::DmlResultByOp).expect("insert outcome"),
            Some(outcome("INSERT", 1))
        );

        let no_verb = nodedb_types::json_to_msgpack(&serde_json::json!({ "affected": 1 }))
            .expect("encode payload");
        assert!(payload_to_dml_outcome(&no_verb, PlanKind::DmlResultByOp).is_err());
    }

    /// A count-bearing response with no count is a handler bug, not a `1`.
    #[test]
    fn dml_outcome_requires_a_reported_count() {
        assert!(payload_to_dml_outcome(&[], PlanKind::DmlResult("DELETE")).is_err());
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "affected": 0 }))
            .expect("encode count payload");
        assert_eq!(
            payload_to_dml_outcome(&payload, PlanKind::DmlResult("DELETE")).expect("delete"),
            Some(outcome("DELETE", 0))
        );
    }

    /// The fold reads the neutral outcome: a count for the count-bearing
    /// kinds, nothing for an opaque execution, a refusal for row kinds.
    #[test]
    fn dml_outcome_follows_plan_kind() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "affected": 2 }))
            .expect("encode count payload");
        assert_eq!(
            payload_to_dml_outcome(&payload, PlanKind::DmlResult("INSERT")).expect("insert"),
            Some(outcome("INSERT", 2))
        );
        assert_eq!(
            payload_to_dml_outcome(&[], PlanKind::Execution).expect("opaque"),
            None
        );
        assert!(payload_to_dml_outcome(&[], PlanKind::MultiRow).is_err());
        assert!(payload_to_dml_outcome(&[], PlanKind::ReturningRows).is_err());
    }

    /// Staged outcomes carry the verb the staging gate decided.
    #[test]
    fn staged_outcome_maps_kind_to_verb() {
        assert_eq!(
            staged_dml_outcome(StagedTagKind::KvUpsert { updated: true }, 1),
            outcome("UPDATE", 1)
        );
        assert_eq!(
            staged_dml_outcome(StagedTagKind::Merge, 4),
            outcome("MERGE", 4)
        );
    }

    /// A staged `TRUNCATE` is the count-less verb.
    #[test]
    fn staged_truncate_carries_no_count() {
        let staged = staged_dml_outcome(StagedTagKind::Truncate, 0);
        assert_eq!(staged, outcome("TRUNCATE", 0));
        assert!(!staged.carries_count());
    }
}
