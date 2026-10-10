// SPDX-License-Identifier: BUSL-1.1

//! The count-bearing result of one write task, and the fold of every task's
//! result into the ONE command tag a statement answers with.
//!
//! Carries no pgwire wire types, so every protocol renders it its own way.
//! The payload-to-outcome readers here are the one place a Data Plane
//! count payload becomes a [`DmlOutcome`]; pgwire and native both call them.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::control::planner::calvin::write_class::{
    plan_counts_toward_statement_tag, plans_have_user_write,
};
use crate::control::server::shared::sql::staging_predicates::{
    StagedTagKind, extract_ingest_rejections, extract_kv_conflict_op, rejected_lines_notice,
    require_affected_count,
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
/// - A task whose [`TaskTagRole`] is `Opaque` folds as an opaque task, whatever
///   outcome it reports. A derived write beside the user's own has that role:
///   a materialized-sum balance move or an implicit graph edge write. Its
///   count describes a row the statement never named.
///
/// The tag is built over the statement's full plan set
/// ([`Self::for_plans`]), because one task alone cannot tell a derived write
/// beside the user's own from a statement that is only that write.
#[derive(Debug)]
pub struct StatementTag {
    dml: Option<DmlOutcome>,
    opaque: bool,
    /// Whether the statement carries the user's own write. When it does, a
    /// derived write folds as opaque.
    has_user_write: bool,
}

/// Whether one task's outcome answers the statement. Read from
/// [`StatementTag::role_of`] for the task's plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskTagRole {
    /// The statement's own write: its verb and count fold into the tag.
    Counts,
    /// A task whose outcome adds no verb and no count: a derived write beside
    /// the user's own, or a task whose rows answer in place of a count.
    Opaque,
}

/// What a statement's fold produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldedTag {
    Dml(DmlOutcome),
    /// Only opaque tasks were folded. Renders as the `OK` tag.
    Opaque,
}

impl StatementTag {
    /// An empty tag for the statement that planned `plans`: every task the
    /// statement dispatches, stages or buffers.
    pub fn for_plans<'a>(plans: impl IntoIterator<Item = &'a PhysicalPlan>) -> Self {
        Self {
            dml: None,
            opaque: false,
            has_user_write: plans_have_user_write(plans),
        }
    }

    /// The role of the task that runs `plan` in this statement.
    pub fn role_of(&self, plan: &PhysicalPlan) -> TaskTagRole {
        if plan_counts_toward_statement_tag(plan, self.has_user_write) {
            TaskTagRole::Counts
        } else {
            TaskTagRole::Opaque
        }
    }

    /// Fold one task's count-bearing outcome into the statement's tag. A task
    /// whose role is [`TaskTagRole::Opaque`] folds as opaque.
    pub fn fold(&mut self, role: TaskTagRole, outcome: DmlOutcome) -> Result<(), DmlFoldError> {
        if role == TaskTagRole::Opaque {
            self.fold_opaque();
            return Ok(());
        }
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

/// The outcome a write an INSTEAD OF trigger body replaced contributes: the
/// statement ran and changed nothing. A count-bearing plan keeps its verb with
/// a zero count, so the fold stays on one verb. `None` for a plan whose
/// outcome folds as opaque.
pub(crate) fn replaced_write_outcome(plan_kind: super::PlanKind) -> Option<DmlOutcome> {
    use super::PlanKind;
    match plan_kind {
        PlanKind::DmlResult(verb) => Some(DmlOutcome { verb, affected: 0 }),
        // The verb is resolved at apply time and no apply happened. The
        // statement is an `INSERT ... ON CONFLICT DO UPDATE`, so it reports
        // as `INSERT`.
        PlanKind::DmlResultByOp => Some(DmlOutcome {
            verb: "INSERT",
            affected: 0,
        }),
        PlanKind::Execution
        | PlanKind::ArraySlice
        | PlanKind::ReturningRows
        | PlanKind::SingleDocument
        | PlanKind::MultiRow => None,
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
        // arm exists only so the match stays exhaustive against a further
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
///
/// A timeseries ingest answer that rejected lines raises a statement notice
/// with the collection and the count: pgwire sends it as a
/// `NoticeResponse`, and the native protocol adds it to `warnings`.
pub(crate) fn dml_outcome_from_payload(
    payload: &[u8],
    verb: &'static str,
) -> crate::Result<DmlOutcome> {
    // A verb that reports no count reads none. A staged TRUNCATE, in a
    // session transaction or a Calvin transaction, stages no count, and
    // both protocols answer it bare.
    if !DmlOutcome::verb_carries_count(verb) {
        return Ok(DmlOutcome { verb, affected: 0 });
    }
    let affected = require_affected_count(payload).map_err(|e| crate::Error::Internal {
        detail: format!("{verb} response is missing its affected count: {e}"),
    })?;
    if let Some((collection, rejected)) = extract_ingest_rejections(payload) {
        crate::control::server::shared::session::statement_notice::raise(rejected_lines_notice(
            &collection,
            rejected,
        ));
    }
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

    use nodedb_physical::physical_plan::{DocumentOp, GraphOp};
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    fn outcome(verb: &'static str, affected: u64) -> DmlOutcome {
        DmlOutcome { verb, affected }
    }

    /// A tag for a statement with no plans: every task counts.
    fn tag() -> StatementTag {
        StatementTag::for_plans(std::iter::empty())
    }

    const COUNTS: TaskTagRole = TaskTagRole::Counts;

    fn entries() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "entries")
    }

    /// The sum source's own insert.
    fn source_insert() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: entries(),
            document_id: "e3".to_owned(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(11),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: vec!["accts".to_owned()],
        })
    }

    /// The sum source's own delete.
    fn source_delete() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: entries(),
            document_id: "e0".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        })
    }

    /// The cross-shard balance move the planner appends beside a sum-source
    /// write.
    fn balance_delta(delta: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accts"),
            document_id: "acc-0".to_owned(),
            surrogate: Surrogate::new(7),
            column: "balance".to_owned(),
            delta: delta.to_owned(),
            join_column: "account_id".to_owned(),
            join_value: "acc-0".to_owned(),
            declared_primary_key: None,
        })
    }

    /// Fold each task's staged outcome in plan order, as the in-transaction
    /// routes of both protocols do.
    fn fold_staged(plans: &[(PhysicalPlan, StagedTagKind)]) -> Option<FoldedTag> {
        let mut tag = StatementTag::for_plans(plans.iter().map(|(plan, _)| plan));
        for (plan, kind) in plans {
            tag.fold(tag.role_of(plan), staged_dml_outcome(*kind, 1))
                .expect("a derived write never mixes its verb into the tag");
        }
        tag.finish()
    }

    #[test]
    fn empty_fold_finishes_to_none() {
        assert_eq!(tag().finish(), None);
    }

    #[test]
    fn same_verb_sums_affected() {
        let mut tag = tag();
        tag.fold(COUNTS, outcome("INSERT", 1)).expect("first fold");
        tag.fold(COUNTS, outcome("INSERT", 0)).expect("second fold");
        tag.fold(COUNTS, outcome("INSERT", 1)).expect("third fold");
        assert_eq!(tag.finish(), Some(FoldedTag::Dml(outcome("INSERT", 2))));
    }

    #[test]
    fn insert_and_update_fold_to_insert_in_either_order() {
        let mut first = tag();
        first.fold(COUNTS, outcome("INSERT", 1)).expect("insert");
        first
            .fold(COUNTS, outcome("UPDATE", 2))
            .expect("update after insert");
        assert_eq!(first.finish(), Some(FoldedTag::Dml(outcome("INSERT", 3))));

        let mut second = tag();
        second.fold(COUNTS, outcome("UPDATE", 2)).expect("update");
        second
            .fold(COUNTS, outcome("INSERT", 1))
            .expect("insert after update");
        assert_eq!(second.finish(), Some(FoldedTag::Dml(outcome("INSERT", 3))));
    }

    #[test]
    fn other_verb_mix_is_an_error() {
        let mut tag = tag();
        tag.fold(COUNTS, outcome("INSERT", 1)).expect("insert");
        assert_eq!(
            tag.fold(COUNTS, outcome("DELETE", 1)),
            Err(DmlFoldError::VerbMismatch {
                first: "INSERT",
                second: "DELETE",
            })
        );
    }

    #[test]
    fn opaque_never_changes_a_dml_tag() {
        let mut tag = tag();
        tag.fold_opaque();
        tag.fold(COUNTS, outcome("DELETE", 4)).expect("delete");
        tag.fold_opaque();
        assert_eq!(tag.finish(), Some(FoldedTag::Dml(outcome("DELETE", 4))));
    }

    #[test]
    fn only_opaque_finishes_to_opaque() {
        let mut tag = tag();
        tag.fold_opaque();
        tag.fold_opaque();
        assert_eq!(tag.finish(), Some(FoldedTag::Opaque));
    }

    /// An opaque-role task adds no verb and no count, whatever it reports.
    #[test]
    fn an_opaque_role_folds_as_opaque() {
        let mut tag = tag();
        tag.fold(COUNTS, outcome("DELETE", 1)).expect("delete");
        tag.fold(TaskTagRole::Opaque, outcome("UPDATE", 1))
            .expect("an opaque role never mixes its verb");
        assert_eq!(tag.finish(), Some(FoldedTag::Dml(outcome("DELETE", 1))));
    }

    /// A DELETE of a sum-source row in a transaction block stages the delete
    /// and the cross-shard balance move. The balance move is a derived
    /// write, so the statement tags `DELETE 1`, never a verb mismatch.
    #[test]
    fn a_staged_sum_source_delete_tags_delete_1() {
        let folded = fold_staged(&[
            (source_delete(), StagedTagKind::Delete),
            (balance_delta("-4"), StagedTagKind::Update),
        ]);
        assert_eq!(folded, Some(FoldedTag::Dml(outcome("DELETE", 1))));
    }

    /// An INSERT of a sum-source row in a transaction block tags `INSERT 0 1`.
    /// The balance move's row is not counted as an inserted row.
    #[test]
    fn a_staged_sum_source_insert_tags_insert_0_1() {
        let folded = fold_staged(&[
            (source_insert(), StagedTagKind::Insert),
            (balance_delta("4"), StagedTagKind::Update),
        ]);
        assert_eq!(folded, Some(FoldedTag::Dml(outcome("INSERT", 1))));
    }

    /// The task order does not matter: a balance move folded first still
    /// adds nothing.
    #[test]
    fn a_derived_write_folded_first_adds_nothing() {
        let folded = fold_staged(&[
            (balance_delta("-4"), StagedTagKind::Update),
            (source_delete(), StagedTagKind::Delete),
        ]);
        assert_eq!(folded, Some(FoldedTag::Dml(outcome("DELETE", 1))));
    }

    /// An implicit graph edge write beside the user's own delete is derived
    /// too. Its count never adds to the deleted rows.
    #[test]
    fn an_implicit_edge_delete_adds_no_count() {
        let edge_cleanup = PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges: Vec::new() });
        let folded = fold_staged(&[
            (source_delete(), StagedTagKind::Delete),
            (edge_cleanup, StagedTagKind::Delete),
        ]);
        assert_eq!(folded, Some(FoldedTag::Dml(outcome("DELETE", 1))));
    }

    /// A statement that is only a derived-shaped write is the user's own
    /// write, so it counts.
    #[test]
    fn a_lone_derived_shaped_write_counts() {
        let folded = fold_staged(&[(balance_delta("4"), StagedTagKind::Update)]);
        assert_eq!(folded, Some(FoldedTag::Dml(outcome("UPDATE", 1))));
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

    /// A TRUNCATE payload with no count, as a Calvin TRUNCATE stages it,
    /// answers the bare tag. A count-bearing verb still requires one.
    #[test]
    fn a_count_less_truncate_payload_answers_the_bare_tag() {
        assert_eq!(
            dml_outcome_from_payload(&[], "TRUNCATE").expect("truncate"),
            outcome("TRUNCATE", 0)
        );
        assert!(dml_outcome_from_payload(&[], "DELETE").is_err());
    }
}
