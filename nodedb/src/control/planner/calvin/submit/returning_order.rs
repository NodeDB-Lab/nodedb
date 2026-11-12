// SPDX-License-Identifier: BUSL-1.1

//! The order the participants of a Calvin write evaluate their `RETURNING`
//! lists in.
//!
//! The plans of one request run in statement order, so a later statement's
//! list sees every sequence change an earlier statement made: its
//! `currval`, and the next value its `nextval` draws. Each participant
//! evaluates its own plans before it votes. A plan whose list reads or
//! changes a sequence follows every earlier plan that changes it: its
//! participants wait for the earlier plans' changes before they evaluate.
//! A plan that changes a sequence and runs on several vShards follows
//! itself: its participants evaluate one after another, by vShard.
//!
//! The order is closed: a plan follows every plan an earlier plan it
//! follows follows. So a participant seeds its session with every change
//! the changes it reads were made after, in statement order, and each
//! earlier participant sends its changes to every participant that seeds
//! them.
//!
//! Every other plan evaluates at once, in parallel with the rest.

use std::collections::BTreeSet;

use nodedb_cluster::calvin::types::PlanReturning;
use nodedb_query::expr::SqlExpr;
use nodedb_types::{ReturningColumns, ReturningItem, ReturningSpec, Value};

use crate::Error;

/// The sequence accessors of one `RETURNING` list.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct PlanAccessors {
    /// Sequences an accessor names by a text literal, as written.
    pub touched: BTreeSet<String>,
    /// Sequences a `nextval` or `setval` names by a text literal.
    pub changed: BTreeSet<String>,
    /// Whether an accessor names its sequence at run time. Such a list can
    /// read or change any sequence.
    pub dynamic: bool,
}

impl PlanAccessors {
    /// The accessors of every computed item of `spec`.
    pub(super) fn of(spec: &ReturningSpec) -> crate::Result<Self> {
        let mut accessors = Self::default();
        if let ReturningColumns::Named(items) = &spec.columns {
            for item in items {
                if let ReturningItem::Computed { expression, name } = item {
                    let expression =
                        zerompk::from_msgpack::<SqlExpr>(expression).map_err(|error| {
                            Error::Codec {
                                detail: format!("RETURNING expression {name:?} decode: {error}"),
                            }
                        })?;
                    accessors.collect(&expression);
                }
            }
        }
        Ok(accessors)
    }

    fn changes_any(&self) -> bool {
        self.dynamic || !self.changed.is_empty()
    }

    fn touches_any(&self) -> bool {
        self.dynamic || !self.touched.is_empty()
    }

    /// Whether a list with these accessors follows `earlier`, a list that
    /// runs before it: `earlier` changes a sequence this list reads or
    /// changes. Sequence names compare without ASCII case.
    pub(super) fn follows(&self, earlier: &Self) -> bool {
        if !earlier.changes_any() || !self.touches_any() {
            return false;
        }
        if earlier.dynamic || self.dynamic {
            return true;
        }
        earlier.changed.iter().any(|changed| {
            self.touched
                .iter()
                .any(|touched| touched.eq_ignore_ascii_case(changed))
        })
    }

    /// Add every accessor in `expr`. Exhaustive over [`SqlExpr`], so a new
    /// variant must decide how it holds an accessor.
    fn collect(&mut self, expr: &SqlExpr) {
        match expr {
            SqlExpr::Column(_)
            | SqlExpr::Literal(_)
            | SqlExpr::OldColumn(_)
            | SqlExpr::ExcludedColumn(_) => {}
            SqlExpr::BinaryOp { left, right, .. } | SqlExpr::NullIf(left, right) => {
                self.collect(left);
                self.collect(right);
            }
            SqlExpr::Negate(inner)
            | SqlExpr::Cast { expr: inner, .. }
            | SqlExpr::IsNull { expr: inner, .. } => self.collect(inner),
            SqlExpr::Case {
                operand,
                when_thens,
                else_expr,
            } => {
                for inner in operand.iter().chain(else_expr.iter()) {
                    self.collect(inner);
                }
                for (when, then) in when_thens {
                    self.collect(when);
                    self.collect(then);
                }
            }
            SqlExpr::Coalesce(items) => {
                for item in items {
                    self.collect(item);
                }
            }
            SqlExpr::Function { name, args } => {
                let lowered = name.to_ascii_lowercase();
                if matches!(lowered.as_str(), "nextval" | "currval" | "setval") {
                    match args.first() {
                        Some(SqlExpr::Literal(Value::String(sequence))) => {
                            self.touched.insert(sequence.clone());
                            if lowered != "currval" {
                                self.changed.insert(sequence.clone());
                            }
                        }
                        _ => self.dynamic = true,
                    }
                }
                for arg in args {
                    self.collect(arg);
                }
            }
        }
    }
}

/// Set `sequence_after` on every entry of `entries`, ascending by plan
/// index, closed over the plans each earlier plan follows. `accessors` is
/// parallel to `entries`, the entries ascend by plan index, and each
/// entry's `vshards` is set.
pub(super) fn order_sequences(entries: &mut [PlanReturning], accessors: &[PlanAccessors]) {
    let mut closed: Vec<BTreeSet<u32>> = Vec::with_capacity(entries.len());
    for (at, entry) in entries.iter().enumerate() {
        let mut after = BTreeSet::new();
        if let Some(own) = accessors.get(at) {
            for (earlier, list) in accessors.iter().enumerate().take(at) {
                if own.follows(list) {
                    after.insert(entries[earlier].plan_index);
                    if let Some(theirs) = closed.get(earlier) {
                        after.extend(theirs.iter().copied());
                    }
                }
            }
            if entry.vshards.len() > 1 && own.follows(own) {
                after.insert(entry.plan_index);
            }
        }
        closed.push(after);
    }
    for (entry, after) in entries.iter_mut().zip(closed) {
        entry.sequence_after = after.into_iter().collect();
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::ReturningContractLimits;
    use nodedb_query::expr::BinaryOp;

    use super::*;

    fn accessor(function: &str, sequence: &str) -> SqlExpr {
        SqlExpr::Function {
            name: function.into(),
            args: vec![SqlExpr::Literal(Value::String(sequence.into()))],
        }
    }

    fn computed(expression: SqlExpr) -> ReturningItem {
        ReturningItem::Computed {
            name: "x".into(),
            expression: zerompk::to_msgpack_vec(&expression).expect("encode expression"),
        }
    }

    fn list(items: Vec<SqlExpr>) -> PlanAccessors {
        PlanAccessors::of(&ReturningSpec {
            columns: ReturningColumns::Named(items.into_iter().map(computed).collect()),
            precommit: None,
        })
        .expect("read the accessors")
    }

    fn entry(plan_index: u32, vshards: &[u32]) -> PlanReturning {
        PlanReturning {
            plan_index,
            collection: "c".into(),
            spec: ReturningSpec {
                columns: ReturningColumns::Star,
                precommit: None,
            },
            source_columns: Vec::new(),
            limits: ReturningContractLimits {
                rows: 1,
                columns: 1,
                cells: 1,
                payload_bytes: 1,
            },
            announced: None,
            vshards: vshards.to_vec(),
            sequence_after: Vec::new(),
        }
    }

    fn order(plans: Vec<(u32, Vec<u32>, PlanAccessors)>) -> Vec<Vec<u32>> {
        let (mut entries, accessors): (Vec<_>, Vec<_>) = plans
            .into_iter()
            .map(|(index, vshards, accessors)| (entry(index, &vshards), accessors))
            .unzip();
        order_sequences(&mut entries, &accessors);
        entries
            .into_iter()
            .map(|entry| entry.sequence_after)
            .collect()
    }

    /// A `currval` inside an expression follows the earlier `nextval`.
    #[test]
    fn a_currval_in_an_expression_follows_an_earlier_nextval() {
        let plus_one = SqlExpr::BinaryOp {
            left: Box::new(accessor("currval", "s")),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(Value::Integer(1))),
        };
        let after = order(vec![
            (0, vec![3], list(vec![accessor("nextval", "s")])),
            (1, vec![5], list(vec![plus_one])),
        ]);
        assert_eq!(after, vec![vec![], vec![0]]);
    }

    /// A later `nextval` follows an earlier `nextval` or `setval` of the
    /// same sequence, so its values come after the earlier ones.
    #[test]
    fn a_later_change_follows_every_earlier_change() {
        let after = order(vec![
            (0, vec![3], list(vec![accessor("nextval", "s")])),
            (1, vec![5], list(vec![accessor("setval", "S")])),
            (2, vec![3], list(vec![accessor("nextval", "s")])),
        ]);
        assert_eq!(after, vec![vec![], vec![0], vec![0, 1]]);
    }

    /// A list of another sequence, or one that only reads, orders nothing
    /// after itself, and a reader follows no earlier reader.
    #[test]
    fn independent_lists_stay_parallel() {
        let after = order(vec![
            (0, vec![3], list(vec![accessor("currval", "s")])),
            (1, vec![5], list(vec![accessor("nextval", "t")])),
            (2, vec![7], list(vec![accessor("currval", "s")])),
            (3, vec![9], PlanAccessors::default()),
        ]);
        assert_eq!(after, vec![Vec::<u32>::new(); 4]);
    }

    /// A list that names its sequence at run time follows every earlier
    /// change, and every later accessor follows it.
    #[test]
    fn a_run_time_name_follows_and_precedes_every_accessor() {
        let dynamic = SqlExpr::Function {
            name: "nextval".into(),
            args: vec![SqlExpr::Column("seq".into())],
        };
        let after = order(vec![
            (0, vec![3], list(vec![accessor("nextval", "a")])),
            (1, vec![5], list(vec![dynamic])),
            (2, vec![7], list(vec![accessor("currval", "b")])),
        ]);
        assert_eq!(after, vec![vec![], vec![0], vec![0, 1]]);
    }

    /// A plan that follows a later change of another sequence follows what
    /// that change followed: plan 2 reads `t`, plan 1 changed `t` after
    /// plan 0's change of `s`, so plan 2 seeds both in order.
    #[test]
    fn the_order_closes_over_earlier_plans() {
        let both = vec![accessor("nextval", "s"), accessor("nextval", "t")];
        let after = order(vec![
            (0, vec![3], list(vec![accessor("nextval", "s")])),
            (1, vec![5], list(both)),
            (2, vec![7], list(vec![accessor("currval", "t")])),
        ]);
        assert_eq!(after, vec![vec![], vec![0], vec![0, 1]]);
    }

    /// A changing plan on several vShards follows itself. A reading one, or
    /// one on a single vShard, does not.
    #[test]
    fn a_changing_plan_on_several_vshards_follows_itself() {
        let after = order(vec![
            (0, vec![3, 5], list(vec![accessor("nextval", "s")])),
            (1, vec![3, 5], list(vec![accessor("currval", "t")])),
            (2, vec![7], list(vec![accessor("nextval", "u")])),
        ]);
        assert_eq!(after, vec![vec![0], vec![], vec![]]);
    }

    /// A star or a plain column holds no accessor.
    #[test]
    fn a_list_with_no_accessor_touches_nothing() {
        let accessors = PlanAccessors::of(&ReturningSpec {
            columns: ReturningColumns::Named(vec![
                ReturningItem::Star,
                ReturningItem::Column {
                    name: "id".into(),
                    alias: None,
                },
            ]),
            precommit: None,
        })
        .expect("read the accessors");
        assert_eq!(accessors, PlanAccessors::default());
    }
}
