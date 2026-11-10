// SPDX-License-Identifier: BUSL-1.1

//! The `currval` items of a Calvin contract that the coordinator answers.
//!
//! The plans of one request run in statement order, so a later statement's
//! `currval` sees an earlier statement's `nextval` or `setval`. Each
//! participant evaluates its own plans before it votes, seeded with the
//! session's values from before the request, so it cannot see a change an
//! earlier plan made on another participant. Such a `currval` evaluates
//! nothing a vote depends on when it is a bare item: the item is the value
//! itself, and no other expression reads it. Its participant answers it as
//! NULL, and the coordinator answers it from the sequence values every
//! earlier plan's frozen reply carries.
//!
//! A plan that changes the sequence itself, or that names one at run time,
//! keeps its own evaluation: its `currval` reads its own changes in order.

use std::collections::BTreeSet;

use nodedb_cluster::calvin::types::{DeferredCurrval, PlanReturning};
use nodedb_query::expr::SqlExpr;
use nodedb_types::{ReturningColumns, ReturningItem, Value};

use crate::Error;

/// The sequences a list changes.
#[derive(Debug, Default)]
struct Changes {
    /// Sequences a `nextval` or `setval` names by a text literal.
    names: BTreeSet<String>,
    /// Whether an accessor names its sequence at run time.
    dynamic: bool,
}

/// Defer every bare `currval` item of `entries`, ascending by plan index,
/// whose sequence an earlier entry changes and its own entry does not.
pub(super) fn defer_currval(entries: &mut [PlanReturning]) -> crate::Result<()> {
    let mut earlier = Changes::default();
    for entry in entries.iter_mut() {
        let ReturningColumns::Named(items) = &mut entry.spec.columns else {
            continue;
        };
        let mut own = Changes::default();
        let mut decoded = Vec::with_capacity(items.len());
        for item in items.iter() {
            let expression = match item {
                ReturningItem::Computed { expression, name } => Some(decode(expression, name)?),
                ReturningItem::Column { .. } | ReturningItem::Star => None,
            };
            if let Some(expression) = &expression {
                collect_changes(expression, &mut own);
            }
            decoded.push(expression);
        }
        // A star expands to many columns, so an item no longer names its
        // column's position.
        let positional = !items.iter().any(|item| matches!(item, ReturningItem::Star));
        if positional && !own.dynamic {
            for (index, (item, expression)) in items.iter_mut().zip(&decoded).enumerate() {
                let Some(sequence) = expression.as_ref().and_then(bare_currval) else {
                    continue;
                };
                let changed = earlier.dynamic || earlier.names.contains(sequence);
                if !changed || own.names.contains(sequence) {
                    continue;
                }
                let sequence = sequence.to_owned();
                if let ReturningItem::Computed { expression, .. } = item {
                    *expression = encode_null()?;
                }
                entry.deferred_currval.push(DeferredCurrval {
                    item: u32::try_from(index).map_err(|_| Error::Internal {
                        detail: "calvin returning contract: item index exceeds u32".to_owned(),
                    })?,
                    sequence,
                });
            }
        }
        earlier.names.extend(own.names);
        earlier.dynamic |= own.dynamic;
    }
    Ok(())
}

fn decode(expression: &[u8], name: &str) -> crate::Result<SqlExpr> {
    zerompk::from_msgpack::<SqlExpr>(expression).map_err(|error| Error::Codec {
        detail: format!("RETURNING expression {name:?} decode: {error}"),
    })
}

fn encode_null() -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&SqlExpr::Literal(Value::Null)).map_err(|error| Error::Codec {
        detail: format!("RETURNING expression encode: {error}"),
    })
}

/// The sequence `expr` reads when it is exactly `currval('<name>')`.
fn bare_currval(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Function { name, args } if name.eq_ignore_ascii_case("currval") => {
            match args.as_slice() {
                [SqlExpr::Literal(Value::String(sequence))] => Some(sequence),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Add every sequence a `nextval` or `setval` in `expr` changes to
/// `changes`. Exhaustive over [`SqlExpr`], so a new variant must decide how
/// it holds an accessor.
fn collect_changes(expr: &SqlExpr, changes: &mut Changes) {
    match expr {
        SqlExpr::Column(_)
        | SqlExpr::Literal(_)
        | SqlExpr::OldColumn(_)
        | SqlExpr::ExcludedColumn(_) => {}
        SqlExpr::BinaryOp { left, right, .. } | SqlExpr::NullIf(left, right) => {
            collect_changes(left, changes);
            collect_changes(right, changes);
        }
        SqlExpr::Negate(inner)
        | SqlExpr::Cast { expr: inner, .. }
        | SqlExpr::IsNull { expr: inner, .. } => collect_changes(inner, changes),
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            for inner in operand.iter().chain(else_expr.iter()) {
                collect_changes(inner, changes);
            }
            for (when, then) in when_thens {
                collect_changes(when, changes);
                collect_changes(then, changes);
            }
        }
        SqlExpr::Coalesce(items) => {
            for item in items {
                collect_changes(item, changes);
            }
        }
        SqlExpr::Function { name, args } => {
            let lowered = name.to_ascii_lowercase();
            let accessor = matches!(lowered.as_str(), "nextval" | "currval" | "setval");
            match args.first() {
                Some(SqlExpr::Literal(Value::String(sequence)))
                    if matches!(lowered.as_str(), "nextval" | "setval") =>
                {
                    changes.names.insert(sequence.clone());
                }
                Some(SqlExpr::Literal(Value::String(_))) => {}
                _ if accessor => changes.dynamic = true,
                _ => {}
            }
            for arg in args {
                collect_changes(arg, changes);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::ReturningContractLimits;
    use nodedb_types::ReturningSpec;

    use super::*;

    fn accessor(function: &str, sequence: &str) -> ReturningItem {
        ReturningItem::Computed {
            name: function.into(),
            expression: zerompk::to_msgpack_vec(&SqlExpr::Function {
                name: function.into(),
                args: vec![SqlExpr::Literal(Value::String(sequence.into()))],
            })
            .expect("encode expression"),
        }
    }

    fn entry(plan_index: u32, items: Vec<ReturningItem>) -> PlanReturning {
        PlanReturning {
            plan_index,
            collection: "c".into(),
            spec: ReturningSpec {
                columns: ReturningColumns::Named(items),
                precommit: None,
            },
            source_columns: Vec::new(),
            limits: ReturningContractLimits {
                rows: 10,
                columns: 10,
                cells: 100,
                payload_bytes: 1024,
            },
            announced: None,
            deferred_currval: Vec::new(),
        }
    }

    fn id() -> ReturningItem {
        ReturningItem::Column {
            name: "id".into(),
            alias: None,
        }
    }

    /// A later plan's bare `currval` of a sequence an earlier plan draws is
    /// deferred and evaluates as NULL. The earlier plan keeps its list.
    #[test]
    fn a_later_currval_of_an_earlier_nextval_defers() {
        let mut entries = vec![
            entry(0, vec![id(), accessor("nextval", "s")]),
            entry(1, vec![id(), accessor("currval", "s")]),
        ];
        defer_currval(&mut entries).expect("defer");
        assert!(entries[0].deferred_currval.is_empty());
        assert_eq!(
            entries[1].deferred_currval,
            vec![DeferredCurrval {
                item: 1,
                sequence: "s".into()
            }]
        );
        let ReturningColumns::Named(items) = &entries[1].spec.columns else {
            panic!("a named list");
        };
        let ReturningItem::Computed { expression, .. } = &items[1] else {
            panic!("a computed item");
        };
        assert_eq!(
            decode(expression, "currval").expect("decode"),
            SqlExpr::Literal(Value::Null)
        );
    }

    /// A `currval` with no earlier change, or in a plan that changes the
    /// sequence itself, evaluates where it runs.
    #[test]
    fn a_currval_that_reads_only_its_own_plan_stays() {
        let mut entries = vec![
            entry(0, vec![accessor("currval", "s")]),
            entry(1, vec![accessor("nextval", "s")]),
            entry(2, vec![accessor("nextval", "s"), accessor("currval", "s")]),
            entry(3, vec![ReturningItem::Star, accessor("currval", "s")]),
        ];
        defer_currval(&mut entries).expect("defer");
        assert!(
            entries
                .iter()
                .all(|entry| entry.deferred_currval.is_empty())
        );
    }
}
