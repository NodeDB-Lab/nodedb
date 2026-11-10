// SPDX-License-Identifier: BUSL-1.1

//! Fix the response contract of a Calvin write that carries `RETURNING`.
//!
//! The coordinator reads everything the response evaluation depends on at
//! submit time: the full item list, the declared source columns, the roles of
//! the writer, the `currval` of the sequences the list names, and the
//! response limits. Every participant then reads the same inputs.

use std::collections::BTreeSet;

use nodedb_cluster::calvin::types::{PlanReturning, ReturningContract, SequenceValue, TxClass};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_query::expr::SqlExpr;
use nodedb_types::{QualifiedCollection, ReturningColumns, ReturningItem, ReturningSpec, Value};

use crate::Error;
use crate::control::security::rls::compile::declared_columns;
use crate::control::server::shared::returning::precommit::ReturningLimits;
use crate::control::server::shared::session::read_scope::current_sequence_values;
use crate::control::state::SharedState;

/// Set `tx_class.returning` from every plan that carries a full `RETURNING`
/// list, one entry per plan keyed by its index. `plans` are the class's
/// decoded plans. A class with no such plan stays as it is. A class that
/// already holds a contract keeps it, so a forwarded submit keeps what its
/// coordinator fixed.
pub(super) fn stamp_returning_contract(
    state: &SharedState,
    tx_class: &mut TxClass,
    plans: &[PhysicalPlan],
) -> crate::Result<()> {
    if tx_class.returning.is_some() {
        return Ok(());
    }
    let limits = ReturningLimits::from_tuning(&state.tuning.network).to_contract();
    let mut entries = Vec::new();
    let mut refs = SequenceRefs::default();
    for (index, plan) in plans.iter().enumerate() {
        let Some((collection, spec)) = plan.returning_target() else {
            continue;
        };
        let Some(columns) = spec.precommit.as_ref() else {
            continue;
        };
        let spec = ReturningSpec {
            columns: columns.clone(),
            precommit: None,
        };
        let source_columns = declared_columns(
            state.credentials.catalog(),
            tx_class.database_id,
            tx_class.tenant_id.as_u64(),
            QualifiedCollection::bare_of_stored(collection.as_str()),
        )?
        .into_iter()
        .map(|column| column.name)
        .collect();
        named_sequences(&spec, &mut refs)?;
        let plan_index = u32::try_from(index).map_err(|_| Error::Internal {
            detail: "calvin returning contract: plan index exceeds u32".to_owned(),
        })?;
        entries.push(PlanReturning {
            plan_index,
            spec,
            source_columns,
            limits,
        });
    }
    if entries.is_empty() {
        return Ok(());
    }
    let sequence_values = sequence_snapshot(tx_class, &refs);
    tx_class.set_returning(Some(ReturningContract {
        plans: entries,
        roles: tx_class.user_roles.clone(),
        sequence_values,
    }));
    Ok(())
}

/// The `currval` the writer's session holds for the sequences `refs` names,
/// ascending by name. A list that names a sequence at run time can reach any
/// sequence, so it takes every value the session holds in the class's tenant
/// and database. A statement with no session snapshots nothing.
fn sequence_snapshot(tx_class: &TxClass, refs: &SequenceRefs) -> Vec<SequenceValue> {
    let Some(session) = current_sequence_values() else {
        return Vec::new();
    };
    let database_id = tx_class.database_id.as_u64();
    let tenant_id = tx_class.tenant_id.as_u64();
    let values = if refs.dynamic {
        session.values_in(database_id, tenant_id)
    } else {
        refs.names
            .iter()
            .filter_map(|name| {
                session
                    .last(database_id, tenant_id, name)
                    .map(|value| (name.clone(), value))
            })
            .collect()
    };
    values
        .into_iter()
        .map(|(name, value)| SequenceValue { name, value })
        .collect()
}

/// The sequences a list names.
#[derive(Debug, Default)]
struct SequenceRefs {
    /// Sequences named by a text literal, ascending.
    names: BTreeSet<String>,
    /// Whether an accessor takes a name that is not a literal.
    dynamic: bool,
}

/// Add the sequence accessors of every computed item of `spec` to `refs`.
fn named_sequences(spec: &ReturningSpec, refs: &mut SequenceRefs) -> crate::Result<()> {
    if let ReturningColumns::Named(items) = &spec.columns {
        for item in items {
            if let ReturningItem::Computed { expression, name } = item {
                let expression =
                    zerompk::from_msgpack::<SqlExpr>(expression).map_err(|error| Error::Codec {
                        detail: format!("RETURNING expression {name:?} decode: {error}"),
                    })?;
                collect_sequences(&expression, refs);
            }
        }
    }
    Ok(())
}

/// Add every sequence an accessor in `expr` names to `refs`. Exhaustive over
/// [`SqlExpr`], so a new variant must decide how it holds an accessor.
fn collect_sequences(expr: &SqlExpr, refs: &mut SequenceRefs) {
    match expr {
        SqlExpr::Column(_)
        | SqlExpr::Literal(_)
        | SqlExpr::OldColumn(_)
        | SqlExpr::ExcludedColumn(_) => {}
        SqlExpr::BinaryOp { left, right, .. } | SqlExpr::NullIf(left, right) => {
            collect_sequences(left, refs);
            collect_sequences(right, refs);
        }
        SqlExpr::Negate(inner)
        | SqlExpr::Cast { expr: inner, .. }
        | SqlExpr::IsNull { expr: inner, .. } => collect_sequences(inner, refs),
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            for inner in operand.iter().chain(else_expr.iter()) {
                collect_sequences(inner, refs);
            }
            for (when, then) in when_thens {
                collect_sequences(when, refs);
                collect_sequences(then, refs);
            }
        }
        SqlExpr::Coalesce(items) => {
            for item in items {
                collect_sequences(item, refs);
            }
        }
        SqlExpr::Function { name, args } => {
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "nextval" | "currval" | "setval"
            ) {
                match args.first() {
                    Some(SqlExpr::Literal(Value::String(sequence))) => {
                        refs.names.insert(sequence.clone());
                    }
                    _ => refs.dynamic = true,
                }
            }
            for arg in args {
                collect_sequences(arg, refs);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_query::expr::BinaryOp;

    use super::*;

    fn computed(name: &str, expression: SqlExpr) -> ReturningItem {
        ReturningItem::Computed {
            name: name.into(),
            expression: zerompk::to_msgpack_vec(&expression).expect("encode expression"),
        }
    }

    fn accessor(function: &str, sequence: SqlExpr) -> SqlExpr {
        SqlExpr::Function {
            name: function.into(),
            args: vec![sequence],
        }
    }

    fn list(items: Vec<ReturningItem>) -> ReturningSpec {
        ReturningSpec {
            columns: ReturningColumns::Named(items),
            precommit: None,
        }
    }

    fn read(spec: &ReturningSpec) -> SequenceRefs {
        let mut refs = SequenceRefs::default();
        named_sequences(spec, &mut refs).expect("read accessors");
        refs
    }

    #[test]
    fn literal_names_in_nested_calls_are_collected_once() {
        let name = |text: &str| SqlExpr::Literal(Value::String(text.into()));
        let spec = list(vec![
            ReturningItem::Column {
                name: "id".into(),
                alias: None,
            },
            computed(
                "sum",
                SqlExpr::BinaryOp {
                    left: Box::new(accessor("NEXTVAL", name("a"))),
                    op: BinaryOp::Add,
                    right: Box::new(accessor("currval", name("b"))),
                },
            ),
            computed("again", accessor("currval", name("a"))),
        ]);
        let refs = read(&spec);
        assert_eq!(
            refs.names.iter().map(String::as_str).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert!(!refs.dynamic);
    }

    #[test]
    fn a_name_read_from_a_column_marks_the_list_dynamic() {
        let spec = list(vec![computed(
            "next",
            accessor("nextval", SqlExpr::Column("seq".into())),
        )]);
        let refs = read(&spec);
        assert!(refs.names.is_empty());
        assert!(refs.dynamic);
    }

    #[test]
    fn a_list_with_no_accessor_names_nothing() {
        let refs = read(&ReturningSpec {
            columns: ReturningColumns::Star,
            precommit: None,
        });
        assert!(refs.names.is_empty() && !refs.dynamic);
    }
}
