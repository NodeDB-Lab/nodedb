// SPDX-License-Identifier: BUSL-1.1

//! Fix the response contract of a Calvin write that carries `RETURNING`.
//!
//! The coordinator reads everything the response evaluation depends on at
//! submit time: the full item list, the declared source columns, the roles of
//! the writer, the `currval` of the sequences the list names, the columns the
//! statement announced, and the response limits. Every participant then
//! reads the same inputs.

use std::collections::BTreeSet;

use nodedb_cluster::calvin::types::{
    PlanReturning, ReturningContract, ReturningContractLimits, SequenceValue, TxClass,
};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_query::expr::SqlExpr;
use nodedb_types::{QualifiedCollection, ReturningColumns, ReturningItem, ReturningSpec, Value};

use crate::Error;
use crate::control::cluster::calvin::scheduler::driver::core::routing::{
    PlanRouting, plan_vshard_in_database,
};
use crate::control::security::rls::compile::declared_columns;
use crate::control::server::shared::returning::precommit::ReturningLimits;
use crate::control::server::shared::session::announced_returning::announced_returning;
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
    // The statement's one `RETURNING` list, announced before any row.
    let announced = announced_returning();
    let mut entries = Vec::new();
    // The vShards each entry's plan routes to, parallel to `entries`.
    let mut participants = Vec::new();
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
        participants.push(plan_participants(plan, tx_class.database_id));
        let plan_index = u32::try_from(index).map_err(|_| Error::Internal {
            detail: "calvin returning contract: plan index exceeds u32".to_owned(),
        })?;
        entries.push(PlanReturning {
            plan_index,
            collection: collection.as_str().to_owned(),
            spec,
            source_columns,
            limits,
            announced: announced.clone(),
            deferred_currval: Vec::new(),
        });
    }
    if entries.is_empty() {
        return Ok(());
    }
    // Several plans share the statement's limits, and the participants of
    // one plan share its part: each participant refuses past its share
    // before it votes, so the whole answer stays within them.
    let share = limits_share(limits, entries.len());
    for (entry, participants) in entries.iter_mut().zip(participants) {
        entry.limits = limits_share(share, participants);
    }
    super::returning_deferral::defer_currval(&mut entries)?;
    let sequence_values = sequence_snapshot(tx_class, &refs);
    tx_class.set_returning(Some(ReturningContract {
        plans: entries,
        roles: tx_class.user_roles.clone(),
        sequence_values,
    }));
    Ok(())
}

/// Each of `parts` parts' share of `limits`: the plans of a statement, or
/// the participants of one plan. Rows, cells and payload bytes add up across
/// the parts' result sets, so each part takes an equal share, rounded down.
/// Columns bound each set alone, so each part takes them all.
fn limits_share(limits: ReturningContractLimits, parts: usize) -> ReturningContractLimits {
    let parts = u64::try_from(parts).unwrap_or(u64::MAX).max(1);
    ReturningContractLimits {
        rows: limits.rows / parts,
        columns: limits.columns,
        cells: limits.cells / parts,
        payload_bytes: limits.payload_bytes / parts,
    }
}

/// The number of vShards `plan` routes to in `database_id`, each a
/// participant that evaluates its own rows of the plan. A plan that routes
/// to none counts one: the scheduler refuses it before any evaluation.
fn plan_participants(plan: &PhysicalPlan, database_id: nodedb_types::DatabaseId) -> usize {
    match plan_vshard_in_database(plan, database_id) {
        PlanRouting::Vshards(vshards) => vshards
            .iter()
            .map(|vshard| vshard.as_u32())
            .collect::<BTreeSet<u32>>()
            .len()
            .max(1),
        PlanRouting::ControlPlaneOnly | PlanRouting::NotAWrite | PlanRouting::Unroutable(_) => 1,
    }
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

    /// A plan that routes to two vShards splits its share between them: the
    /// two participants together answer at most the plan's share.
    #[test]
    fn a_plan_on_several_vshards_splits_its_share() {
        use nodedb_physical::physical_plan::GraphOp;
        use nodedb_types::Surrogate;
        let vshard = |key: &str| crate::types::VShardId::from_key(key.as_bytes()).as_u32();
        let dst = (0..2048)
            .map(|i| format!("node_{i}"))
            .find(|key| vshard(key) != vshard("node_src"))
            .expect("a key on another vShard");
        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, "follows"),
            src_id: "node_src".to_owned(),
            label: "knows".to_owned(),
            dst_id: dst,
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
        });
        let participants = plan_participants(&plan, nodedb_types::DatabaseId::DEFAULT);
        assert_eq!(participants, 2, "an edge on two homes has two participants");
        let limits = ReturningContractLimits {
            rows: 12,
            columns: 8,
            cells: 96,
            payload_bytes: 1200,
        };
        let share = limits_share(limits, participants);
        let total = |per: u64| per * u64::try_from(participants).expect("a count");
        assert!(total(share.rows) <= limits.rows);
        assert!(total(share.cells) <= limits.cells);
        assert!(total(share.payload_bytes) <= limits.payload_bytes);
        assert_eq!(share.columns, limits.columns);
        assert_eq!(limits_share(limits, 3).rows, 4);
    }

    /// Two plans each take half the rows, cells and bytes, and every column.
    #[test]
    fn several_plans_share_the_limits() {
        let limits = ReturningContractLimits {
            rows: 5,
            columns: 8,
            cells: 40,
            payload_bytes: 1000,
        };
        assert_eq!(limits_share(limits, 1), limits);
        assert_eq!(
            limits_share(limits, 2),
            ReturningContractLimits {
                rows: 2,
                columns: 8,
                cells: 20,
                payload_bytes: 500,
            }
        );
    }

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
