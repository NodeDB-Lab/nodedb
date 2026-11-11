// SPDX-License-Identifier: BUSL-1.1

//! Whether a write's `RETURNING` contract can fail once its source commits.
//!
//! A write whose list cannot fail answers its rows after the commit with no
//! risk of an error past the stored write. Every other contract write
//! evaluates its list before the commit.
//!
//! A list cannot fail only when all of these hold:
//!
//! - Every item is a column, `*`, or a literal. A function call, operator,
//!   cast or sequence accessor can raise an error.
//! - The write changes at most one row by construction: a point insert,
//!   put, upsert, update or delete by primary key.
//! - One row of the list fits the configured row, column and cell limits,
//!   and its payload bound fits `network.max_query_result_bytes`.
//!
//! Every stored row image is at most `MAX_ROW_IMAGE_BYTES`, so a column
//! cell renders to a bounded size (see `payload_bound`). A literal's cell is
//! exact. A `*` expands to the row's fields: a strict collection declares
//! how many, and a schemaless row has no field cap. So a `*` on a schemaless
//! collection fits only with no column, cell or byte limit.

use nodedb_cluster::calvin::types::AnnouncedColumn;
use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::config::tuning::NetworkTuning;

use super::list_shape::{ListShape, RowFields};
use super::precommit::ReturningLimits;

/// Whether `plan`'s `RETURNING` contract can fail under the limits `network`
/// sets. `fields` is what a `*` expands to on the plan's collection.
/// `announced` is the columns the statement announced, or `None` when it
/// announced none. A plan with no contract has nothing to fail.
pub fn returning_can_fail(
    plan: &PhysicalPlan,
    network: &NetworkTuning,
    fields: RowFields,
    announced: Option<&[AnnouncedColumn]>,
) -> bool {
    let Some(list) = plan
        .returning_target()
        .and_then(|(_, spec)| spec.precommit.as_ref())
    else {
        return false;
    };
    if !writes_at_most_one_row(plan) {
        return true;
    }
    match ListShape::of(list) {
        Some(shape) => {
            !shape.fits_one_row(&ReturningLimits::from_tuning(network), fields, announced)
        }
        None => true,
    }
}

/// Whether any task of one statement's `tasks` carries a contract that can
/// fail. `fields_of` gives what a `*` expands to on a task plan's
/// collection.
pub fn tasks_returning_can_fail(
    tasks: &[PhysicalTask],
    network: &NetworkTuning,
    announced: Option<&[AnnouncedColumn]>,
    fields_of: impl Fn(&PhysicalPlan) -> RowFields,
) -> bool {
    tasks
        .iter()
        .any(|task| returning_can_fail(&task.plan, network, fields_of(&task.plan), announced))
}

/// Whether `plan` changes at most one row by construction. A variant not
/// listed here counts as many rows, so a new variant can fail until it is
/// listed.
fn writes_at_most_one_row(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Document(
            DocumentOp::PointInsert { .. }
                | DocumentOp::PointPut { .. }
                | DocumentOp::Upsert { .. }
                | DocumentOp::PointUpdate { .. }
                | DocumentOp::PointDelete { .. },
        ) | PhysicalPlan::Kv(
            KvOp::Insert { .. }
                | KvOp::InsertIfAbsent { .. }
                | KvOp::InsertOnConflictUpdate { .. }
                | KvOp::Put { .. }
                | KvOp::FieldSet { .. },
        )
    ) || matches!(plan, PhysicalPlan::Kv(KvOp::Delete { keys, .. }) if keys.len() <= 1)
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::UpdateValue;
    use nodedb_physical::physical_task::PostSetOp;
    use nodedb_query::expr::{BinaryOp, CastType, SqlExpr};
    use nodedb_types::{
        DatabaseId, QualifiedCollection, ReturningColumns, ReturningItem, ReturningSpec,
        RlsWriteCheck, Surrogate, Value,
    };

    use super::super::payload_bound::{
        column_cell_bytes, envelope_bytes, literal_cell_bytes, string_bytes,
    };
    use super::*;
    use crate::types::{TenantId, VShardId};

    /// [`returning_can_fail`] on a schemaless collection, nothing announced.
    fn can_fail(plan: &PhysicalPlan, network: &NetworkTuning) -> bool {
        returning_can_fail(plan, network, RowFields::Unbounded, None)
    }

    fn collection() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "items")
    }

    fn spec(items: Vec<ReturningItem>) -> Option<ReturningSpec> {
        let list = ReturningColumns::Named(items);
        Some(ReturningSpec {
            columns: list.clone(),
            precommit: Some(list),
        })
    }

    fn column(name: &str) -> ReturningItem {
        ReturningItem::Column {
            name: name.into(),
            alias: None,
        }
    }

    fn computed(name: &str, expression: SqlExpr) -> ReturningItem {
        ReturningItem::Computed {
            expression: zerompk::to_msgpack_vec(&expression).expect("encode expression"),
            name: name.into(),
        }
    }

    fn point_insert(returning: Option<ReturningSpec>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: collection(),
            document_id: "a".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(1),
            returning,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        })
    }

    fn batch_insert(returning: Option<ReturningSpec>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: collection(),
            documents: vec![("a".into(), Vec::new()), ("b".into(), Vec::new())],
            if_absent: false,
            surrogates: vec![Surrogate::new(1), Surrogate::new(2)],
            returning,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    fn updates() -> Vec<(String, UpdateValue)> {
        vec![(
            "n".into(),
            UpdateValue::Literal(zerompk::to_msgpack_vec(&Value::Integer(1)).expect("encode")),
        )]
    }

    fn point_update(returning: Option<ReturningSpec>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: collection(),
            document_id: "a".into(),
            surrogate: Some(Surrogate::new(1)),
            pk_bytes: b"a".to_vec(),
            updates: updates(),
            returning,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    fn bulk_update(returning: Option<ReturningSpec>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: collection(),
            filters: Vec::new(),
            updates: updates(),
            returning,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
            system_time: Default::default(),
        })
    }

    /// A node with no byte, column or cell limit.
    fn unbounded() -> NetworkTuning {
        NetworkTuning {
            max_query_result_bytes: u64::MAX,
            max_returning_columns: u64::MAX,
            max_returning_cells: u64::MAX,
            ..NetworkTuning::default()
        }
    }

    fn sequence_call(name: &str) -> SqlExpr {
        SqlExpr::Function {
            name: name.into(),
            args: vec![SqlExpr::Literal(Value::String("s".into()))],
        }
    }

    #[test]
    fn a_column_list_on_a_point_insert_cannot_fail() {
        let plan = point_insert(spec(vec![column("id"), column("n")]));
        assert!(!can_fail(&plan, &unbounded()));
    }

    #[test]
    fn columns_literals_and_a_star_on_point_writes_cannot_fail() {
        let items = vec![
            column("id"),
            ReturningItem::Star,
            computed("one", SqlExpr::Literal(Value::Integer(1))),
        ];
        assert!(!can_fail(&point_insert(spec(items.clone())), &unbounded()));
        assert!(!can_fail(&point_update(spec(items)), &unbounded()));
    }

    /// The payload bound of `id` and the literal `1`, named `one`, in one
    /// row.
    fn id_and_one_bound() -> usize {
        envelope_bytes(2)
            + string_bytes("id".len())
            + string_bytes("one".len())
            + column_cell_bytes()
            + literal_cell_bytes(&Value::Integer(1)).expect("literal")
    }

    #[test]
    fn a_fixed_list_fits_at_exactly_its_bound() {
        let plan = point_insert(spec(vec![
            column("id"),
            computed("one", SqlExpr::Literal(Value::Integer(1))),
        ]));
        let at = |bytes: usize| NetworkTuning {
            max_query_result_bytes: bytes as u64,
            ..unbounded()
        };
        let bound = id_and_one_bound();
        assert!(!can_fail(&plan, &at(bound)));
        assert!(can_fail(&plan, &at(bound - 1)));
    }

    #[test]
    fn returning_id_on_a_point_write_cannot_fail_by_default() {
        let network = NetworkTuning::default();
        for plan in [
            point_insert(spec(vec![column("id")])),
            point_update(spec(vec![column("id")])),
        ] {
            assert!(!can_fail(&plan, &network));
        }
    }

    #[test]
    fn a_star_on_a_strict_collection_with_few_columns_cannot_fail_by_default() {
        let plan = point_insert(spec(vec![ReturningItem::Star]));
        let fields = RowFields::declared(["id", "name", "email", "balance"]);
        let network = NetworkTuning::default();
        assert!(!returning_can_fail(&plan, &network, fields, None));
        let star_only = point_insert(Some(ReturningSpec {
            columns: ReturningColumns::Star,
            precommit: Some(ReturningColumns::Star),
        }));
        assert!(!returning_can_fail(&star_only, &network, fields, None));
    }

    #[test]
    fn a_star_past_the_byte_or_column_limit_can_fail() {
        let plan = point_insert(spec(vec![ReturningItem::Star]));
        let network = NetworkTuning::default();
        let wide = |count: usize| RowFields::Declared {
            count,
            name_bytes: count,
        };
        // Each column cell can render a whole stored row, so a default
        // byte limit holds far fewer columns than the column limit.
        let cells_per_payload = network.max_query_result_bytes as usize / column_cell_bytes();
        assert!(returning_can_fail(
            &plan,
            &network,
            wide(cells_per_payload + 1),
            None
        ));
        let no_bytes = NetworkTuning {
            max_query_result_bytes: u64::MAX,
            ..NetworkTuning::default()
        };
        let columns = network.max_returning_columns as usize;
        assert!(!returning_can_fail(&plan, &no_bytes, wide(columns), None));
        assert!(returning_can_fail(
            &plan,
            &no_bytes,
            wide(columns + 1),
            None
        ));
    }

    #[test]
    fn a_star_on_a_schemaless_collection_can_fail() {
        let plan = point_insert(spec(vec![ReturningItem::Star]));
        assert!(can_fail(&plan, &NetworkTuning::default()));
        let capped_bytes = NetworkTuning {
            max_query_result_bytes: 1 << 40,
            ..unbounded()
        };
        assert!(can_fail(&plan, &capped_bytes));
    }

    #[test]
    fn announced_names_count_toward_the_bound() {
        let plan = point_insert(spec(vec![
            column("id"),
            computed("one", SqlExpr::Literal(Value::Integer(1))),
        ]));
        let named = |name: &str| AnnouncedColumn {
            name: name.into(),
            ty: "text".into(),
        };
        let network = NetworkTuning {
            max_query_result_bytes: id_and_one_bound() as u64,
            ..unbounded()
        };
        let same = [named("id"), named("one")];
        assert!(!returning_can_fail(
            &plan,
            &network,
            RowFields::Unbounded,
            Some(&same)
        ));
        let longer = [named("id"), named("one_longer")];
        assert!(returning_can_fail(
            &plan,
            &network,
            RowFields::Unbounded,
            Some(&longer)
        ));
        let fewer = [named("id")];
        assert!(returning_can_fail(
            &plan,
            &network,
            RowFields::Unbounded,
            Some(&fewer)
        ));
    }

    #[test]
    fn a_statement_can_fail_when_any_task_can() {
        let task = |plan| PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let network = NetworkTuning::default();
        let point = task(point_insert(spec(vec![column("id")])));
        let batch = task(batch_insert(spec(vec![column("id")])));
        let unbounded_fields = |_: &PhysicalPlan| RowFields::Unbounded;
        assert!(!tasks_returning_can_fail(
            std::slice::from_ref(&point),
            &network,
            None,
            unbounded_fields
        ));
        assert!(tasks_returning_can_fail(
            &[point, batch],
            &network,
            None,
            unbounded_fields
        ));
    }

    #[test]
    fn a_list_past_the_column_or_cell_limit_can_fail() {
        let plan = point_insert(spec(vec![column("a"), column("b")]));
        for network in [
            NetworkTuning {
                max_returning_columns: 1,
                ..unbounded()
            },
            NetworkTuning {
                max_returning_cells: 1,
                ..unbounded()
            },
            NetworkTuning {
                max_returning_rows: 0,
                ..unbounded()
            },
        ] {
            assert!(can_fail(&plan, &network));
        }
        let star = point_insert(spec(vec![ReturningItem::Star]));
        let capped = NetworkTuning {
            max_returning_columns: 1_024,
            ..unbounded()
        };
        assert!(can_fail(&star, &capped), "a star has no width cap");
    }

    #[test]
    fn a_division_can_fail() {
        let divide = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Integer(1))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Column("x".into())),
        };
        let plan = point_insert(spec(vec![column("id"), computed("q", divide)]));
        assert!(can_fail(&plan, &unbounded()));
    }

    #[test]
    fn a_sequence_accessor_can_fail() {
        for name in ["nextval", "currval", "setval"] {
            let plan = point_insert(spec(vec![computed("n", sequence_call(name))]));
            assert!(can_fail(&plan, &unbounded()), "{name}");
        }
    }

    #[test]
    fn a_cast_can_fail() {
        let cast = SqlExpr::Cast {
            expr: Box::new(SqlExpr::Column("n".into())),
            to_type: CastType::Int,
        };
        let plan = point_insert(spec(vec![computed("n", cast)]));
        assert!(can_fail(&plan, &unbounded()));
    }

    #[test]
    fn an_update_by_predicate_can_fail() {
        let plan = bulk_update(spec(vec![column("id")]));
        assert!(can_fail(&plan, &unbounded()));
    }

    #[test]
    fn a_multi_row_insert_can_fail() {
        let plan = batch_insert(spec(vec![column("id")]));
        assert!(can_fail(&plan, &unbounded()));
    }

    #[test]
    fn an_undecodable_item_can_fail() {
        let item = ReturningItem::Computed {
            expression: vec![0xc1],
            name: "bad".into(),
        };
        assert!(can_fail(&point_insert(spec(vec![item])), &unbounded()));
    }

    #[test]
    fn a_write_with_no_contract_has_nothing_to_fail() {
        assert!(!can_fail(&bulk_update(None), &NetworkTuning::default()));
        let stored_only = Some(ReturningSpec {
            columns: ReturningColumns::Named(vec![column("id")]),
            precommit: None,
        });
        assert!(!can_fail(
            &batch_insert(stored_only),
            &NetworkTuning::default()
        ));
    }
}
