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
//! - One row of the list fits the configured row, column, cell and payload
//!   byte limits.
//!
//! The payload byte limit is `network.max_query_result_bytes`. No write caps
//! the size of a stored row, and a literal's size is the statement's. So one
//! row can exceed any finite byte limit, and only a node with no byte limit
//! proves the bytes fit. A `*` expands to the row's fields, and a schemaless
//! row has no field cap. So a `*` fits only with no column or cell limit.

use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};
use nodedb_query::expr::SqlExpr;
use nodedb_types::config::tuning::NetworkTuning;
use nodedb_types::{ReturningColumns, ReturningItem};

use super::precommit::ReturningLimits;

/// Whether `plan`'s `RETURNING` contract can fail under the limits `network`
/// sets. A plan with no contract has nothing to fail.
pub fn returning_can_fail(plan: &PhysicalPlan, network: &NetworkTuning) -> bool {
    let Some(list) = plan
        .returning_target()
        .and_then(|(_, spec)| spec.precommit.as_ref())
    else {
        return false;
    };
    if !writes_at_most_one_row(plan) {
        return true;
    }
    match ListWidth::of(list) {
        Some(width) => !width.fits_one_row(&ReturningLimits::from_tuning(network)),
        None => true,
    }
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

/// The item count of a list whose items cannot fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListWidth {
    /// Columns and literals only: one cell each.
    Fixed(usize),
    /// A `*`: as many cells as the row has fields.
    Star,
}

impl ListWidth {
    /// The width of `list`. `None` when an item can fail.
    fn of(list: &ReturningColumns) -> Option<Self> {
        let items = match list {
            ReturningColumns::Star => return Some(Self::Star),
            ReturningColumns::Named(items) => items,
        };
        let mut star = false;
        for item in items {
            match item {
                ReturningItem::Star => star = true,
                ReturningItem::Column { .. } => {}
                // Any other expression can raise an error. An item that does
                // not decode fails its evaluation.
                ReturningItem::Computed { expression, .. } => {
                    if !matches!(
                        zerompk::from_msgpack::<SqlExpr>(expression),
                        Ok(SqlExpr::Literal(_))
                    ) {
                        return None;
                    }
                }
            }
        }
        Some(if star {
            Self::Star
        } else {
            Self::Fixed(items.len())
        })
    }

    /// Whether one row of this width fits `limits`.
    fn fits_one_row(self, limits: &ReturningLimits) -> bool {
        if limits.rows < 1 || limits.payload_bytes != usize::MAX {
            return false;
        }
        match self {
            Self::Fixed(width) => width <= limits.columns && width <= limits.cells,
            Self::Star => limits.columns == usize::MAX && limits.cells == usize::MAX,
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::UpdateValue;
    use nodedb_query::expr::{BinaryOp, CastType};
    use nodedb_types::{
        DatabaseId, QualifiedCollection, ReturningSpec, RlsWriteCheck, Surrogate, Value,
    };

    use super::*;

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
        assert!(!returning_can_fail(&plan, &unbounded()));
    }

    #[test]
    fn columns_literals_and_a_star_on_point_writes_cannot_fail() {
        let items = vec![
            column("id"),
            ReturningItem::Star,
            computed("one", SqlExpr::Literal(Value::Integer(1))),
        ];
        assert!(!returning_can_fail(
            &point_insert(spec(items.clone())),
            &unbounded()
        ));
        assert!(!returning_can_fail(
            &point_update(spec(items)),
            &unbounded()
        ));
    }

    /// No write caps a stored row, so one row can exceed a byte limit.
    #[test]
    fn a_byte_limit_makes_a_column_list_fallible() {
        let plan = point_insert(spec(vec![column("id")]));
        let network = NetworkTuning {
            max_query_result_bytes: 1 << 30,
            ..unbounded()
        };
        assert!(returning_can_fail(&plan, &network));
        assert!(returning_can_fail(&plan, &NetworkTuning::default()));
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
            assert!(returning_can_fail(&plan, &network));
        }
        let star = point_insert(spec(vec![ReturningItem::Star]));
        let capped = NetworkTuning {
            max_returning_columns: 1_024,
            ..unbounded()
        };
        assert!(
            returning_can_fail(&star, &capped),
            "a star has no width cap"
        );
    }

    #[test]
    fn a_division_can_fail() {
        let divide = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Integer(1))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Column("x".into())),
        };
        let plan = point_insert(spec(vec![column("id"), computed("q", divide)]));
        assert!(returning_can_fail(&plan, &unbounded()));
    }

    #[test]
    fn a_sequence_accessor_can_fail() {
        for name in ["nextval", "currval", "setval"] {
            let plan = point_insert(spec(vec![computed("n", sequence_call(name))]));
            assert!(returning_can_fail(&plan, &unbounded()), "{name}");
        }
    }

    #[test]
    fn a_cast_can_fail() {
        let cast = SqlExpr::Cast {
            expr: Box::new(SqlExpr::Column("n".into())),
            to_type: CastType::Int,
        };
        let plan = point_insert(spec(vec![computed("n", cast)]));
        assert!(returning_can_fail(&plan, &unbounded()));
    }

    #[test]
    fn an_update_by_predicate_can_fail() {
        let plan = bulk_update(spec(vec![column("id")]));
        assert!(returning_can_fail(&plan, &unbounded()));
    }

    #[test]
    fn a_multi_row_insert_can_fail() {
        let plan = batch_insert(spec(vec![column("id")]));
        assert!(returning_can_fail(&plan, &unbounded()));
    }

    #[test]
    fn an_undecodable_item_can_fail() {
        let item = ReturningItem::Computed {
            expression: vec![0xc1],
            name: "bad".into(),
        };
        assert!(returning_can_fail(
            &point_insert(spec(vec![item])),
            &unbounded()
        ));
    }

    #[test]
    fn a_write_with_no_contract_has_nothing_to_fail() {
        assert!(!returning_can_fail(
            &bulk_update(None),
            &NetworkTuning::default()
        ));
        let stored_only = Some(ReturningSpec {
            columns: ReturningColumns::Named(vec![column("id")]),
            precommit: None,
        });
        assert!(!returning_can_fail(
            &batch_insert(stored_only),
            &NetworkTuning::default()
        ));
    }
}
