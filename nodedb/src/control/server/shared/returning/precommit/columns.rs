// SPDX-License-Identifier: BUSL-1.1

//! Source column order and the compiled output items of a response.

use std::collections::HashSet;

use nodedb_query::expr::SqlExpr;

use crate::control::server::response_shape::types::ShapedRow;
use crate::control::server::shared::returning::{ReturningColumns, ReturningItem, ReturningSpec};

pub(super) fn source_columns(declared: &[String], rows: &[ShapedRow]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();
    for name in declared
        .iter()
        .chain(rows.iter().flat_map(|row| row.keys()))
    {
        if seen.insert(name.as_str()) {
            columns.push(name.clone());
        }
    }
    columns
}

pub(super) fn compile_items(
    spec: &ReturningSpec,
    source: &[String],
) -> crate::Result<Vec<(String, SqlExpr)>> {
    let expand_star = || {
        source
            .iter()
            .map(|name| (name.clone(), SqlExpr::Column(name.clone())))
    };
    match &spec.columns {
        ReturningColumns::Star => Ok(expand_star().collect()),
        ReturningColumns::Named(items) => {
            let mut compiled = Vec::new();
            for item in items {
                match item {
                    ReturningItem::Star => compiled.extend(expand_star()),
                    ReturningItem::Column { name, alias } => compiled.push((
                        alias.as_ref().unwrap_or(name).clone(),
                        SqlExpr::Column(name.clone()),
                    )),
                    ReturningItem::Computed { expression, name } => {
                        let expression =
                            zerompk::from_msgpack::<SqlExpr>(expression).map_err(|error| {
                                crate::Error::Codec {
                                    detail: format!(
                                        "RETURNING expression {name:?} decode: {error}"
                                    ),
                                }
                            })?;
                        compiled.push((name.clone(), expression));
                    }
                }
            }
            Ok(compiled)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_query::expr::BinaryOp;
    use nodedb_types::{DatabaseId, TenantId, Value};

    use super::super::fixtures::{call, computed, finalize, limits, literal, registry};
    use super::super::types::PrecommitReturning;
    use super::*;
    use crate::control::security::redaction::RedactionStore;
    use crate::control::sequence::{SequenceAccess, SessionSequenceAccess, SessionSequenceValues};
    use crate::control::server::shared::returning::transcript::AccessorTranscript;

    #[test]
    fn zero_rows_and_duplicate_names_keep_positional_columns() {
        let snapshot = RedactionStore::new().snapshot_rules(TenantId::new(7), "users", &[]);
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                ReturningItem::Column {
                    name: "b".into(),
                    alias: Some("same".into()),
                },
                ReturningItem::Column {
                    name: "a".into(),
                    alias: Some("same".into()),
                },
                ReturningItem::Star,
            ]),
            precommit: None,
        };
        let source = vec!["b".into(), "a".into()];
        let crs = nodedb_crs::CustomCrsSet::empty();
        let request = PrecommitReturning {
            spec: &spec,
            source_columns: &source,
            redaction: &snapshot,
            sequences: &crate::control::server::response_shape::stamp::NoSequenceAccess,
            projection: None,
            crs_set: crs,
            limits: limits(),
        };
        let empty =
            finalize(&request, Vec::new(), &AccessorTranscript::default()).expect("empty rows");
        assert_eq!(empty.shaped.columns, ["same", "same", "b", "a"]);
        assert!(empty.shaped.rows.is_empty());
        let row = ShapedRow::from([
            ("a".into(), Value::Integer(1)),
            ("b".into(), Value::Integer(2)),
        ]);
        let result = finalize(&request, vec![row], &AccessorTranscript::default()).expect("rows");
        let keys = result.shaped.cell_keys();
        assert_eq!(result.shaped.rows[0][&keys[0]], Value::Integer(2));
        assert_eq!(result.shaped.rows[0][&keys[1]], Value::Integer(1));
        let decoded: crate::data::executor::response_codec::ReturningRowsReply =
            zerompk::from_msgpack(&result.payload).expect("decode rows");
        assert_eq!(
            decoded.rows[0]
                .iter()
                .map(|cell| cell.0.clone())
                .collect::<Vec<_>>(),
            [
                Value::Integer(2),
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(1)
            ]
        );
    }
    #[test]
    fn output_aliases_never_replace_source_columns_expressions_sequences_or_star() {
        let registry = registry();
        let access = SessionSequenceAccess::new(
            &registry,
            Some(Arc::new(SessionSequenceValues::new())),
            DatabaseId::new(1),
            TenantId::new(7),
        );
        let snapshot = RedactionStore::new().snapshot_rules(TenantId::new(7), "users", &[]);
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                ReturningItem::Column {
                    name: "a".into(),
                    alias: Some("b".into()),
                },
                ReturningItem::Column {
                    name: "b".into(),
                    alias: None,
                },
                computed(
                    "b",
                    SqlExpr::BinaryOp {
                        left: Box::new(SqlExpr::Column("a".into())),
                        op: BinaryOp::Add,
                        right: Box::new(literal(Value::Integer(1))),
                    },
                ),
                computed("read_b", SqlExpr::Column("b".into())),
                computed("seq", literal(Value::String("other".into()))),
                computed("n", call("nextval", vec![SqlExpr::Column("seq".into())])),
                ReturningItem::Star,
            ]),
            precommit: None,
        };
        let source = vec!["a".into(), "b".into(), "seq".into()];
        let request = PrecommitReturning {
            spec: &spec,
            source_columns: &source,
            redaction: &snapshot,
            sequences: &access,
            projection: None,
            crs_set: nodedb_crs::CustomCrsSet::empty(),
            limits: limits(),
        };
        let result = finalize(
            &request,
            vec![ShapedRow::from([
                ("a".into(), Value::Integer(9)),
                ("b".into(), Value::Integer(2)),
                ("seq".into(), Value::String("s".into())),
            ])],
            &AccessorTranscript::default(),
        )
        .expect("finalize collisions");
        let decoded: crate::data::executor::response_codec::ReturningRowsReply =
            zerompk::from_msgpack(&result.payload).expect("decode collisions");
        assert_eq!(
            decoded.rows[0]
                .iter()
                .map(|cell| cell.0.clone())
                .collect::<Vec<_>>(),
            [
                Value::Integer(9),
                Value::Integer(2),
                Value::Integer(10),
                Value::Integer(2),
                Value::String("other".into()),
                Value::Integer(1),
                Value::Integer(9),
                Value::Integer(2),
                Value::String("s".into()),
            ]
        );
        assert_eq!(access.currval("s").expect("original sequence"), 1);
        assert_eq!(access.nextval("other").expect("unreferenced sequence"), 1);
    }
}
