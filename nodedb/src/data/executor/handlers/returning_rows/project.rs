// SPDX-License-Identifier: BUSL-1.1

//! Project documents into a `RowsPayload`: read policy first, then the
//! `RETURNING` columns.

use crate::data::executor::handlers::rls_eval;
use crate::data::executor::response_codec::{IngestRejection, RejectingRowsPayload, RowsPayload};
use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};
use nodedb_types::{NativeCell, Value};

/// [`build_rows_payload`] for a KV or vector-primary write, whose encode
/// errors name `RETURNING encode`.
pub(in crate::data::executor) fn encode_stored_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    docs: &[Value],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    build_rows_payload(spec, rls_filters, docs, crs).map_err(|e| crate::Error::Internal {
        detail: format!("RETURNING encode: {e}"),
    })
}

/// Project documents per `spec` (Star = field name order, Named = spec
/// order, missing → `Value::Null`) into a `RowsPayload` msgpack blob.
/// `rls_filters` (the compiled read policy) is applied BEFORE projection — a
/// predicate often references a column `RETURNING` omits, so a
/// post-projection check would leak the row. Only the visible row set
/// shrinks; the affected count already counted the write.
pub(in crate::data::executor::handlers) fn build_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    docs: &[Value],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    build_rejecting_rows_payload(spec, rls_filters, docs, None, crs)
}

/// [`build_rows_payload`] for an ingest that rejected rows: with a
/// `rejection`, the blob is a [`RejectingRowsPayload`] that reports them
/// beside the rows.
pub(in crate::data::executor::handlers) fn build_rejecting_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    docs: &[Value],
    rejection: Option<IngestRejection>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    let RowsPayload { columns, rows } = rows_payload(spec, rls_filters, docs, crs)?;
    let encoded = match rejection {
        None => zerompk::to_msgpack_vec(&RowsPayload { columns, rows }),
        Some(rejected) => zerompk::to_msgpack_vec(&RejectingRowsPayload {
            columns,
            rows,
            rejected,
        }),
    };
    encoded.map_err(|e| crate::Error::Codec {
        detail: format!("RowsPayload encode: {e}"),
    })
}

/// The projected, policy-filtered rows of `docs`.
fn rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    docs: &[Value],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<RowsPayload> {
    use nodedb_physical::physical_plan::ReturningItem;
    let visible: Vec<&Value> = docs
        .iter()
        .filter(|doc| rls_eval::rls_check_value(rls_filters, doc, crs))
        .collect();
    let mut star_names: Vec<String> = visible
        .first()
        .and_then(|row| match row {
            Value::Object(fields) => Some(fields.keys().cloned().collect()),
            _ => None,
        })
        .unwrap_or_default();
    star_names.sort_unstable();
    let items = match &spec.columns {
        ReturningColumns::Star => vec![ReturningItem::Star],
        ReturningColumns::Named(items) => items.clone(),
    };
    let mut columns = Vec::new();
    let mut expressions = Vec::with_capacity(items.len());
    for item in &items {
        match item {
            ReturningItem::Star => {
                columns.extend(star_names.iter().cloned());
                expressions.push(None);
            }
            ReturningItem::Column { .. } => {
                columns.push(item.output_name().to_owned());
                expressions.push(None);
            }
            ReturningItem::Computed { expression, name } => {
                columns.push(name.clone());
                let expr = zerompk::from_msgpack::<nodedb_query::expr::SqlExpr>(expression)
                    .map_err(|error| crate::Error::Codec {
                        detail: format!("decode RETURNING expression '{name}': {error}"),
                    })?;
                expressions.push(Some(expr));
            }
        }
    }
    let rows = visible
        .into_iter()
        .map(|doc| {
            let mut row = Vec::with_capacity(columns.len());
            for (item, expr) in items.iter().zip(&expressions) {
                match item {
                    ReturningItem::Star => row.extend(
                        star_names
                            .iter()
                            .map(|name| NativeCell(doc.get(name).cloned().unwrap_or(Value::Null))),
                    ),
                    ReturningItem::Column { name, .. } => {
                        row.push(NativeCell(doc.get(name).cloned().unwrap_or(Value::Null)))
                    }
                    ReturningItem::Computed { name, .. } => {
                        let expression = expr.as_ref().ok_or_else(|| crate::Error::Internal {
                            detail: format!("RETURNING expression '{name}' is not compiled"),
                        })?;
                        let value = expression.eval(doc, crs)?;
                        row.push(NativeCell(value));
                    }
                }
            }
            Ok(row)
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(RowsPayload { columns, rows })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    use crate::util::rmpv_value::rmpv_to_value;
    use nodedb_physical::physical_plan::ReturningItem;
    use serde_json::json;

    /// `owner = <value>`, the shape a compiled read policy lands in.
    fn owner_policy(value: &str) -> Vec<u8> {
        let filter = ScanFilter {
            field: "owner".into(),
            op: FilterOp::Eq,
            value: nodedb_types::Value::String(value.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
    }

    fn named(cols: &[&str]) -> ReturningSpec {
        ReturningSpec {
            columns: ReturningColumns::Named(
                cols.iter()
                    .map(|c| ReturningItem::Column {
                        name: (*c).to_string(),
                        alias: None,
                    })
                    .collect(),
            ),
            precommit: None,
        }
    }

    fn decode(payload: &[u8]) -> RowsPayload {
        zerompk::from_msgpack(payload).expect("decode RowsPayload")
    }

    fn cells(row: &[NativeCell]) -> Vec<Value> {
        row.iter().map(|c| c.0.clone()).collect()
    }

    fn docs() -> Vec<Value> {
        vec![
            Value::from(json!({"id": "r1", "owner": "alice", "note": "hidden"})),
            Value::from(json!({"id": "r2", "owner": "bob", "note": "shown"})),
        ]
    }

    /// An ingest that rejected rows reports them beside its rows. Without a
    /// rejection the blob is a plain `RowsPayload`.
    #[test]
    fn a_rejection_travels_beside_the_rows() {
        use crate::data::executor::response_codec::ReturningRowsReply;
        let rejection = IngestRejection {
            collection: "metrics".into(),
            lines: 2,
        };
        let payload = build_rejecting_rows_payload(
            &named(&["id"]),
            &[],
            &docs(),
            Some(rejection.clone()),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("build");
        let reply: ReturningRowsReply = zerompk::from_msgpack(&payload).expect("decode reply");
        assert_eq!(reply.rows.len(), 2);
        assert_eq!(reply.rejected, Some(rejection));

        let plain = build_rejecting_rows_payload(
            &named(&["id"]),
            &[],
            &docs(),
            None,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("build");
        assert_eq!(decode(&plain).rows.len(), 2);
        let reply: ReturningRowsReply = zerompk::from_msgpack(&plain).expect("decode reply");
        assert!(reply.rejected.is_none());
    }

    #[test]
    fn empty_filters_return_every_row() {
        let payload = build_rows_payload(
            &named(&["id"]),
            &[],
            &docs(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("build");
        let rows = decode(&payload).rows;
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn excluded_rows_are_dropped() {
        let payload = build_rows_payload(
            &named(&["id"]),
            &owner_policy("bob"),
            &docs(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("build");
        let decoded = decode(&payload);
        assert_eq!(decoded.rows.len(), 1);
        assert_eq!(cells(&decoded.rows[0]), vec![Value::String("r2".into())]);
    }

    /// The predicate names a column the projection omits — the filter must
    /// still see it, which it only can before projection.
    #[test]
    fn a_policy_column_outside_the_projection_still_filters() {
        let payload = build_rows_payload(
            &named(&["note"]),
            &owner_policy("bob"),
            &docs(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("build");
        let decoded = decode(&payload);
        assert_eq!(decoded.columns, vec!["note".to_string()]);
        assert_eq!(decoded.rows.len(), 1);
        assert_eq!(cells(&decoded.rows[0]), vec![Value::String("shown".into())]);
    }

    /// `RETURNING *` derives its column list from the first VISIBLE row, so a
    /// hidden leading row cannot dictate the shape of the result.
    #[test]
    fn star_columns_come_from_the_first_visible_row() {
        let docs = vec![
            Value::from(json!({"id": "r1", "owner": "alice", "secret": "hidden"})),
            Value::from(json!({"id": "r2", "owner": "bob"})),
        ];
        let spec = ReturningSpec {
            columns: ReturningColumns::Star,
            precommit: None,
        };
        let payload = build_rows_payload(
            &spec,
            &owner_policy("bob"),
            &docs,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("build");
        let decoded = decode(&payload);
        assert_eq!(decoded.columns, vec!["id".to_string(), "owner".to_string()]);
        assert_eq!(decoded.rows.len(), 1);
    }

    /// A malformed policy denies rather than passing rows through.
    #[test]
    fn a_corrupt_policy_returns_no_rows() {
        let payload = build_rows_payload(
            &named(&["id"]),
            &[0xFF, 0xFE],
            &docs(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("build");
        assert!(decode(&payload).rows.is_empty());
    }

    /// Cells keep their stored type: an instant stays an instant, a missing
    /// field is SQL NULL, a nested object stays nested.
    #[test]
    fn cells_keep_their_stored_type() {
        let at = nodedb_types::NdbDateTime::from_micros(1_583_402_400_000_000);
        let mut doc = std::collections::HashMap::new();
        doc.insert("at".to_string(), Value::NaiveDateTime(at));
        doc.insert("n".to_string(), Value::Integer(7));
        doc.insert(
            "meta".to_string(),
            Value::Object(std::collections::HashMap::from([(
                "k".to_string(),
                Value::Bool(true),
            )])),
        );
        let payload = build_rows_payload(
            &named(&["at", "n", "meta", "absent"]),
            &[],
            &[Value::Object(doc)],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("build");
        let decoded = decode(&payload);
        assert_eq!(
            cells(&decoded.rows[0]),
            vec![
                Value::NaiveDateTime(at),
                Value::Integer(7),
                Value::Object(std::collections::HashMap::from([(
                    "k".to_string(),
                    Value::Bool(true),
                )])),
                Value::Null,
            ]
        );
    }

    /// A timeseries row's instant ext crosses into the payload typed.
    #[test]
    fn a_timeseries_instant_cell_stays_typed() {
        let at = nodedb_types::NdbDateTime::from_micros(42);
        let row = crate::util::rmpv_value::value_to_rmpv(&Value::Object(
            std::collections::HashMap::from([("t".to_string(), Value::DateTime(at))]),
        ));
        let docs = vec![rmpv_to_value(&row)];
        let payload = build_rows_payload(
            &named(&["t"]),
            &[],
            &docs,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("build");
        assert_eq!(cells(&decode(&payload).rows[0]), vec![Value::DateTime(at)]);
    }
    #[test]
    fn native_expression_evaluates_after_read_policy() {
        use nodedb_query::expr::{BinaryOp, SqlExpr};
        let expression = zerompk::to_msgpack_vec(&SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Integer(8))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Column("divisor".into())),
        })
        .expect("expression");
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![ReturningItem::Computed {
                expression,
                name: "ratio".into(),
            }]),
            precommit: None,
        };
        let rows = vec![
            Value::from(json!({"owner":"alice", "divisor":0})),
            Value::from(json!({"owner":"bob", "divisor":2})),
        ];
        let payload = build_rows_payload(
            &spec,
            &owner_policy("bob"),
            &rows,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("visible expression");
        let payload = decode(&payload);
        assert_eq!(payload.columns, ["ratio"]);
        assert_eq!(cells(&payload.rows[0]), [Value::Integer(4)]);
        assert!(
            build_rows_payload(&spec, &[], &rows, &nodedb_crs::CrsContext::builtin_only()).is_err()
        );
    }
}
