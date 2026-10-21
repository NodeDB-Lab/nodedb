// SPDX-License-Identifier: BUSL-1.1

//! The one `INSERT ... ON CONFLICT (key) DO UPDATE SET` post-image for a KV
//! row.
//!
//! The live handler, the transaction resolver, statement staging, and WAL
//! replay all compute the post-image here, so a staged value, its durable
//! replay, and the autocommit write never diverge. A KV body is either raw
//! scalar bytes or a msgpack map; the merge decodes both sides through
//! `kv_body_to_row` and re-encodes in the existing body's shape, so a raw
//! row stays raw and RESP `GET` keeps returning the bare value.

use nodedb_physical::physical_plan::{DeclaredColumn, UpdateValue};
use nodedb_query::msgpack_scan::{KvBodyError, kv_body_to_row, row_to_kv_body};
use nodedb_types::Value;

use crate::data::executor::handlers::kv::declared_body::coerce_kv_body;
use crate::data::executor::handlers::upsert::apply_on_conflict_updates;
use crate::data::executor::strict_format::coerce_declared_row;

/// The body the upsert stores. With no `existing` row it is `incoming`.
/// Otherwise it is `updates` applied to `existing`, with `incoming` as the
/// `EXCLUDED` row, in `existing`'s shape.
///
/// Every `declared` column of the stored body holds the value its type
/// stores, in both branches.
pub(in crate::data::executor) fn merge_kv_conflict_body(
    existing: Option<&[u8]>,
    incoming: &[u8],
    updates: &[(String, UpdateValue)],
    declared: &[DeclaredColumn],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    let Some(existing) = existing else {
        return Ok(coerce_kv_body(incoming, declared)?.into_owned());
    };
    let (existing_row, shape) = kv_body_to_row(existing).map_err(KvBodyError::from)?;
    let (excluded_row, _) = kv_body_to_row(incoming).map_err(KvBodyError::from)?;
    let mut merged = apply_on_conflict_updates(existing_row, &excluded_row, updates, crs)?;
    if let Value::Object(map) = &mut merged {
        coerce_declared_row(map, declared)?;
    }
    Ok(row_to_kv_body(&merged, shape)?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_physical::physical_plan::{DeclaredColumn, UpdateValue};
    use nodedb_query::SqlExpr;
    use nodedb_types::Value;

    use super::merge_kv_conflict_body;

    fn map_body(fields: &[(&str, i64)]) -> Vec<u8> {
        let map: HashMap<String, Value> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Integer(*v)))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode body")
    }

    fn literal(v: Value) -> UpdateValue {
        UpdateValue::Literal(nodedb_types::value_to_msgpack(&v).expect("encode literal"))
    }

    /// `SET value = EXCLUDED.value`, the bare-form overwrite.
    fn set_value_from_excluded() -> Vec<(String, UpdateValue)> {
        vec![(
            "value".to_string(),
            UpdateValue::Expr(SqlExpr::ExcludedColumn("value".to_string())),
        )]
    }

    #[test]
    fn raw_body_overwritten_from_excluded_stays_raw() {
        let merged = merge_kv_conflict_body(
            Some(b"first".as_slice()),
            b"second-longer-value",
            &set_value_from_excluded(),
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("merge");
        assert_eq!(merged, b"second-longer-value".to_vec());
    }

    #[test]
    fn raw_single_byte_body_keeps_its_shape() {
        // 0x31 is a msgpack fixint; the merge must still treat it as the
        // string "1" and write back "2" as one raw byte.
        let merged = merge_kv_conflict_body(
            Some(b"1".as_slice()),
            b"2",
            &set_value_from_excluded(),
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("merge");
        assert_eq!(merged, b"2".to_vec());
    }

    #[test]
    fn raw_body_with_literal_value_assignment_stays_raw() {
        let updates = vec![("value".to_string(), literal(Value::String("lit".into())))];
        let merged = merge_kv_conflict_body(
            Some(b"first".as_slice()),
            b"ignored",
            &updates,
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("merge");
        assert_eq!(merged, b"lit".to_vec());
    }

    #[test]
    fn raw_body_refuses_a_typed_column_assignment() {
        let updates = vec![("n".to_string(), literal(Value::Integer(1)))];
        let err = merge_kv_conflict_body(
            Some(b"first".as_slice()),
            b"second",
            &updates,
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("a raw row cannot grow a typed column");
        assert!(
            matches!(err, crate::Error::BadRequest { .. }),
            "got {err:?}"
        );
        assert!(err.to_string().contains("cannot set n"), "{err}");
    }

    #[test]
    fn map_body_merges_and_stays_a_map() {
        let updates = vec![("mana".to_string(), literal(Value::Integer(5)))];
        let merged = merge_kv_conflict_body(
            Some(map_body(&[("hp", 10)]).as_slice()),
            &map_body(&[("hp", 1)]),
            &updates,
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("merge");
        let row = nodedb_types::value_from_msgpack(&merged).expect("map body");
        assert_eq!(row.get("hp"), Some(&Value::Integer(10)));
        assert_eq!(row.get("mana"), Some(&Value::Integer(5)));
    }

    #[test]
    fn map_body_reads_excluded_columns() {
        let updates = vec![(
            "hp".to_string(),
            UpdateValue::Expr(SqlExpr::ExcludedColumn("hp".to_string())),
        )];
        let merged = merge_kv_conflict_body(
            Some(map_body(&[("hp", 10)]).as_slice()),
            &map_body(&[("hp", 1)]),
            &updates,
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("merge");
        let row = nodedb_types::value_from_msgpack(&merged).expect("map body");
        assert_eq!(row.get("hp"), Some(&Value::Integer(1)));
    }

    /// A computed assignment past a declared `SMALLINT` is refused, and the
    /// insert branch re-types the incoming row.
    #[test]
    fn declared_columns_are_retyped_in_both_branches() {
        let declared = [DeclaredColumn::from_declared("hp", "SMALLINT").expect("smallint")];
        let updates = vec![(
            "hp".to_string(),
            UpdateValue::Expr(SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Column("hp".to_string())),
                op: nodedb_query::BinaryOp::Add,
                right: Box::new(SqlExpr::Literal(Value::Integer(39999))),
            }),
        )];
        let err = merge_kv_conflict_body(
            Some(map_body(&[("hp", 10)]).as_slice()),
            &map_body(&[("hp", 1)]),
            &updates,
            &declared,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("39999 + 10 is past smallint");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );

        let err = merge_kv_conflict_body(
            None,
            &map_body(&[("hp", 40000)]),
            &updates,
            &declared,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("the insert row is past smallint");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
    }
}
