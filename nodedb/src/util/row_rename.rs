// SPDX-License-Identifier: BUSL-1.1

//! Renaming the columns of a MessagePack result-row array.
//!
//! An aggregate keys each GROUP BY value by its internal row name: the bare
//! column, or `group_{index}` for a computed key. HAVING and ORDER BY read
//! those names. Once the rows are final, an aliased key takes its SELECT-list
//! name, so a derived table or an outer query reads it by that name.

use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};
use nodedb_types::{ObjectFormat, ObjectView};

/// The tag of a native object `[7, {field: tagged value}]`, as a positive fixint.
const NATIVE_OBJECT_TAG: u8 = 0x07;

/// The `(row name, alias)` pairs that give each aliased GROUP BY key its
/// SELECT-list name.
///
/// `aliases` is parallel to `group_by`. A missing entry, a `None` entry, or
/// an alias equal to the row name renames nothing. An alias another cell of
/// the row already ends under (an aggregate, a kept key, an earlier renamed
/// key) renames nothing either, so a row never holds two cells under one key.
/// The response schema reads each key by the same rule.
pub fn group_key_renames(
    group_by: &[GroupKeySpec],
    aliases: &[Option<String>],
    aggregates: &[AggregateSpec],
) -> Vec<(String, String)> {
    let row_names: Vec<String> = group_by
        .iter()
        .map(|spec| spec.output_name.clone())
        .collect();
    let aggregate_names: Vec<&str> = aggregates
        .iter()
        .map(|agg| agg.user_alias.as_deref().unwrap_or(&agg.alias))
        .collect();
    nodedb_sql::planner::agg_naming::group_key_final_names(&row_names, aliases, &aggregate_names)
        .into_iter()
        .zip(row_names)
        .filter(|(target, row)| target != row)
        .map(|(target, row)| (row, target))
        .collect()
}

/// `payload`, a MessagePack array of row objects, with every `from` key of
/// each row renamed to its `to`.
///
/// Each row is a plain map or a native object `[7, {field: tagged value}]`.
/// A renamed row keeps its own encoding and its field bytes unchanged. An
/// empty `renames` returns the payload unchanged. A payload that is not an
/// array of row objects is an error: an aggregate result always is one.
pub fn rename_row_keys(payload: &[u8], renames: &[(String, String)]) -> crate::Result<Vec<u8>> {
    if renames.is_empty() {
        return Ok(payload.to_vec());
    }
    let (count, mut pos) =
        nodedb_query::msgpack_scan::array_header(payload, 0).ok_or_else(|| {
            crate::Error::Codec {
                detail: "aggregate result rename: the result is not an array of rows".into(),
            }
        })?;
    let mut renamed = Vec::with_capacity(payload.len());
    nodedb_query::msgpack_scan::write_array_header(&mut renamed, count);
    for index in 0..count {
        let row = nodedb_types::msgpack_span::skip_value(payload, pos)
            .and_then(|end| payload.get(pos..end).map(|row| (row, end)));
        let Some((row, end)) = row else {
            return Err(crate::Error::Codec {
                detail: format!("aggregate result rename: row {index} is truncated"),
            });
        };
        let view = ObjectView::new(row).map_err(|e| crate::Error::Codec {
            detail: format!("aggregate result rename: row {index} is not an object: {e}"),
        })?;
        write_renamed_row(&mut renamed, view, renames).map_err(|e| crate::Error::Codec {
            detail: format!("aggregate result rename: row {index}: {e}"),
        })?;
        pos = end;
    }
    if pos != payload.len() {
        return Err(crate::Error::Codec {
            detail: "aggregate result rename: bytes follow the row array".into(),
        });
    }
    Ok(renamed)
}

/// Append `view` to `out` in its own encoding, with each renamed key replaced.
fn write_renamed_row(
    out: &mut Vec<u8>,
    view: ObjectView<'_>,
    renames: &[(String, String)],
) -> nodedb_types::MsgpackResult<()> {
    let fields = view
        .fields()
        .collect::<nodedb_types::MsgpackResult<Vec<_>>>()?;
    if view.format() == ObjectFormat::Native {
        nodedb_query::msgpack_scan::write_array_header(out, 2);
        out.push(NATIVE_OBJECT_TAG);
    }
    nodedb_query::msgpack_scan::write_map_header(out, fields.len());
    for (key, bytes) in fields {
        let key = renames
            .iter()
            .find(|(from, _)| from == key)
            .map_or(key, |(_, to)| to.as_str());
        nodedb_query::msgpack_scan::write_str(out, key);
        out.extend_from_slice(bytes);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(value: &rmpv::Value) -> Vec<u8> {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, value).expect("encode");
        buf
    }

    #[test]
    fn aliased_keys_take_their_select_list_name() {
        let group_by = vec![
            GroupKeySpec::column("station"),
            GroupKeySpec::column("host"),
            GroupKeySpec {
                output_name: "group_2".into(),
                field: None,
                expr: None,
            },
        ];
        let aliases = vec![Some("s".into()), None, Some("b".into())];
        let renames = group_key_renames(&group_by, &aliases, &[]);
        assert_eq!(
            renames,
            vec![
                ("station".to_string(), "s".to_string()),
                ("group_2".to_string(), "b".to_string()),
            ]
        );
        let payload = encode(&rmpv::Value::Array(vec![rmpv::Value::Map(vec![
            (rmpv::Value::from("station"), rmpv::Value::from("TMR")),
            (rmpv::Value::from("host"), rmpv::Value::from("h1")),
            (rmpv::Value::from("group_2"), rmpv::Value::from(7)),
            (rmpv::Value::from("n"), rmpv::Value::from(2)),
        ])]));
        let renamed = rename_row_keys(&payload, &renames).expect("rename");
        assert_eq!(
            crate::util::bounded_msgpack::read_value(&renamed).expect("decode"),
            rmpv::Value::Array(vec![rmpv::Value::Map(vec![
                (rmpv::Value::from("s"), rmpv::Value::from("TMR")),
                (rmpv::Value::from("host"), rmpv::Value::from("h1")),
                (rmpv::Value::from("b"), rmpv::Value::from(7)),
                (rmpv::Value::from("n"), rmpv::Value::from(2)),
            ])])
        );
    }

    #[test]
    fn a_self_alias_renames_nothing() {
        let renames = group_key_renames(
            &[GroupKeySpec::column("station")],
            &[Some("station".into())],
            &[],
        );
        assert!(renames.is_empty());
    }

    #[test]
    fn native_rows_keep_their_encoding_and_value_types() {
        use nodedb_types::Value;
        use std::collections::HashMap;

        let decimal = rust_decimal::Decimal::new(1234, 2);
        let row = Value::Object(HashMap::from([
            ("group_0".to_string(), Value::Decimal(decimal)),
            ("n".to_string(), Value::Integer(2)),
        ]));
        let payload =
            crate::data::executor::response_codec::encode_native_rows(&[row]).expect("encode rows");
        let renamed = rename_row_keys(&payload, &[("group_0".into(), "b".into())]).expect("rename");

        let (count, start) =
            nodedb_query::msgpack_scan::array_header(&renamed, 0).expect("row array");
        assert_eq!(count, 1);
        let row_bytes = &renamed[start..];
        let view = ObjectView::new(row_bytes).expect("one object");
        assert_eq!(view.format(), ObjectFormat::Native);
        assert_eq!(
            nodedb_types::decode_object(row_bytes).expect("decode"),
            Value::Object(HashMap::from([
                ("b".to_string(), Value::Decimal(decimal)),
                ("n".to_string(), Value::Integer(2)),
            ]))
        );
    }

    fn count_as(user_alias: &str) -> AggregateSpec {
        AggregateSpec {
            function: "count".into(),
            alias: "count(*)".into(),
            user_alias: Some(user_alias.into()),
            field: "*".into(),
            expr: None,
            param: None,
        }
    }

    /// `SELECT a AS x, b AS x, count(*) AS n ... GROUP BY a, b` and
    /// `SELECT a AS n, count(*) AS n ...` keep every cell: a rename never
    /// lands on a key another cell of the row ends under.
    #[test]
    fn a_taken_alias_renames_nothing_and_every_cell_survives() {
        let group_by = vec![GroupKeySpec::column("a"), GroupKeySpec::column("b")];
        let renames = group_key_renames(
            &group_by,
            &[Some("x".into()), Some("x".into())],
            &[count_as("n")],
        );
        assert_eq!(renames, vec![("a".to_string(), "x".to_string())]);
        let payload = encode(&rmpv::Value::Array(vec![rmpv::Value::Map(vec![
            (rmpv::Value::from("a"), rmpv::Value::from(1)),
            (rmpv::Value::from("b"), rmpv::Value::from(2)),
            (rmpv::Value::from("n"), rmpv::Value::from(3)),
        ])]));
        assert_eq!(
            crate::util::bounded_msgpack::read_value(
                &rename_row_keys(&payload, &renames).expect("rename")
            )
            .expect("decode"),
            rmpv::Value::Array(vec![rmpv::Value::Map(vec![
                (rmpv::Value::from("x"), rmpv::Value::from(1)),
                (rmpv::Value::from("b"), rmpv::Value::from(2)),
                (rmpv::Value::from("n"), rmpv::Value::from(3)),
            ])])
        );

        let renames = group_key_renames(
            &[GroupKeySpec::column("a")],
            &[Some("n".into())],
            &[count_as("n")],
        );
        assert!(renames.is_empty());
    }

    #[test]
    fn a_row_that_is_not_an_object_is_an_error() {
        let payload = encode(&rmpv::Value::Array(vec![rmpv::Value::from(7)]));
        assert!(rename_row_keys(&payload, &[("a".into(), "b".into())]).is_err());
    }
}
