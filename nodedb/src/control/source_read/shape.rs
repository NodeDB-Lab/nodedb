// SPDX-License-Identifier: BUSL-1.1

//! Shaping of one source row into the row an orchestrated write consumes.

use crate::bridge::expr_eval::ComputedColumn;

/// The column map of a source relation, decoded once per statement.
pub(crate) struct RowShaper {
    /// One entry per target column: its `alias` is the target column and its
    /// `expr` reads the source row. Empty passes each row through.
    columns: Vec<ComputedColumn>,
}

impl RowShaper {
    /// Decode a serialized `Vec<ComputedColumn>`. Empty bytes pass every row
    /// through unchanged.
    pub(crate) fn decode(column_map: &[u8]) -> crate::Result<Self> {
        let columns = if column_map.is_empty() {
            Vec::new()
        } else {
            zerompk::from_msgpack(column_map).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("source column map: {e}"),
            })?
        };
        Ok(Self { columns })
    }

    /// Shape one msgpack source row. A source column absent from the row
    /// yields SQL NULL, as on every other projection path. A division by zero
    /// in a bound expression fails the statement instead of writing NULL.
    pub(crate) fn shape(
        &self,
        row: Vec<u8>,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<Vec<u8>> {
        if self.columns.is_empty() {
            return Ok(row);
        }
        let row =
            nodedb_types::value_from_msgpack(&row).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("source row: {e}"),
            })?;
        self.shape_value(&row, crs)
    }

    /// Shape one decoded source row.
    pub(crate) fn shape_value(
        &self,
        row: &nodedb_types::Value,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<Vec<u8>> {
        if self.columns.is_empty() {
            return encode_row(row);
        }
        let mut shaped = std::collections::HashMap::with_capacity(self.columns.len());
        for column in &self.columns {
            shaped.insert(column.alias.clone(), column.expr.eval(row, crs)?);
        }
        encode_row(&nodedb_types::Value::Object(shaped))
    }
}

fn encode_row(row: &nodedb_types::Value) -> crate::Result<Vec<u8>> {
    nodedb_types::value_to_msgpack(row).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("shaped source row: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::expr_eval::SqlExpr;
    use nodedb_types::Value;

    fn row(pairs: &[(&str, Value)]) -> Vec<u8> {
        let map = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode row")
    }

    fn decode(bytes: &[u8]) -> Value {
        nodedb_types::value_from_msgpack(bytes).expect("decode row")
    }

    #[test]
    fn an_empty_map_passes_the_row_through() {
        let shaper = RowShaper::decode(&[]).expect("decode map");
        let input = row(&[("a", Value::Integer(1))]);
        assert_eq!(
            shaper
                .shape(input.clone(), &nodedb_crs::CrsContext::builtin_only())
                .expect("shape"),
            input
        );
    }

    #[test]
    fn a_map_renames_and_drops_columns() {
        let columns = vec![ComputedColumn {
            alias: "target".into(),
            expr: SqlExpr::Column("out".into()),
        }];
        let bytes = zerompk::to_msgpack_vec(&columns).expect("encode map");
        let shaper = RowShaper::decode(&bytes).expect("decode map");
        let shaped = shaper
            .shape(
                row(&[("out", Value::Integer(7)), ("extra", Value::Bool(true))]),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .expect("shape");
        let Value::Object(map) = decode(&shaped) else {
            panic!("shaped row is not an object");
        };
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("target"), Some(&Value::Integer(7)));
    }

    #[test]
    fn an_absent_source_column_is_null() {
        let columns = vec![ComputedColumn {
            alias: "target".into(),
            expr: SqlExpr::Column("missing".into()),
        }];
        let bytes = zerompk::to_msgpack_vec(&columns).expect("encode map");
        let shaper = RowShaper::decode(&bytes).expect("decode map");
        let shaped = shaper
            .shape(row(&[]), &nodedb_crs::CrsContext::builtin_only())
            .expect("shape");
        let Value::Object(map) = decode(&shaped) else {
            panic!("shaped row is not an object");
        };
        assert_eq!(map.get("target"), Some(&Value::Null));
    }
}
