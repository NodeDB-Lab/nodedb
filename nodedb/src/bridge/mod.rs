// SPDX-License-Identifier: BUSL-1.1

pub mod admission_chokepoint;
pub mod dispatch;
pub mod envelope;
pub mod quiesce;

// Shared query engine re-exports. Origin's internal code names
// `crate::bridge::expr_eval`, `crate::bridge::json_ops`,
// `crate::bridge::scan_filter`, and `crate::bridge::window_func`; the types
// behind them come from nodedb-query.
pub mod expr_eval {
    pub use nodedb_query::expr::{BinaryOp, CastType, ComputedColumn, SqlExpr};
}
pub mod scan_filter;
pub mod window_func {
    pub use nodedb_query::window::*;

    /// Evaluate `specs` over MessagePack-encoded object rows, in place.
    ///
    /// The rows keep the `nodedb_types::Value` codec end to end. Decoding them
    /// to JSON first downgrades tagged cells — MessagePack binary has no JSON
    /// representation, so a `BYTES` cell becomes a base64 `String` — and every
    /// cell the window set does not even touch changes shape with it.
    ///
    /// Every step fails loudly: a malformed row, a non-object row, or an
    /// evaluator column count that does not match the rows is an internal
    /// error, never a silently skipped row.
    pub fn evaluate_window_functions_on_msgpack_rows(
        rows: &mut [Vec<u8>],
        specs: &[WindowFuncSpec],
    ) -> crate::Result<()> {
        if specs.is_empty() || rows.is_empty() {
            return Ok(());
        }

        let mut maps: Vec<nodedb_types::Value> = Vec::with_capacity(rows.len());
        let mut column_index: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for row in rows.iter() {
            let value =
                nodedb_types::value_from_msgpack(row).map_err(|e| crate::Error::Internal {
                    detail: format!("window row decode: {e}"),
                })?;
            let nodedb_types::Value::Object(map) = &value else {
                return Err(crate::Error::Internal {
                    detail: "window row is not an object".to_string(),
                });
            };
            for key in map.keys() {
                let next = column_index.len();
                column_index.entry(key.clone()).or_insert(next);
            }
            maps.push(value);
        }

        let width = column_index.len();
        let mut arrays: Vec<Vec<nodedb_types::Value>> = maps
            .iter()
            .map(|value| {
                let nodedb_types::Value::Object(map) = value else {
                    unreachable!("every row was validated as an object above");
                };
                let mut cells = vec![nodedb_types::Value::Null; width];
                for (key, position) in &column_index {
                    if let Some(cell) = map.get(key) {
                        cells[*position] = cell.clone();
                    }
                }
                cells
            })
            .collect();

        let new_cols = evaluate_window_functions_value(&mut arrays, &column_index, specs)?;

        for (value, cells) in maps.iter_mut().zip(arrays) {
            if cells.len() != width + new_cols.len() {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "window evaluator appended {} columns but a row carries {} cells",
                        new_cols.len(),
                        cells.len().saturating_sub(width)
                    ),
                });
            }
            let nodedb_types::Value::Object(map) = value else {
                unreachable!("every row was validated as an object above");
            };
            for (offset, name) in new_cols.iter().enumerate() {
                map.insert(name.clone(), cells[width + offset].clone());
            }
        }

        for (slot, value) in rows.iter_mut().zip(maps) {
            *slot = nodedb_types::value_to_msgpack(&value).map_err(|e| crate::Error::Internal {
                detail: format!("window row encode: {e}"),
            })?;
        }
        Ok(())
    }
}

pub use admission_chokepoint::writes_bypassed_admission_gate;
pub use dispatch::Dispatcher;
pub use envelope::{Request, Response, Status};
