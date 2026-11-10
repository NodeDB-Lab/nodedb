// SPDX-License-Identifier: BUSL-1.1

//! Value-generating recursive CTE handler.
//!
//! Evaluates `WITH RECURSIVE name(cols) AS (anchor UNION [ALL] step WHERE cond)`
//! entirely in memory — no collection scan, no storage I/O.
//!
//! Algorithm:
//! 1. Evaluate `init_exprs` against an empty context → row 0.
//! 2. Loop: evaluate `condition` (if present) against the working row; a
//!    `false` result ends the recursion. Evaluate `step_exprs` against the
//!    working row → new row. Stop on fixed point (UNION dedup) or when
//!    `max_depth` is exceeded (a typed error).
//! 3. Serialise all rows as a msgpack array and return.
//!
//! An expression that fails to evaluate — an undefined column, an unsupported
//! shape, an overflow — aborts the statement with a typed error. It never ends
//! the loop. A truncated result set is indistinguishable from a correct one at
//! the client, so a silent stop reports wrong data as success.

use std::collections::{HashMap, HashSet};

use super::eval::{Ctx, eval_condition, eval_row_exprs};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_recursive_value`].
pub(in crate::data::executor) struct RecursiveValueParams<'a> {
    pub cte_name: &'a str,
    pub columns: &'a [String],
    pub init_exprs: &'a [String],
    pub step_exprs: &'a [String],
    pub condition: Option<&'a str>,
    pub max_depth: usize,
    pub distinct: bool,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_recursive_value(
        &mut self,
        task: &ExecutionTask,
        params: RecursiveValueParams<'_>,
    ) -> Response {
        let RecursiveValueParams {
            cte_name,
            columns,
            init_exprs,
            step_exprs,
            condition,
            max_depth,
            distinct,
        } = params;

        // ── Anchor row ────────────────────────────────────────────────────────
        let init_values = match eval_row_exprs(init_exprs, &Ctx::new()) {
            Ok(v) => v,
            Err(e) => return self.response_error(task, anchor_error(cte_name, e)),
        };

        let mut results: Vec<nodedb_types::Value> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        let init_obj = build_object(columns, init_values);
        if should_keep_obj(&init_obj, distinct, &mut seen) {
            results.push(init_obj.clone());
        }

        // ── Iterative step ────────────────────────────────────────────────────
        let mut current = obj_to_ctx(&init_obj);

        for depth in 0..max_depth {
            // Evaluate the WHERE condition against the CURRENT row before
            // computing the next step — this matches SQL semantics where
            // `WHERE n < 5` filters which rows from the working set participate
            // in the recursive step.
            if let Some(cond_sql) = condition {
                match eval_condition(cond_sql, &current) {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(e) => return self.response_error(task, step_error(cte_name, e)),
                }
            }

            let step_values = match eval_row_exprs(step_exprs, &current) {
                Ok(v) => v,
                Err(e) => return self.response_error(task, step_error(cte_name, e)),
            };
            let new_obj = build_object(columns, step_values);

            if distinct {
                let key = obj_dedup_key(&new_obj);
                if !seen.insert(key) {
                    break; // Duplicate → fixed point.
                }
            }

            results.push(new_obj.clone());
            current = obj_to_ctx(&new_obj);

            // Depth limit: exceeded after recording depth+1 rows beyond anchor.
            if depth + 1 == max_depth {
                return self.response_error(
                    task,
                    ErrorCode::RecursionDepthExceeded {
                        cte_name: cte_name.to_owned(),
                        max_depth,
                    },
                );
            }
        }

        // ── Serialise to msgpack array ─────────────────────────────────────────
        // The rows feed the outer query's operators, so each value keeps its
        // type. The public boundary renders them.
        match crate::data::executor::response_codec::encode_native_rows(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("recursive value serialisation failed: {e}"),
                },
            ),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Name the CTE and the arm in an evaluation failure, so a client can tell
/// which half of the `UNION` it must fix. An undefined column keeps its own
/// code — `42703` is what the same reference reports anywhere else — and only
/// the free-text codes gain the prefix.
fn locate_error(cte_name: &str, arm: &str, err: super::eval::EvalError) -> ErrorCode {
    match ErrorCode::from(err) {
        ErrorCode::Unsupported { detail } => ErrorCode::Unsupported {
            detail: format!("WITH RECURSIVE '{cte_name}' ({arm}): {detail}"),
        },
        ErrorCode::DataException { detail } => ErrorCode::DataException {
            detail: format!("WITH RECURSIVE '{cte_name}' ({arm}): {detail}"),
        },
        ErrorCode::BadRequest { detail } => ErrorCode::BadRequest {
            detail: format!("WITH RECURSIVE '{cte_name}' ({arm}): {detail}"),
        },
        other => other,
    }
}

fn anchor_error(cte_name: &str, err: super::eval::EvalError) -> ErrorCode {
    locate_error(cte_name, "anchor", err)
}

fn step_error(cte_name: &str, err: super::eval::EvalError) -> ErrorCode {
    locate_error(cte_name, "recursive step", err)
}

/// Build a `Value::Object` from ordered column names and values.
fn build_object(columns: &[String], values: Vec<nodedb_types::Value>) -> nodedb_types::Value {
    let map: HashMap<String, nodedb_types::Value> = columns
        .iter()
        .zip(values)
        .map(|(k, v)| (k.clone(), v))
        .collect();
    nodedb_types::Value::Object(map)
}

/// Extract a column→value map from a `Value::Object` for use as an eval context.
fn obj_to_ctx(obj: &nodedb_types::Value) -> Ctx {
    match obj {
        nodedb_types::Value::Object(m) => m.clone(),
        _ => Ctx::new(),
    }
}

/// Produce a stable deduplication key for a `Value::Object`.
fn obj_dedup_key(obj: &nodedb_types::Value) -> String {
    match obj {
        nodedb_types::Value::Object(m) => {
            let mut pairs: Vec<(&String, &nodedb_types::Value)> = m.iter().collect();
            pairs.sort_by_key(|(k, _)| k.as_str());
            pairs
                .iter()
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect::<Vec<_>>()
                .join(",")
        }
        other => format!("{other:?}"),
    }
}

/// Returns `true` if the row should be added to results.
fn should_keep_obj(obj: &nodedb_types::Value, distinct: bool, seen: &mut HashSet<String>) -> bool {
    if !distinct {
        return true;
    }
    seen.insert(obj_dedup_key(obj))
}
