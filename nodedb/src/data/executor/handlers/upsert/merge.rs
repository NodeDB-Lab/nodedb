// SPDX-License-Identifier: BUSL-1.1

//! The two value-merge rules the upsert branches share with the staged
//! in-transaction path, so an `UPSERT INTO` merges identically whether it runs
//! through the autocommit handler or the transaction overlay.

/// Apply `ON CONFLICT DO UPDATE SET` assignments against the existing row.
///
/// Each assignment's RHS is evaluated via `SqlExpr::eval` — identical to
/// the UPDATE handler's path — so arithmetic (`n = n + 1`), functions
/// (`name = UPPER(name)`), `CASE`, and concatenation all work. Literal
/// assignments bypass the evaluator and decode their msgpack directly.
///
/// Both rows must be objects: every caller decodes them from a stored or
/// planned row body, so a non-object here is a body that does not hold what
/// it claims, and is refused rather than merged onto a blank row.
pub(in crate::data::executor) fn apply_on_conflict_updates(
    existing: nodedb_types::Value,
    excluded: &nodedb_types::Value,
    updates: &[(String, nodedb_physical::physical_plan::UpdateValue)],
) -> crate::Result<nodedb_types::Value> {
    let existing_kind = existing.type_name();
    let nodedb_types::Value::Object(mut obj) = existing else {
        return Err(non_object_row("existing", existing_kind));
    };
    if !matches!(excluded, nodedb_types::Value::Object(_)) {
        return Err(non_object_row("EXCLUDED", excluded.type_name()));
    }
    // Snapshot the row before any assignment applies, so all assignments
    // see the pre-update state — matches PostgreSQL semantics. `excluded`
    // is the row proposed for INSERT that triggered the conflict — it
    // resolves `EXCLUDED.col` references inside the RHS expressions.
    let snapshot = nodedb_types::Value::Object(obj.clone());
    for (field, update_val) in updates {
        let new_val: nodedb_types::Value = match update_val {
            nodedb_physical::physical_plan::UpdateValue::Literal(bytes) => {
                nodedb_types::value_from_msgpack(bytes).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("ON CONFLICT DO UPDATE SET {field}: literal: {e}"),
                    }
                })?
            }
            // `ON CONFLICT DO UPDATE SET` is write-path-shaped: a
            // division/modulo-by-zero fails the statement instead of
            // silently writing NULL.
            nodedb_physical::physical_plan::UpdateValue::Expr(expr) => {
                expr.eval_with_excluded(&snapshot, excluded)?
            }
        };
        obj.insert(field.clone(), new_val);
    }
    Ok(nodedb_types::Value::Object(obj))
}

/// The row body decoded to something other than an object.
fn non_object_row(side: &str, kind: &'static str) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("ON CONFLICT DO UPDATE: {side} row is {kind}, not an object"),
    }
}

/// Merge two `nodedb_types::Value` objects: overlay `new` fields onto `existing`.
///
/// Shared with the in-transaction staging path (`stage_write/stage_upsert.rs`)
/// so a staged `UPSERT INTO` with no `ON CONFLICT DO UPDATE` clause merges
/// identically to the autocommit handler above.
pub(in crate::data::executor) fn merge_values(
    existing: nodedb_types::Value,
    new: nodedb_types::Value,
) -> nodedb_types::Value {
    match (existing, new) {
        (nodedb_types::Value::Object(mut existing_map), nodedb_types::Value::Object(new_map)) => {
            for (k, v) in new_map {
                existing_map.insert(k, v);
            }
            nodedb_types::Value::Object(existing_map)
        }
        // If shapes don't match, new value wins entirely.
        (_, new) => new,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_physical::physical_plan::UpdateValue;
    use nodedb_types::Value;

    use super::apply_on_conflict_updates;

    fn obj(fields: &[(&str, i64)]) -> Value {
        Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.to_string(), Value::Integer(*v)))
                .collect(),
        )
    }

    fn literal(v: Value) -> UpdateValue {
        UpdateValue::Literal(nodedb_types::value_to_msgpack(&v).expect("encode literal"))
    }

    #[test]
    fn literal_assignment_overlays_the_existing_row() {
        let updates = vec![("n".to_string(), literal(Value::Integer(2)))];
        let merged = apply_on_conflict_updates(obj(&[("n", 1), ("m", 9)]), &obj(&[]), &updates)
            .expect("merge");
        assert_eq!(merged, obj(&[("n", 2), ("m", 9)]));
    }

    #[test]
    fn non_object_existing_row_is_refused_not_merged_onto_a_blank_row() {
        let updates = vec![("n".to_string(), literal(Value::Integer(2)))];
        let err = apply_on_conflict_updates(Value::Integer(118), &obj(&[]), &updates)
            .expect_err("a scalar existing row must be refused");
        assert!(
            matches!(err, crate::Error::Serialization { .. }),
            "got {err:?}"
        );
        assert!(err.to_string().contains("existing row is int"), "{err}");
    }

    #[test]
    fn non_object_excluded_row_is_refused() {
        let updates = vec![("n".to_string(), literal(Value::Integer(2)))];
        let err = apply_on_conflict_updates(obj(&[]), &Value::String("x".into()), &updates)
            .expect_err("a scalar EXCLUDED row must be refused");
        assert!(err.to_string().contains("EXCLUDED row is string"), "{err}");
    }

    #[test]
    fn undecodable_literal_fails_the_statement() {
        // fixmap header claiming one entry, then nothing.
        let updates = vec![("n".to_string(), UpdateValue::Literal(vec![0x81]))];
        let err = apply_on_conflict_updates(obj(&[("n", 1)]), &obj(&[]), &updates)
            .expect_err("a truncated literal must not be skipped");
        assert!(
            matches!(err, crate::Error::Serialization { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn assignments_see_the_pre_update_snapshot() {
        let mut fields: HashMap<String, Value> = HashMap::new();
        fields.insert("a".into(), Value::Integer(1));
        let updates = vec![
            ("a".to_string(), literal(Value::Integer(5))),
            ("b".to_string(), literal(Value::Integer(6))),
        ];
        let merged =
            apply_on_conflict_updates(Value::Object(fields), &obj(&[]), &updates).expect("merge");
        assert_eq!(merged, obj(&[("a", 5), ("b", 6)]));
    }
}
