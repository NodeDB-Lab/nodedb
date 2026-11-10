// SPDX-License-Identifier: Apache-2.0

//! A `PROPERTIES { key: $n, ... }` object literal whose values are bound
//! parameters.
//!
//! Every scalar binds as a typed parameter by the rules of `document_bind`,
//! so a Decimal, a UUID, an instant and bytes reach the server under the OID
//! of their exact type. The server writes each into the object literal in
//! its typed form, and the property stores the typed value. An array is an
//! object-literal array and a nested object a nested literal, each element
//! bound the same way.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

use super::core::{NodeDbRemote, pg_error_detail};
use super::document_bind::{TypedParam, scalar_param};

/// The object literal of `properties`, keys in name order, pushing each
/// value's parameters onto `params`. `op` names the write in a refusal.
pub(super) fn properties_literal(
    op: &str,
    properties: &HashMap<String, Value>,
    params: &mut Vec<TypedParam>,
) -> NodeDbResult<String> {
    let mut fields: Vec<(&String, &Value)> = properties.iter().collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));
    let mut entries = Vec::with_capacity(fields.len());
    for (key, value) in fields {
        let bound = bind_property(op, key, value, params)?;
        entries.push(format!("{}: {bound}", literal_key(key)));
    }
    Ok(format!("{{ {} }}", entries.join(", ")))
}

/// A key as the object literal reads it: a plain identifier as is, any
/// other text double-quoted.
fn literal_key(key: &str) -> String {
    let plain = key
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if plain {
        key.to_owned()
    } else {
        format!("\"{}\"", key.replace('"', "\"\""))
    }
}

fn bind_property(
    op: &str,
    path: &str,
    value: &Value,
    params: &mut Vec<TypedParam>,
) -> NodeDbResult<String> {
    let refuse = |why: &str| {
        NodeDbError::bad_request(format!(
            "{op}: property '{path}' holds {value:?}, {why}; \
             store it as a document field with NativeClient::document_put"
        ))
    };
    match value {
        Value::Null => return Ok("null".to_owned()),
        Value::Float(f) if !f.is_finite() => {
            return Err(refuse("which an object literal cannot carry"));
        }
        Value::Array(items) => {
            let elements = items
                .iter()
                .enumerate()
                .map(|(index, item)| bind_property(op, &format!("{path}[{index}]"), item, params))
                .collect::<NodeDbResult<Vec<_>>>()?;
            return Ok(format!("[{}]", elements.join(", ")));
        }
        Value::Object(fields) => {
            return properties_literal(op, fields, params);
        }
        _ => {}
    }
    match scalar_param(value).map_err(refuse)? {
        Some(param) => {
            params.push(param);
            Ok(format!("${}", params.len()))
        }
        None => Err(refuse(
            "which an object literal cannot carry as a typed value",
        )),
    }
}

impl NodeDbRemote {
    /// Run `sql` over the extended protocol with `params` bound typed, and
    /// return the command tag's row count.
    pub(super) async fn execute_typed(
        &self,
        sql: &str,
        params: &[TypedParam],
    ) -> NodeDbResult<u64> {
        let types: Vec<_> = params.iter().map(|(_, ty)| ty.clone()).collect();
        let values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|(value, _)| {
                let value: &(dyn tokio_postgres::types::ToSql + Sync) = value.as_ref();
                value
            })
            .collect();
        let failed = |step: &str, e: &tokio_postgres::Error| {
            NodeDbError::storage(format!("pgwire {step} failed: {}", pg_error_detail(e)))
        };
        let client = self.client.lock().await;
        let statement = client
            .prepare_typed(sql, &types)
            .await
            .map_err(|e| failed("prepare", &e))?;
        client
            .execute(&statement, &values)
            .await
            .map_err(|e| failed("execute", &e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::Type;

    #[test]
    fn every_scalar_binds_typed_and_composites_nest() {
        let properties: HashMap<String, Value> = [
            (
                "price".to_string(),
                Value::Decimal("12.50".parse().unwrap()),
            ),
            (
                "uid".to_string(),
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            ("note".to_string(), Value::Null),
            (
                "tags".to_string(),
                Value::Array(vec![Value::String("a".into()), Value::Bytes(vec![1])]),
            ),
            ("odd key".to_string(), Value::Integer(1)),
        ]
        .into();
        let mut params = Vec::new();
        let literal = properties_literal("op", &properties, &mut params).expect("binds");
        assert_eq!(
            literal,
            "{ note: null, \"odd key\": $1, price: $2, tags: [$3, $4], uid: $5 }"
        );
        let types: Vec<Type> = params.iter().map(|(_, ty)| ty.clone()).collect();
        assert_eq!(
            types,
            vec![
                Type::INT8,
                Type::NUMERIC,
                Type::TEXT,
                Type::BYTEA,
                Type::UUID
            ]
        );
    }

    #[test]
    fn a_value_with_no_typed_form_is_refused_naming_the_property() {
        let properties: HashMap<String, Value> = [("w".to_string(), Value::Float(f64::NAN))].into();
        let err = properties_literal("op", &properties, &mut Vec::new()).expect_err("NaN");
        assert!(err.to_string().contains("'w'"), "{err}");
    }
}
