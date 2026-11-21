// SPDX-License-Identifier: Apache-2.0

//! ID generation and detection scalar functions.

use super::shared::{bool_id_check, num_arg};
use nodedb_types::Value;

pub(super) fn try_eval(name: &str, args: &[Value]) -> Option<Value> {
    let v = match name {
        "uuid" | "uuid_v4" | "gen_random_uuid" => Value::String(nodedb_types::id_gen::uuid_v4()),
        "uuid_v7" => Value::String(nodedb_types::id_gen::uuid_v7()),
        "ulid" => Value::String(nodedb_types::id_gen::ulid()),
        "cuid2" => Value::String(nodedb_types::id_gen::cuid2()),
        "nanoid" => {
            let len = num_arg(args, 0).map(|n| n as usize);
            match len {
                Some(l) => Value::String(nodedb_types::id_gen::nanoid_with_length(l)),
                None => Value::String(nodedb_types::id_gen::nanoid()),
            }
        }
        "is_uuid" => bool_id_check(args, nodedb_types::id_gen::is_uuid),
        "is_ulid" => bool_id_check(args, nodedb_types::id_gen::is_ulid),
        "is_cuid2" => bool_id_check(args, nodedb_types::id_gen::is_cuid2),
        "is_nanoid" => bool_id_check(args, nodedb_types::id_gen::is_nanoid),
        "id_type" => args
            .first()
            .and_then(|v| v.as_str())
            .map_or(Value::Null, |s| {
                Value::String(nodedb_types::id_gen::detect_id_type(s).as_str().to_string())
            }),
        "uuid_version" => args
            .first()
            .and_then(|v| v.as_str())
            .filter(|s| nodedb_types::id_gen::is_uuid(s))
            .map_or(Value::Null, |s| {
                Value::Integer(nodedb_types::id_gen::uuid_version(s) as i64)
            }),
        "ulid_timestamp" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::id_gen::ulid_timestamp_ms)
            .map_or(Value::Null, |ms| Value::Integer(ms as i64)),
        _ => return None,
    };
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn eval(name: &str, arg: Value) -> Value {
        try_eval(name, &[arg]).expect("known function")
    }

    #[test]
    fn id_type_of_null_is_null() {
        assert_eq!(eval("id_type", Value::Null), Value::Null);
    }

    #[test]
    fn id_type_of_text_still_answers() {
        assert_eq!(
            eval("id_type", Value::String(UUID.into())),
            Value::String("uuid".into())
        );
        assert_eq!(
            eval("id_type", Value::String(String::new())),
            Value::String("custom".into())
        );
    }

    #[test]
    fn uuid_version_of_null_or_non_uuid_is_null() {
        assert_eq!(eval("uuid_version", Value::Null), Value::Null);
        assert_eq!(
            eval("uuid_version", Value::String("not-a-uuid".into())),
            Value::Null
        );
    }

    #[test]
    fn uuid_version_of_a_uuid_answers_its_version() {
        assert_eq!(
            eval("uuid_version", Value::String(UUID.into())),
            Value::Integer(4)
        );
    }
}
