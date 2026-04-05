//! ID generation and detection scalar functions.

use super::shared::{bool_id_check, num_arg};

pub(super) fn try_eval(name: &str, args: &[serde_json::Value]) -> Option<serde_json::Value> {
    let v = match name {
        "uuid" | "uuid_v4" | "gen_random_uuid" => {
            serde_json::Value::String(nodedb_types::id_gen::uuid_v4())
        }
        "uuid_v7" => serde_json::Value::String(nodedb_types::id_gen::uuid_v7()),
        "ulid" => serde_json::Value::String(nodedb_types::id_gen::ulid()),
        "cuid2" => serde_json::Value::String(nodedb_types::id_gen::cuid2()),
        "nanoid" => {
            let len = num_arg(args, 0).map(|n| n as usize);
            match len {
                Some(l) => serde_json::Value::String(nodedb_types::id_gen::nanoid_with_length(l)),
                None => serde_json::Value::String(nodedb_types::id_gen::nanoid()),
            }
        }
        "is_uuid" => bool_id_check(args, nodedb_types::id_gen::is_uuid),
        "is_ulid" => bool_id_check(args, nodedb_types::id_gen::is_ulid),
        "is_cuid2" => bool_id_check(args, nodedb_types::id_gen::is_cuid2),
        "is_nanoid" => bool_id_check(args, nodedb_types::id_gen::is_nanoid),
        "id_type" => args
            .first()
            .and_then(|v| v.as_str())
            .map_or(serde_json::Value::String("unknown".into()), |s| {
                serde_json::Value::String(nodedb_types::id_gen::detect_id_type(s).to_string())
            }),
        "uuid_version" => args
            .first()
            .and_then(|v| v.as_str())
            .map_or(serde_json::Value::Number(0.into()), |s| {
                serde_json::Value::Number(nodedb_types::id_gen::uuid_version(s).into())
            }),
        "ulid_timestamp" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::id_gen::ulid_timestamp_ms)
            .map_or(serde_json::Value::Null, |ms| {
                serde_json::Value::Number(serde_json::Number::from(ms as i64))
            }),
        _ => return None,
    };
    Some(v)
}
