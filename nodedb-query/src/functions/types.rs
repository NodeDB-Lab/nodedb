//! Type-checking scalar functions.

pub(super) fn try_eval(name: &str, args: &[serde_json::Value]) -> Option<serde_json::Value> {
    let v = match name {
        "typeof" | "type_of" => {
            let type_name = match args.first() {
                Some(serde_json::Value::Null) => "null",
                Some(serde_json::Value::Bool(_)) => "bool",
                Some(serde_json::Value::Number(n)) => {
                    if n.is_i64() {
                        "int"
                    } else {
                        "float"
                    }
                }
                Some(serde_json::Value::String(_)) => "string",
                Some(serde_json::Value::Array(_)) => "array",
                Some(serde_json::Value::Object(_)) => "object",
                None => "null",
            };
            serde_json::Value::String(type_name.to_string())
        }
        _ => return None,
    };
    Some(v)
}
