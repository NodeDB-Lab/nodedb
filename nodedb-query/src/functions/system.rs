// SPDX-License-Identifier: Apache-2.0

//! System/metadata scalar functions (version, ...).

use nodedb_types::Value;

/// Evaluate a system scalar function; returns `None` if `name` is not handled.
pub(crate) fn try_eval(name: &str, args: &[Value]) -> Option<Value> {
    match name {
        "version" => Some(Value::String(nodedb_types::pg_compat::version_string())),
        "format_type" => Some(format_type(args)),
        "pg_get_expr" => Some(args.first().cloned().unwrap_or(Value::Null)),
        // NodeDB does not persist per-column comments yet.
        "col_description" => Some(Value::Null),
        _ => None,
    }
}

fn format_type(args: &[Value]) -> Value {
    let Some(Value::Integer(oid)) = args.first() else {
        return Value::Null;
    };
    let name = match *oid {
        16 => "boolean",
        17 => "bytea",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        25 => "text",
        26 => "oid",
        114 => "json",
        700 => "real",
        701 => "double precision",
        1042 => "character",
        1043 => "character varying",
        1082 => "date",
        1083 => "time without time zone",
        1114 => "timestamp without time zone",
        1184 => "timestamp with time zone",
        1186 => "interval",
        1700 => "numeric",
        2950 => "uuid",
        3802 => "jsonb",
        _ => return Value::String("text".into()),
    };
    Value::String(name.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_catalog_metadata_functions() {
        assert_eq!(
            try_eval("format_type", &[Value::Integer(23), Value::Integer(-1)]),
            Some(Value::String("integer".into()))
        );
        assert_eq!(
            try_eval(
                "pg_get_expr",
                &[Value::String("'untitled'".into()), Value::Integer(42)]
            ),
            Some(Value::String("'untitled'".into()))
        );
        assert_eq!(
            try_eval("col_description", &[Value::Integer(42), Value::Integer(1)]),
            Some(Value::Null)
        );
    }
}
