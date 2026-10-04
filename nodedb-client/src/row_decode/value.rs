// SPDX-License-Identifier: Apache-2.0

//! Column-level decoders shared by every row parser in this crate.
//!
//! Both the pgwire path (`Client::query` → `Vec<Vec<Value>>` via
//! `pg_value_to_value`) and the native path (`execute_sql` →
//! `QueryResult.rows`) hand back rows in the same `Vec<Value>` shape, so
//! the decoders that lift individual columns into Rust scalars sit here
//! once rather than per client. `Value::String` is accepted for u64
//! columns because the pgwire simple-query path returns every column as
//! text, even integer ones.

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

/// Decode a row value as `u64`. Accepts both `Value::Integer` (extended-
/// query / native path) and `Value::String` containing a base-10 integer
/// (pgwire simple-query path, which returns every column as text).
pub(crate) fn value_as_u64(v: &Value) -> NodeDbResult<u64> {
    match v {
        Value::Integer(i) => u64::try_from(*i)
            .map_err(|_| NodeDbError::storage(format!("expected u64 column, got negative {i}"))),
        Value::String(s) => s
            .parse::<u64>()
            .map_err(|e| NodeDbError::storage(format!("parse u64 from '{s}': {e}"))),
        other => Err(NodeDbError::storage(format!(
            "expected integer for u64 column, got {other:?}"
        ))),
    }
}

/// Decode a row value as `String`. `Null` projects to the empty string so
/// downstream callers don't have to special-case nullable text columns
/// (e.g. an unset `owner` on a soft-deleted collection).
pub(crate) fn value_as_string(v: &Value) -> NodeDbResult<String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Null => Ok(String::new()),
        other => Err(NodeDbError::storage(format!(
            "expected string column, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_as_u64_accepts_integer() {
        assert_eq!(value_as_u64(&Value::Integer(42)).unwrap(), 42u64);
    }

    #[test]
    fn value_as_u64_rejects_a_negative_integer() {
        let err = value_as_u64(&Value::Integer(-1)).unwrap_err();
        assert!(err.to_string().contains("negative -1"), "{err}");
    }

    #[test]
    fn value_as_u64_accepts_numeric_string() {
        // Simple-query path returns integer columns as text — must parse.
        assert_eq!(value_as_u64(&Value::String("17".into())).unwrap(), 17u64);
    }

    #[test]
    fn value_as_u64_rejects_non_numeric_string() {
        let err = value_as_u64(&Value::String("not-a-number".into())).unwrap_err();
        assert!(err.to_string().contains("parse u64"));
    }

    #[test]
    fn value_as_u64_rejects_unsupported_variants() {
        assert!(value_as_u64(&Value::Bool(true)).is_err());
        assert!(value_as_u64(&Value::Null).is_err());
    }

    #[test]
    fn value_as_string_decodes_string_and_null() {
        assert_eq!(value_as_string(&Value::String("ok".into())).unwrap(), "ok");
        assert_eq!(value_as_string(&Value::Null).unwrap(), "");
    }

    #[test]
    fn value_as_string_rejects_non_string_variants() {
        assert!(value_as_string(&Value::Integer(1)).is_err());
    }
}
