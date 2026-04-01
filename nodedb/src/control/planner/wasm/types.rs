//! SQL ↔ WASM type mapping.

use wasmtime::ValType;

/// Map a SQL type string to a WASM value type.
pub fn sql_type_to_wasm(sql_type: &str) -> Option<ValType> {
    match sql_type.to_uppercase().as_str() {
        "INT" | "INTEGER" | "INT4" => Some(ValType::I32),
        "BIGINT" | "INT8" => Some(ValType::I64),
        "FLOAT" | "REAL" | "FLOAT4" => Some(ValType::F32),
        "DOUBLE" | "FLOAT8" | "DOUBLE PRECISION" => Some(ValType::F64),
        "BOOLEAN" | "BOOL" => Some(ValType::I32),
        "TEXT" | "VARCHAR" | "STRING" => Some(ValType::I32),
        "BYTEA" | "BLOB" => Some(ValType::I32),
        _ => None,
    }
}

/// Map a WASM value type back to a SQL type string.
pub fn wasm_type_to_sql(wasm_type: &ValType) -> &'static str {
    match wasm_type {
        ValType::I32 => "INT",
        ValType::I64 => "BIGINT",
        ValType::F32 => "FLOAT",
        ValType::F64 => "DOUBLE",
        _ => "TEXT",
    }
}

/// Compare two ValType values by their debug representation.
/// wasmtime::ValType doesn't implement PartialEq in all versions.
fn valtype_eq(a: &ValType, b: &ValType) -> bool {
    format!("{a:?}") == format!("{b:?}")
}

/// Validate that a WASM module's exported function signature matches
/// the declared SQL parameter types and return type.
pub fn validate_signature(
    module: &wasmtime::Module,
    export_name: &str,
    param_types: &[&str],
    return_type: &str,
) -> crate::Result<()> {
    let func_type = module
        .exports()
        .find(|e| e.name() == export_name)
        .and_then(|e| e.ty().func().cloned())
        .ok_or_else(|| crate::Error::BadRequest {
            detail: format!("WASM module does not export function '{export_name}'"),
        })?;

    let wasm_params: Vec<_> = func_type.params().collect();
    if wasm_params.len() != param_types.len() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "WASM function '{export_name}' has {} parameters, expected {}",
                wasm_params.len(),
                param_types.len()
            ),
        });
    }

    for (i, (wasm_ty, sql_ty)) in wasm_params.iter().zip(param_types.iter()).enumerate() {
        let expected = sql_type_to_wasm(sql_ty).ok_or_else(|| crate::Error::BadRequest {
            detail: format!("unsupported SQL type '{sql_ty}' for WASM parameter {i}"),
        })?;
        if !valtype_eq(wasm_ty, &expected) {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "WASM parameter {i}: expected {expected:?} for SQL type '{sql_ty}', got {wasm_ty:?}"
                ),
            });
        }
    }

    let wasm_returns: Vec<_> = func_type.results().collect();
    if wasm_returns.len() != 1 {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "WASM function '{export_name}' must return exactly 1 value, returns {}",
                wasm_returns.len()
            ),
        });
    }
    let expected_ret = sql_type_to_wasm(return_type).ok_or_else(|| crate::Error::BadRequest {
        detail: format!("unsupported SQL return type '{return_type}'"),
    })?;
    if !valtype_eq(&wasm_returns[0], &expected_ret) {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "WASM return type mismatch: expected {expected_ret:?}, got {:?}",
                wasm_returns[0]
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_to_wasm_mapping() {
        assert!(sql_type_to_wasm("INT").is_some());
        assert!(sql_type_to_wasm("BIGINT").is_some());
        assert!(sql_type_to_wasm("FLOAT").is_some());
        assert!(sql_type_to_wasm("DOUBLE").is_some());
        assert!(sql_type_to_wasm("BOOLEAN").is_some());
        assert!(sql_type_to_wasm("TEXT").is_some());
        assert!(sql_type_to_wasm("UNKNOWN_TYPE").is_none());
    }

    #[test]
    fn wasm_to_sql_mapping() {
        assert_eq!(wasm_type_to_sql(&ValType::I32), "INT");
        assert_eq!(wasm_type_to_sql(&ValType::I64), "BIGINT");
        assert_eq!(wasm_type_to_sql(&ValType::F32), "FLOAT");
        assert_eq!(wasm_type_to_sql(&ValType::F64), "DOUBLE");
    }

    #[test]
    fn valtype_eq_works() {
        assert!(valtype_eq(&ValType::I32, &ValType::I32));
        assert!(!valtype_eq(&ValType::I32, &ValType::I64));
    }
}
