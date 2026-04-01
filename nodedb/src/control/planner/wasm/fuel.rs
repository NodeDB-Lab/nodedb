//! Fuel and memory limit enforcement for WASM UDFs.
//!
//! Fuel metering: each WASM instruction consumes fuel. When fuel hits zero,
//! execution traps with DEADLINE_EXCEEDED. This prevents infinite loops
//! and runaway computation in user-submitted WASM code.

/// Check if a wasmtime error is a fuel exhaustion trap.
pub fn is_fuel_exhausted(err: &wasmtime::Error) -> bool {
    let msg = err.to_string();
    msg.contains("fuel") || msg.contains("all fuel consumed")
}

/// Convert a wasmtime trap to a NodeDbError.
pub fn trap_to_error(err: wasmtime::Error, func_name: &str) -> crate::Error {
    if is_fuel_exhausted(&err) {
        crate::Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(0),
        }
    } else {
        crate::Error::BadRequest {
            detail: format!("WASM UDF '{func_name}' execution failed: {err}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuel_exhaustion_detection() {
        // Simulate a fuel error message pattern.
        let err = wasmtime::Error::msg("all fuel consumed by WebAssembly");
        assert!(is_fuel_exhausted(&err));
    }

    #[test]
    fn non_fuel_error() {
        let err = wasmtime::Error::msg("memory access out of bounds");
        assert!(!is_fuel_exhausted(&err));
    }
}
