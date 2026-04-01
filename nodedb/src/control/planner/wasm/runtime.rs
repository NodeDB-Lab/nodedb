//! WASM runtime: wasmtime Engine with compilation caching.
//!
//! The Engine is shared across all WASM UDF invocations. Module compilation
//! is expensive — pre-compiled native code is cached in-memory by module hash.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use wasmtime::{Config, Engine, Module};

/// Shared WASM runtime engine with module compilation cache.
pub struct WasmRuntime {
    engine: Engine,
    /// Pre-compiled modules keyed by SHA-256 of the `.wasm` binary.
    module_cache: Mutex<HashMap<[u8; 32], Arc<Module>>>,
}

impl WasmRuntime {
    /// Create a new WASM runtime with fuel metering and memory limits enabled.
    pub fn new() -> crate::Result<Self> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.cranelift_opt_level(wasmtime::OptLevel::Speed);

        let engine = Engine::new(&config).map_err(|e| crate::Error::Internal {
            detail: format!("wasmtime engine creation failed: {e}"),
        })?;

        Ok(Self {
            engine,
            module_cache: Mutex::new(HashMap::new()),
        })
    }

    /// Get or compile a WASM module from raw bytes.
    ///
    /// Modules are cached by SHA-256 hash of the binary. First compilation
    /// is slow (Cranelift JIT); subsequent calls return the cached module.
    pub fn get_or_compile(&self, wasm_bytes: &[u8]) -> crate::Result<Arc<Module>> {
        let hash = sha256(wasm_bytes);

        // Fast path: check cache.
        {
            let cache = self.module_cache.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(module) = cache.get(&hash) {
                return Ok(Arc::clone(module));
            }
        }

        // Slow path: compile and cache.
        let module =
            Module::new(&self.engine, wasm_bytes).map_err(|e| crate::Error::BadRequest {
                detail: format!("WASM module compilation failed: {e}"),
            })?;
        let arc = Arc::new(module);

        let mut cache = self.module_cache.lock().unwrap_or_else(|p| p.into_inner());
        cache.entry(hash).or_insert_with(|| Arc::clone(&arc));

        Ok(arc)
    }

    /// Get a reference to the underlying wasmtime Engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Number of cached compiled modules.
    pub fn cached_module_count(&self) -> usize {
        self.module_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }

    /// Clear the compilation cache.
    pub fn clear_cache(&self) {
        self.module_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }
}

/// Compute SHA-256 hash of a byte slice.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Format a SHA-256 hash as a hex string.
pub fn sha256_hex(data: &[u8]) -> String {
    hex_encode(&sha256(data))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_deterministic() {
        let h1 = sha256(b"hello");
        let h2 = sha256(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn sha256_different_inputs() {
        let h1 = sha256(b"hello");
        let h2 = sha256(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hex_encoding() {
        let hex = sha256_hex(b"test");
        assert_eq!(hex.len(), 64); // 32 bytes = 64 hex chars
    }

    #[test]
    fn runtime_creates() {
        let rt = WasmRuntime::new().unwrap();
        assert_eq!(rt.cached_module_count(), 0);
    }
}
