pub mod fuel;
pub mod pool;
pub mod runtime;
pub mod store;
pub mod types;
pub mod udf;

/// Configuration for the WASM UDF runtime.
pub struct WasmConfig {
    /// Default fuel budget per invocation (default 1_000_000).
    pub default_fuel: u64,
    /// Default linear memory limit in bytes (default 16 MB).
    pub default_memory_bytes: usize,
    /// Maximum `.wasm` binary size in bytes (default 10 MB).
    pub max_binary_size: usize,
    /// Number of pre-warmed instances per function (default 4).
    pub pool_size: usize,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            default_fuel: 1_000_000,
            default_memory_bytes: 16 * 1024 * 1024,
            max_binary_size: 10 * 1024 * 1024,
            pool_size: 4,
        }
    }
}
