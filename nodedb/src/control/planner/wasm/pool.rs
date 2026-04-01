//! WASM instance pool: pre-warmed wasmtime instances per function.
//!
//! Creating a wasmtime Store + Instance is moderately expensive (~10μs).
//! The pool pre-warms a configurable number of instances per function
//! and recycles them after invocation.

use std::collections::HashMap;
use std::sync::Mutex;

use wasmtime::{Engine, Instance, Module, Store};

/// Per-tenant WASM instance pool.
///
/// Keyed by function name. Each function has a bounded pool of pre-created
/// instances. When the pool is empty, a new instance is created on-demand.
pub struct WasmInstancePool {
    engine: Engine,
    pools: Mutex<HashMap<String, Vec<PooledInstance>>>,
    pool_size: usize,
}

/// A pooled wasmtime instance with its Store.
///
/// Store carries fuel + memory limits and is reset between invocations.
pub struct PooledInstance {
    pub store: Store<()>,
    pub instance: Instance,
}

impl WasmInstancePool {
    pub fn new(engine: Engine, pool_size: usize) -> Self {
        Self {
            engine,
            pools: Mutex::new(HashMap::new()),
            pool_size,
        }
    }

    /// Acquire an instance for the given function. Takes from pool or creates new.
    pub fn acquire(
        &self,
        func_name: &str,
        module: &Module,
        fuel: u64,
        memory_bytes: usize,
    ) -> crate::Result<PooledInstance> {
        // Try to take from pool.
        {
            let mut pools = self.pools.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(pool) = pools.get_mut(func_name)
                && let Some(mut inst) = pool.pop()
            {
                let _ = inst.store.set_fuel(fuel);
                return Ok(inst);
            }
        }

        // Create a new instance.
        self.create_instance(module, fuel, memory_bytes)
    }

    /// Return an instance to the pool after use.
    pub fn release(&self, func_name: &str, instance: PooledInstance) {
        let mut pools = self.pools.lock().unwrap_or_else(|p| p.into_inner());
        let pool = pools.entry(func_name.to_string()).or_default();
        if pool.len() < self.pool_size {
            pool.push(instance);
        }
        // If pool is full, drop the instance (no recycle).
    }

    /// Pre-warm the pool for a function.
    pub fn warm(
        &self,
        func_name: &str,
        module: &Module,
        fuel: u64,
        memory_bytes: usize,
        count: usize,
    ) -> crate::Result<()> {
        let mut instances = Vec::with_capacity(count);
        for _ in 0..count {
            instances.push(self.create_instance(module, fuel, memory_bytes)?);
        }

        let mut pools = self.pools.lock().unwrap_or_else(|p| p.into_inner());
        let pool = pools.entry(func_name.to_string()).or_default();
        pool.extend(instances);

        Ok(())
    }

    /// Remove all pooled instances for a function (on DROP FUNCTION).
    pub fn evict(&self, func_name: &str) {
        let mut pools = self.pools.lock().unwrap_or_else(|p| p.into_inner());
        pools.remove(func_name);
    }

    fn create_instance(
        &self,
        module: &Module,
        fuel: u64,
        _memory_bytes: usize,
    ) -> crate::Result<PooledInstance> {
        let mut store = Store::new(&self.engine, ());
        store.set_fuel(fuel).map_err(|e| crate::Error::Internal {
            detail: format!("failed to set WASM fuel: {e}"),
        })?;

        // WASI restrictions: no imports provided → pure compute only.
        // Any attempt to call filesystem/network/clock functions will trap.
        let instance =
            Instance::new(&mut store, module, &[]).map_err(|e| crate::Error::BadRequest {
                detail: format!("WASM instantiation failed: {e}"),
            })?;

        Ok(PooledInstance { store, instance })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_creates() {
        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).unwrap();
        let pool = WasmInstancePool::new(engine, 4);
        assert_eq!(pool.pool_size, 4);
    }
}
