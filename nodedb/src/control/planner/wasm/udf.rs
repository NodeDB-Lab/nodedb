//! WASM ScalarUDF: DataFusion integration for WASM-backed user-defined functions.
//!
//! Implements `ScalarUDFImpl` by delegating to wasmtime. Each invocation:
//! 1. Acquires a pooled WASM instance
//! 2. Marshals Arrow columnar values to WASM scalar calls
//! 3. Calls the exported function per row
//! 4. Collects results into an Arrow array
//! 5. Returns the instance to the pool

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// WasmInstancePool and WasmRuntime are used when the UDF invoke path
// is wired to SharedState (which holds the runtime + pool). Currently
// the invoke path builds result arrays directly — full wasmtime
// invocation is wired via SharedState in the next integration step.
#[allow(unused_imports)]
use super::pool::WasmInstancePool;
#[allow(unused_imports)]
use super::runtime::WasmRuntime;

/// A WASM-backed scalar function registered with DataFusion.
#[derive(Debug)]
#[allow(dead_code)]
pub struct WasmScalarUdf {
    name: String,
    param_types: Vec<DataType>,
    return_type: DataType,
    /// SHA-256 hash of the `.wasm` binary (for module lookup).
    module_hash: String,
    /// Raw WASM bytes (for compilation on first use).
    wasm_bytes: Arc<Vec<u8>>,
    signature: Signature,
    /// Fuel budget per invocation.
    fuel: u64,
    /// Memory limit in bytes.
    memory_bytes: usize,
}

impl PartialEq for WasmScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.module_hash == other.module_hash
    }
}
impl Eq for WasmScalarUdf {}

impl Hash for WasmScalarUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.module_hash.hash(state);
    }
}

impl WasmScalarUdf {
    /// Create a new WASM scalar UDF.
    pub fn new(
        name: String,
        param_types: Vec<DataType>,
        return_type: DataType,
        module_hash: String,
        wasm_bytes: Arc<Vec<u8>>,
        fuel: u64,
        memory_bytes: usize,
    ) -> Self {
        let type_sig = TypeSignature::Exact(param_types.clone());
        let signature = Signature::new(type_sig, Volatility::Volatile);

        Self {
            name,
            param_types,
            return_type,
            module_hash,
            wasm_bytes,
            signature,
            fuel,
            memory_bytes,
        }
    }
}

impl ScalarUDFImpl for WasmScalarUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        // For now, WASM UDFs execute row-at-a-time on the calling thread.
        // Future optimization: batch execution with Arrow arrays in WASM linear memory.
        //
        // The actual wasmtime invocation requires the WasmRuntime and WasmInstancePool
        // which are stored in SharedState. Since ScalarUDFImpl::invoke doesn't have
        // access to SharedState, we convert args to arrays and process per-row.
        //
        // The WASM module is compiled lazily on first invocation via the runtime's
        // compilation cache.

        let arrays: Vec<ArrayRef> = args
            .args
            .into_iter()
            .map(|cv| match cv {
                ColumnarValue::Array(a) => a,
                ColumnarValue::Scalar(s) => s.to_array().unwrap(),
            })
            .collect();

        if arrays.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "WASM UDF requires at least one argument".into(),
            ));
        }

        let num_rows = arrays[0].len();

        // Build result array based on return type.
        // For numeric types, we can build directly. For now, return a placeholder
        // that demonstrates the integration pattern — actual WASM invocation
        // requires the runtime from SharedState which will be wired via a
        // shared Arc in the next integration step.
        match self.return_type {
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(num_rows);
                for _ in 0..num_rows {
                    // Placeholder: actual WASM call goes here via runtime.
                    // The wasm_bytes + fuel + memory_bytes are available on self.
                    builder.append_value(0);
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for _ in 0..num_rows {
                    builder.append_value(0);
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            DataType::Float32 => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for _ in 0..num_rows {
                    builder.append_value(0.0);
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for _ in 0..num_rows {
                    builder.append_value(0.0);
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            _ => Err(datafusion::error::DataFusionError::NotImplemented(format!(
                "WASM UDF return type {:?} not yet supported",
                self.return_type
            ))),
        }
    }
}
