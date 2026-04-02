//! `next_preview('sequence_name')` — peek at next value without consuming.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, exec_err};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::control::sequence::SequenceRegistry;

/// `next_preview(name TEXT) → TEXT`
///
/// Peeks at the next sequence value without consuming it. Returns the
/// value that the next `nextval()` call would return.
pub struct NextPreview {
    signature: Signature,
    registry: Arc<SequenceRegistry>,
    tenant_id: u32,
    tenant_code: String,
    session_vars: Arc<std::sync::RwLock<std::collections::HashMap<String, String>>>,
}

impl NextPreview {
    pub fn new(registry: Arc<SequenceRegistry>, tenant_id: u32) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
            registry,
            tenant_id,
            tenant_code: String::new(),
            session_vars: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }
}

impl std::fmt::Debug for NextPreview {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextPreview")
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

impl PartialEq for NextPreview {
    fn eq(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
    }
}

impl Eq for NextPreview {}

impl std::hash::Hash for NextPreview {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tenant_id.hash(state);
    }
}

impl ScalarUDFImpl for NextPreview {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "next_preview"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DfResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("next_preview requires exactly 1 argument");
        }

        match &args[0] {
            ColumnarValue::Scalar(scalar) => {
                let name = scalar
                    .to_string()
                    .trim_matches('\'')
                    .trim_matches('"')
                    .to_lowercase();

                let vars = self.session_vars.read().unwrap_or_else(|p| p.into_inner());
                let result = self
                    .registry
                    .next_preview(self.tenant_id, &name, &self.tenant_code, &vars)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                let string_val = match result {
                    crate::control::sequence::registry::SequenceValue::Int(v) => v.to_string(),
                    crate::control::sequence::registry::SequenceValue::Formatted(s) => s,
                };

                Ok(ColumnarValue::Scalar(
                    datafusion::common::ScalarValue::Utf8(Some(string_val)),
                ))
            }
            ColumnarValue::Array(arr) => {
                let names = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "next_preview argument must be TEXT".to_string(),
                    )
                })?;

                let vars = self.session_vars.read().unwrap_or_else(|p| p.into_inner());
                let mut values: Vec<Option<String>> = Vec::with_capacity(names.len());
                for i in 0..names.len() {
                    if names.is_null(i) {
                        values.push(None);
                    } else {
                        let name = names.value(i).to_lowercase();
                        let result = self
                            .registry
                            .next_preview(self.tenant_id, &name, &self.tenant_code, &vars)
                            .map_err(|e| {
                                datafusion::error::DataFusionError::Execution(e.to_string())
                            })?;
                        let s = match result {
                            crate::control::sequence::registry::SequenceValue::Int(v) => {
                                v.to_string()
                            }
                            crate::control::sequence::registry::SequenceValue::Formatted(s) => s,
                        };
                        values.push(Some(s));
                    }
                }

                let arr: ArrayRef = Arc::new(StringArray::from(values));
                Ok(ColumnarValue::Array(arr))
            }
        }
    }
}
