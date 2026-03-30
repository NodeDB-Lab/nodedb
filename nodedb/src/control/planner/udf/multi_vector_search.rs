//! `multi_vector_search(query_vector)` — stub UDF for DataFusion type checking.
//!
//! Enables SQL: `SELECT * FROM collection ORDER BY multi_vector_search(ARRAY[...]) LIMIT k`.
//! The PlanConverter recognizes this function name in Sort expressions and
//! rewrites the plan to `PhysicalPlan::VectorOp::MultiSearch`, which executes
//! multi-field RRF fusion on the Data Plane.

use std::any::Any;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MultiVectorSearch {
    signature: Signature,
}

impl MultiVectorSearch {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(1)], Volatility::Volatile),
        }
    }
}

impl Default for MultiVectorSearch {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for MultiVectorSearch {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "multi_vector_search"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DfResult<ColumnarValue> {
        let array = Float64Array::from(vec![0.0f64; args.number_rows]);
        Ok(ColumnarValue::Array(std::sync::Arc::new(array)))
    }
}
