//! `sparse_score(column, query_sparse_vector)` — stub UDF for DataFusion.
//!
//! The PlanConverter recognizes this function name in Sort/Filter expressions
//! and rewrites the plan to `PhysicalPlan::Vector(VectorOp::SparseSearch)`,
//! which executes dot-product scoring on the Data Plane's inverted index.

use std::any::Any;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparseScore {
    signature: Signature,
}

impl SparseScore {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // sparse_score(column_name, query_sparse_vector)
                    TypeSignature::Any(2),
                ],
                Volatility::Volatile,
            ),
        }
    }
}

impl Default for SparseScore {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparseScore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sparse_score"
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
        // Stub: real execution is rewritten by PlanConverter to VectorOp::SparseSearch.
        let array = Float64Array::from(vec![0.0f64; args.number_rows]);
        Ok(ColumnarValue::Array(std::sync::Arc::new(array)))
    }
}
