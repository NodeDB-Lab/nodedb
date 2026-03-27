//! `rrf_score(rank1, rank2, ...)` — Reciprocal Rank Fusion scoring UDF.
//!
//! Computes: RRF_score = Σ 1 / (k + rank_i) for each rank input.
//! Used in ORDER BY for combining multiple ranking signals:
//!
//! ```sql
//! SELECT * FROM docs
//! ORDER BY rrf_score(vector_distance(embedding, ARRAY[...]), bm25_rank)
//! LIMIT 10
//! ```
//!
//! This is a pure scalar function that executes on the Control Plane
//! for post-processing of already-retrieved results. For full Data Plane
//! fusion, use the SEARCH ... USING FUSION DSL syntax instead.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use nodedb_query::DEFAULT_RRF_K;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RrfScore {
    signature: Signature,
}

impl RrfScore {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // 2 rank inputs.
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
                    // 3 rank inputs.
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    // Variadic fallback.
                    TypeSignature::Variadic(vec![DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for RrfScore {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for RrfScore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "rrf_score"
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
        let num_rows = args.number_rows;
        // Convert all args to arrays.
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|a| match a {
                ColumnarValue::Array(arr) => Ok(Arc::clone(arr)),
                ColumnarValue::Scalar(s) => s.to_array(),
            })
            .collect::<DfResult<Vec<_>>>()?;

        if arrays.is_empty() {
            return Ok(ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                0.0f64;
                num_rows
            ]))));
        }

        let len = arrays[0].len();
        let mut scores = vec![0.0f64; len];

        for arr in &arrays {
            let rank_arr = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "rrf_score: expected Float64 array".into(),
                )
            })?;

            for (i, score) in scores.iter_mut().enumerate().take(len) {
                if !rank_arr.is_null(i) {
                    let rank = rank_arr.value(i);
                    // RRF: 1 / (k + rank). Rank is treated as a distance/score
                    // where lower = better. The +1 makes it 1-based.
                    *score += 1.0 / (DEFAULT_RRF_K + rank.abs() + 1.0);
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(Float64Array::from(scores))))
    }
}
