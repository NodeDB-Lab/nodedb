//! Algorithm result → Arrow RecordBatch conversion.
//!
//! Converts algorithm output (per-node scores, component IDs, etc.)
//! into Arrow RecordBatches for seamless DataFusion integration.
//! Enables `SELECT * FROM (GRAPH ALGO PAGERANK ON users) WHERE rank > 0.01`.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use super::params::{AlgoColumnType, GraphAlgorithm};

/// A batch of algorithm results ready for Arrow/DataFusion consumption.
///
/// Results are stored column-major: each column is a `Vec` of typed values.
/// `to_record_batch()` converts to an Arrow `RecordBatch`.
#[derive(Debug)]
pub struct AlgoResultBatch {
    algorithm: GraphAlgorithm,
    text_columns: Vec<Vec<String>>,
    f64_columns: Vec<Vec<f64>>,
    i64_columns: Vec<Vec<i64>>,
    /// Column ordering: maps schema column index → (type, vec_index).
    column_map: Vec<(AlgoColumnType, usize)>,
    row_count: usize,
}

impl AlgoResultBatch {
    /// Create a new result batch for the given algorithm.
    pub fn new(algorithm: GraphAlgorithm) -> Self {
        let schema = algorithm.result_schema();
        let mut text_count = 0usize;
        let mut f64_count = 0usize;
        let mut i64_count = 0usize;
        let mut column_map = Vec::with_capacity(schema.len());

        for &(_, col_type) in schema {
            match col_type {
                AlgoColumnType::Text => {
                    column_map.push((col_type, text_count));
                    text_count += 1;
                }
                AlgoColumnType::Float64 => {
                    column_map.push((col_type, f64_count));
                    f64_count += 1;
                }
                AlgoColumnType::Int64 => {
                    column_map.push((col_type, i64_count));
                    i64_count += 1;
                }
            }
        }

        Self {
            algorithm,
            text_columns: vec![Vec::new(); text_count],
            f64_columns: vec![Vec::new(); f64_count],
            i64_columns: vec![Vec::new(); i64_count],
            column_map,
            row_count: 0,
        }
    }

    /// Add a row with node_id and f64 score (PageRank, SSSP, LCC, centrality).
    pub fn push_node_f64(&mut self, node_id: String, value: f64) {
        self.text_columns[0].push(node_id);
        self.f64_columns[0].push(value);
        self.row_count += 1;
    }

    /// Add a row with node_id and i64 value (WCC component, community, triangles, coreness).
    pub fn push_node_i64(&mut self, node_id: String, value: i64) {
        self.text_columns[0].push(node_id);
        self.i64_columns[0].push(value);
        self.row_count += 1;
    }

    /// Add a Louvain row: node_id, community_id, modularity.
    pub fn push_louvain(&mut self, node_id: String, community_id: i64, modularity: f64) {
        self.text_columns[0].push(node_id);
        self.i64_columns[0].push(community_id);
        self.f64_columns[0].push(modularity);
        self.row_count += 1;
    }

    /// Add a diameter row: diameter, radius.
    pub fn push_diameter(&mut self, diameter: i64, radius: i64) {
        self.i64_columns[0].push(diameter);
        self.i64_columns[1].push(radius);
        self.row_count += 1;
    }

    /// Number of result rows.
    pub fn len(&self) -> usize {
        self.row_count
    }

    /// Whether the result set is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Build the Arrow schema from the algorithm's result schema definition.
    fn arrow_schema(&self) -> Schema {
        let fields: Vec<Field> = self
            .algorithm
            .result_schema()
            .iter()
            .map(|&(name, col_type)| {
                let dt = match col_type {
                    AlgoColumnType::Text => DataType::Utf8,
                    AlgoColumnType::Float64 => DataType::Float64,
                    AlgoColumnType::Int64 => DataType::Int64,
                };
                Field::new(name, dt, false)
            })
            .collect();
        Schema::new(fields)
    }

    /// Convert to an Arrow `RecordBatch`.
    ///
    /// Consumes the batch. Returns `Err` if column lengths are inconsistent
    /// (should never happen with correct `push_*` usage).
    pub fn to_record_batch(self) -> Result<RecordBatch, crate::Error> {
        let schema = Arc::new(self.arrow_schema());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.column_map.len());

        for &(col_type, idx) in &self.column_map {
            let array: ArrayRef = match col_type {
                AlgoColumnType::Text => Arc::new(StringArray::from(self.text_columns[idx].clone())),
                AlgoColumnType::Float64 => {
                    Arc::new(Float64Array::from(self.f64_columns[idx].clone()))
                }
                AlgoColumnType::Int64 => Arc::new(Int64Array::from(self.i64_columns[idx].clone())),
            };
            columns.push(array);
        }

        RecordBatch::try_new(schema, columns).map_err(|e| crate::Error::Internal {
            detail: format!("arrow result batch: {e}"),
        })
    }

    /// Serialize the result batch as JSON (for pgwire/REST response).
    ///
    /// Returns a JSON array of objects: `[{"node_id": "alice", "rank": 0.42}, ...]`.
    pub fn to_json(&self) -> Result<Vec<u8>, crate::Error> {
        let schema = self.algorithm.result_schema();
        let mut rows = Vec::with_capacity(self.row_count);

        for row_idx in 0..self.row_count {
            let mut obj = serde_json::Map::new();
            for (col_idx, &(col_name, _col_type)) in schema.iter().enumerate() {
                let (col_type, vec_idx) = self.column_map[col_idx];
                let val = match col_type {
                    AlgoColumnType::Text => {
                        serde_json::Value::String(self.text_columns[vec_idx][row_idx].clone())
                    }
                    AlgoColumnType::Float64 => {
                        serde_json::json!(self.f64_columns[vec_idx][row_idx])
                    }
                    AlgoColumnType::Int64 => {
                        serde_json::json!(self.i64_columns[vec_idx][row_idx])
                    }
                };
                obj.insert(col_name.to_string(), val);
            }
            rows.push(serde_json::Value::Object(obj));
        }

        serde_json::to_vec(&rows).map_err(|e| crate::Error::Internal {
            detail: format!("json serialization: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagerank_result_to_record_batch() {
        let mut batch = AlgoResultBatch::new(GraphAlgorithm::PageRank);
        batch.push_node_f64("alice".into(), 0.42);
        batch.push_node_f64("bob".into(), 0.31);
        batch.push_node_f64("carol".into(), 0.27);

        assert_eq!(batch.len(), 3);

        let rb = batch.to_record_batch().unwrap();
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 2);
        assert_eq!(rb.schema().field(0).name(), "node_id");
        assert_eq!(rb.schema().field(1).name(), "rank");
    }

    #[test]
    fn wcc_result_to_record_batch() {
        let mut batch = AlgoResultBatch::new(GraphAlgorithm::Wcc);
        batch.push_node_i64("n1".into(), 0);
        batch.push_node_i64("n2".into(), 0);
        batch.push_node_i64("n3".into(), 1);

        let rb = batch.to_record_batch().unwrap();
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.schema().field(1).name(), "component_id");
    }

    #[test]
    fn louvain_result_three_columns() {
        let mut batch = AlgoResultBatch::new(GraphAlgorithm::Louvain);
        batch.push_louvain("alice".into(), 1, 0.45);
        batch.push_louvain("bob".into(), 2, 0.45);

        let rb = batch.to_record_batch().unwrap();
        assert_eq!(rb.num_columns(), 3);
        assert_eq!(rb.schema().field(0).name(), "node_id");
        assert_eq!(rb.schema().field(1).name(), "community_id");
        assert_eq!(rb.schema().field(2).name(), "modularity");
    }

    #[test]
    fn diameter_result() {
        let mut batch = AlgoResultBatch::new(GraphAlgorithm::Diameter);
        batch.push_diameter(7, 4);

        let rb = batch.to_record_batch().unwrap();
        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 2);
    }

    #[test]
    fn empty_result() {
        let batch = AlgoResultBatch::new(GraphAlgorithm::PageRank);
        assert!(batch.is_empty());
        let rb = batch.to_record_batch().unwrap();
        assert_eq!(rb.num_rows(), 0);
    }

    #[test]
    fn result_to_json() {
        let mut batch = AlgoResultBatch::new(GraphAlgorithm::PageRank);
        batch.push_node_f64("alice".into(), 0.5);
        batch.push_node_f64("bob".into(), 0.3);

        let json_bytes = batch.to_json().unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["node_id"], "alice");
        assert_eq!(parsed[0]["rank"], 0.5);
    }

    #[test]
    fn kcore_result_to_json() {
        let mut batch = AlgoResultBatch::new(GraphAlgorithm::KCore);
        batch.push_node_i64("hub".into(), 5);

        let json_bytes = batch.to_json().unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(parsed[0]["coreness"], 5);
    }
}
