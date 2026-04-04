//! Field-aware BM25 scoring: weighted multi-field scoring.
//!
//! `final_score = Σ(weight_i * bm25(field_i))`
//!
//! Each field has its own fieldnorm array and avg_doc_len.
//! Default weights configurable per collection.

use std::collections::HashMap;

use crate::bm25::bm25_score;
use crate::posting::Bm25Params;

/// Per-field scoring weight.
#[derive(Debug, Clone)]
pub struct FieldWeight {
    pub field: String,
    pub weight: f32,
}

/// Configuration for field-aware BM25 scoring.
#[derive(Debug, Clone, Default)]
pub struct FieldScoringConfig {
    /// Field weights: field_name → weight. Missing fields get weight 1.0.
    pub weights: HashMap<String, f32>,
}

impl FieldScoringConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the weight for a field. Higher weight = more influence on final score.
    pub fn set_weight(&mut self, field: &str, weight: f32) {
        self.weights.insert(field.to_string(), weight);
    }

    /// Get the weight for a field. Returns 1.0 if not configured.
    pub fn weight(&self, field: &str) -> f32 {
        self.weights.get(field).copied().unwrap_or(1.0)
    }
}

/// Per-field BM25 scoring data for a single document across one query term.
pub struct FieldScore {
    pub field: String,
    pub tf: u32,
    pub doc_len: u32,
}

/// Compute weighted multi-field BM25 score for a document.
///
/// `field_scores` contains per-field data for the document.
/// `config` provides per-field weights.
///
/// Formula: `Σ(weight_i * bm25(tf_i, df, doc_len_i, N, avg_dl_i))`
pub fn field_aware_bm25(
    field_scores: &[FieldScore],
    df: u32,
    total_docs: u32,
    avg_doc_lens: &HashMap<String, f32>,
    params: &Bm25Params,
    config: &FieldScoringConfig,
) -> f32 {
    let mut total = 0.0f32;

    for fs in field_scores {
        let avg_dl = avg_doc_lens.get(&fs.field).copied().unwrap_or(1.0);
        let weight = config.weight(&fs.field);
        let score = bm25_score(fs.tf, df, fs.doc_len, total_docs, avg_dl, params);
        total += weight * score;
    }

    total
}

/// Serialize field scoring config to bytes for metadata persistence.
pub fn config_to_bytes(config: &FieldScoringConfig) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(config.weights.len() as u32).to_le_bytes());
    for (field, weight) in &config.weights {
        buf.extend_from_slice(&(field.len() as u16).to_le_bytes());
        buf.extend_from_slice(field.as_bytes());
        buf.extend_from_slice(&weight.to_le_bytes());
    }
    buf
}

/// Deserialize field scoring config from bytes.
pub fn config_from_bytes(buf: &[u8]) -> Option<FieldScoringConfig> {
    if buf.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let mut pos = 4;
    let mut weights = HashMap::with_capacity(count);

    for _ in 0..count {
        if pos + 2 > buf.len() {
            return None;
        }
        let field_len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        if pos + field_len + 4 > buf.len() {
            return None;
        }
        let field = std::str::from_utf8(&buf[pos..pos + field_len]).ok()?;
        pos += field_len;
        let weight = f32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
        pos += 4;
        weights.insert(field.to_string(), weight);
    }

    Some(FieldScoringConfig { weights })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_weight_is_one() {
        let config = FieldScoringConfig::new();
        assert!((config.weight("title") - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn custom_weights() {
        let mut config = FieldScoringConfig::new();
        config.set_weight("title", 3.0);
        config.set_weight("body", 1.0);

        assert!((config.weight("title") - 3.0).abs() < f32::EPSILON);
        assert!((config.weight("body") - 1.0).abs() < f32::EPSILON);
        assert!((config.weight("other") - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn field_aware_scoring() {
        let mut config = FieldScoringConfig::new();
        config.set_weight("title", 3.0);
        config.set_weight("body", 1.0);

        let params = Bm25Params::default();
        let mut avg_lens = HashMap::new();
        avg_lens.insert("title".to_string(), 10.0);
        avg_lens.insert("body".to_string(), 200.0);

        let fields = vec![
            FieldScore {
                field: "title".into(),
                tf: 2,
                doc_len: 8,
            },
            FieldScore {
                field: "body".into(),
                tf: 1,
                doc_len: 150,
            },
        ];

        let score = field_aware_bm25(&fields, 10, 1000, &avg_lens, &params, &config);
        assert!(score > 0.0);

        // Title-only score should be lower (missing body contribution).
        let title_only = field_aware_bm25(&fields[..1], 10, 1000, &avg_lens, &params, &config);
        assert!(score > title_only);
    }

    #[test]
    fn config_serialization_roundtrip() {
        let mut config = FieldScoringConfig::new();
        config.set_weight("title", 3.0);
        config.set_weight("body", 1.5);

        let bytes = config_to_bytes(&config);
        let restored = config_from_bytes(&bytes).unwrap();

        assert!((restored.weight("title") - 3.0).abs() < f32::EPSILON);
        assert!((restored.weight("body") - 1.5).abs() < f32::EPSILON);
    }

    #[test]
    fn empty_config_serialization() {
        let config = FieldScoringConfig::new();
        let bytes = config_to_bytes(&config);
        let restored = config_from_bytes(&bytes).unwrap();
        assert!(restored.weights.is_empty());
    }
}
