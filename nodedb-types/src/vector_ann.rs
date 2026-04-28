//! Runtime ANN tuning options threaded from SQL planner → Data Plane.
//!
//! Mirrors `nodedb_sql::types::VectorAnnOptions` but lives in `nodedb-types`
//! so the wire-codec types in `nodedb` (which cannot depend on `nodedb-sql`)
//! can serialize them across the SPSC bridge.

#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VectorAnnOptions {
    pub quantization: Option<VectorQuantization>,
    pub oversample: Option<u8>,
    pub query_dim: Option<u32>,
    pub meta_token_budget: Option<u8>,
    pub ef_search_override: Option<usize>,
    pub target_recall: Option<f32>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum VectorQuantization {
    #[default]
    None,
    Sq8,
    Pq,
    RaBitQ,
    Bbq,
    Binary,
    Ternary,
    Opq,
}
