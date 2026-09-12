// SPDX-License-Identifier: BUSL-1.1

//! Response row structs for the search / graph / array execution paths.
//!
//! Most types use `#[derive(zerompk::ToMessagePack)]` with `#[msgpack(map)]`
//! which fixes field names to struct identifiers. `HybridSearchHit` carries
//! its score under a caller-supplied alias (the SELECT `AS <name>` for
//! `rrf_score(...)`), so it implements `ToMessagePack` by hand to write the
//! score under a dynamic key. Without this the alias would be lost and the
//! response would always carry `rrf_score` regardless of what the SQL named it.

use serde::Serialize;

use crate::data::executor::handlers::hybrid_key::HybridFusionKey;

#[derive(Serialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(in crate::data::executor) struct VectorSearchHit {
    pub id: HybridFusionKey,
    pub distance: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc_id: Option<String>,
    /// Raw msgpack body of the document, populated by the Data Plane only
    /// when `rls_filters` is non-empty so the Control Plane response
    /// translator can evaluate the predicate at the security boundary.
    /// Always `None` for non-RLS queries and stripped before the payload
    /// reaches the client.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Vec<u8>>,
}

#[derive(Serialize, Clone)]
pub(in crate::data::executor) struct DocumentRow {
    pub id: String,
    pub data: serde_json::Value,
}

impl zerompk::ToMessagePack for DocumentRow {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_map_len(2)?;
        writer.write_string("id")?;
        writer.write_string(&self.id)?;
        writer.write_string("data")?;
        nodedb_types::json_msgpack::JsonValue(self.data.clone()).write(writer)
    }
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(in crate::data::executor) struct NeighborEntry<'a> {
    pub label: &'a str,
    pub node: &'a str,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(in crate::data::executor) struct NeighborMultiEntry<'a> {
    pub src: &'a str,
    pub label: &'a str,
    pub node: &'a str,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(in crate::data::executor) struct SubgraphEdge<'a> {
    pub src: &'a str,
    pub label: &'a str,
    pub dst: &'a str,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(in crate::data::executor) struct GraphRagResult {
    pub node_id: String,
    pub rrf_score: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_distance: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hop_distance: Option<usize>,
}

/// Structured response for DML with RETURNING clause.
///
/// Carries one entry per affected row, with the projected column values.
/// The Control Plane decodes this to build a multi-column pgwire QueryResponse
/// (one pgwire field per entry in `columns`).
#[derive(Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct RowsPayload {
    /// Projected column names (output names, respecting AS aliases).
    pub columns: Vec<String>,
    /// One inner Vec per affected row; each inner Vec has one cell per
    /// column in the same order as `columns`. `None` denotes SQL NULL
    /// (missing field or JSON null); `Some` carries the TEXT representation.
    pub rows: Vec<Vec<Option<String>>>,
}

/// Carries the row payload alongside a flag that signals whether the
/// query's `system_as_of` cutoff fell below the oldest tile version on
/// this shard, meaning history was truncated. The shape is identical for
/// both local single-node responses and cluster shard responses.
#[derive(Serialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct ArraySliceResponse {
    /// Msgpack-encoded row bytes. Each element is one encoded `Value`.
    pub rows_msgpack: Vec<u8>,
    /// True when `system_as_of` is below the oldest tile version and the
    /// response may be incomplete due to horizon truncation.
    pub truncated_before_horizon: bool,
}

/// Structured response for `ArrayOp::Aggregate` (non-partial path).
///
/// Carries the aggregate row payload plus the horizon-truncation flag.
/// Partial responses (cluster fan-out) use a separate encoding to avoid
/// coupling the partial merge protocol to this struct.
#[derive(Serialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
#[allow(dead_code)]
pub(crate) struct ArrayAggregateResponse {
    /// Msgpack-encoded rows.
    pub rows_msgpack: Vec<u8>,
    /// True when the query's `system_as_of` fell below the oldest tile version.
    pub truncated_before_horizon: bool,
}

/// Hybrid-search hit row.
///
/// `score_field` is the map key under which `rrf_score` is serialized.
/// Defaults to `"rrf_score"` when no SELECT alias was provided. The custom
/// `ToMessagePack` impl exists because the derive macro fixes field names
/// to struct field identifiers, which would prevent the caller's chosen
/// alias from reaching the response — the precise bug this struct exists
/// to fix.
pub(in crate::data::executor) struct HybridSearchHit<'a> {
    pub doc_id: &'a str,
    pub score_field: &'a str,
    pub rrf_score: f64,
    pub vector_rank: Option<usize>,
    pub text_rank: Option<usize>,
}

impl<'a> zerompk::ToMessagePack for HybridSearchHit<'a> {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        let count =
            2usize + self.vector_rank.is_some() as usize + self.text_rank.is_some() as usize;
        writer.write_map_len(count)?;
        writer.write_string("doc_id")?;
        writer.write_string(self.doc_id)?;
        writer.write_string(self.score_field)?;
        writer.write_f64(self.rrf_score)?;
        if let Some(vr) = self.vector_rank {
            writer.write_string("vector_rank")?;
            writer.write_u64(vr as u64)?;
        }
        if let Some(tr) = self.text_rank {
            writer.write_string("text_rank")?;
            writer.write_u64(tr as u64)?;
        }
        Ok(())
    }
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(in crate::data::executor) struct GraphRagResponse {
    pub results: Vec<GraphRagResult>,
    pub metadata: GraphRagMetadata,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(in crate::data::executor) struct GraphRagMetadata {
    pub vector_candidates: usize,
    pub graph_expanded: usize,
    pub truncated: bool,
    /// Expanded nodes that carry no surrogate and therefore cannot intersect
    /// another engine's candidates. They are traversed and returned like any
    /// other node; what this reports is that the *cross-engine* view of the
    /// expansion is narrower than the expansion itself. Non-zero alongside a
    /// low result count means missing identity bindings, not a small graph.
    pub graph_unaddressable: usize,
    /// Snapshot watermark LSN at the time of query execution.
    /// Consumers can use this to verify they are reading a consistent view.
    pub watermark_lsn: u64,
}

#[cfg(test)]
mod tests {
    use super::super::encode::{decode_payload_to_json, encode};
    use super::*;

    #[test]
    fn encode_vector_hits() {
        let hits = vec![
            VectorSearchHit {
                id: HybridFusionKey::for_surrogate(nodedb_types::Surrogate::new(1)),
                distance: 0.5,
                doc_id: None,
                body: None,
            },
            VectorSearchHit {
                id: HybridFusionKey::for_surrogate(nodedb_types::Surrogate::new(2)),
                distance: 0.8,
                doc_id: None,
                body: None,
            },
        ];
        let bytes = encode(&hits).unwrap();
        assert!(!bytes.is_empty());

        let json = decode_payload_to_json(&bytes);
        assert!(json.contains("\"id\""));
        assert!(json.contains("\"distance\""));
    }
}
