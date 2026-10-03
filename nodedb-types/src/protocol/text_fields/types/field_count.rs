// SPDX-License-Identifier: Apache-2.0

//! Present-field counting for the MsgPack map length header.

use super::TextFields;

impl TextFields {
    /// Count the number of `Some(...)` fields — used by the MsgPack encoder
    /// to write the correct map length header.
    pub(in crate::protocol::text_fields) fn present_field_count(&self) -> usize {
        let mut n = 0usize;
        if self.auth.is_some() {
            n += 1;
        }
        if self.sql.is_some() {
            n += 1;
        }
        if self.key.is_some() {
            n += 1;
        }
        if self.value.is_some() {
            n += 1;
        }
        if self.collection.is_some() {
            n += 1;
        }
        if self.document_id.is_some() {
            n += 1;
        }
        if self.data.is_some() {
            n += 1;
        }
        if self.query_vector.is_some() {
            n += 1;
        }
        if self.top_k.is_some() {
            n += 1;
        }
        if self.field.is_some() {
            n += 1;
        }
        if self.limit.is_some() {
            n += 1;
        }
        if self.delta.is_some() {
            n += 1;
        }
        if self.peer_id.is_some() {
            n += 1;
        }
        if self.vector_top_k.is_some() {
            n += 1;
        }
        if self.edge_labels.is_some() {
            n += 1;
        }
        if self.direction.is_some() {
            n += 1;
        }
        if self.expansion_depth.is_some() {
            n += 1;
        }
        if self.final_top_k.is_some() {
            n += 1;
        }
        if self.vector_k.is_some() {
            n += 1;
        }
        if self.graph_k.is_some() {
            n += 1;
        }
        if self.vector_field.is_some() {
            n += 1;
        }
        if self.start_node.is_some() {
            n += 1;
        }
        if self.end_node.is_some() {
            n += 1;
        }
        if self.depth.is_some() {
            n += 1;
        }
        if self.from_node.is_some() {
            n += 1;
        }
        if self.to_node.is_some() {
            n += 1;
        }
        if self.edge_type.is_some() {
            n += 1;
        }
        if self.properties.is_some() {
            n += 1;
        }
        if self.query_text.is_some() {
            n += 1;
        }
        if self.vector_weight.is_some() {
            n += 1;
        }
        if self.fuzzy.is_some() {
            n += 1;
        }
        if self.ef_search.is_some() {
            n += 1;
        }
        if self.field_name.is_some() {
            n += 1;
        }
        if self.lower_bound.is_some() {
            n += 1;
        }
        if self.upper_bound.is_some() {
            n += 1;
        }
        if self.mutation_id.is_some() {
            n += 1;
        }
        if self.vectors.is_some() {
            n += 1;
        }
        if self.documents.is_some() {
            n += 1;
        }
        if self.query_geometry.is_some() {
            n += 1;
        }
        if self.spatial_predicate.is_some() {
            n += 1;
        }
        if self.distance_meters.is_some() {
            n += 1;
        }
        if self.payload.is_some() {
            n += 1;
        }
        if self.format.is_some() {
            n += 1;
        }
        if self.time_range_start.is_some() {
            n += 1;
        }
        if self.time_range_end.is_some() {
            n += 1;
        }
        if self.bucket_interval.is_some() {
            n += 1;
        }
        if self.ttl_ms.is_some() {
            n += 1;
        }
        if self.cursor.is_some() {
            n += 1;
        }
        if self.match_pattern.is_some() {
            n += 1;
        }
        if self.keys.is_some() {
            n += 1;
        }
        if self.entries.is_some() {
            n += 1;
        }
        if self.fields.is_some() {
            n += 1;
        }
        if self.incr_delta.is_some() {
            n += 1;
        }
        if self.incr_float_delta.is_some() {
            n += 1;
        }
        if self.expected.is_some() {
            n += 1;
        }
        if self.new_value.is_some() {
            n += 1;
        }
        if self.index_name.is_some() {
            n += 1;
        }
        if self.sort_columns.is_some() {
            n += 1;
        }
        if self.key_column.is_some() {
            n += 1;
        }
        if self.window_type.is_some() {
            n += 1;
        }
        if self.window_timestamp_column.is_some() {
            n += 1;
        }
        if self.window_start_ms.is_some() {
            n += 1;
        }
        if self.window_end_ms.is_some() {
            n += 1;
        }
        if self.top_k_count.is_some() {
            n += 1;
        }
        if self.score_min.is_some() {
            n += 1;
        }
        if self.score_max.is_some() {
            n += 1;
        }
        if self.updates.is_some() {
            n += 1;
        }
        if self.filters.is_some() {
            n += 1;
        }
        if self.vector.is_some() {
            n += 1;
        }
        if self.vector_id.is_some() {
            n += 1;
        }
        if self.policy.is_some() {
            n += 1;
        }
        if self.algorithm.is_some() {
            n += 1;
        }
        if self.match_query.is_some() {
            n += 1;
        }
        if self.algo_params.is_some() {
            n += 1;
        }
        if self.index_paths.is_some() {
            n += 1;
        }
        if self.source_collection.is_some() {
            n += 1;
        }
        if self.field_position.is_some() {
            n += 1;
        }
        if self.backfill.is_some() {
            n += 1;
        }
        if self.m.is_some() {
            n += 1;
        }
        if self.ef_construction.is_some() {
            n += 1;
        }
        if self.metric.is_some() {
            n += 1;
        }
        if self.index_type.is_some() {
            n += 1;
        }
        if self.vector_dim.is_some() {
            n += 1;
        }
        if self.database.is_some() {
            n += 1;
        }
        if self.sql_params.is_some() {
            n += 1;
        }
        if self.list_path.is_some() {
            n += 1;
        }
        if self.list_index.is_some() {
            n += 1;
        }
        if self.list_from_index.is_some() {
            n += 1;
        }
        if self.list_to_index.is_some() {
            n += 1;
        }
        if self.list_fields_json.is_some() {
            n += 1;
        }
        if self.text_mode.is_some() {
            n += 1;
        }
        if self.allowed_ids.is_some() {
            n += 1;
        }
        n
    }
}
