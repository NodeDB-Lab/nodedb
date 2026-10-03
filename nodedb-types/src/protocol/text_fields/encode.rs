// SPDX-License-Identifier: Apache-2.0

//! MsgPack encoding for [`TextFields`].

use crate::json_msgpack::JsonValue;

use super::field_ids::*;
use super::types::TextFields;

/// Write an optional field: if `Some`, write the field ID then the value.
macro_rules! write_opt_field {
    ($writer:expr, $id:expr, $val:expr) => {
        if let Some(ref v) = $val {
            $writer.write_u16($id)?;
            v.write($writer)?;
        }
    };
    // variant for types that need JsonValue wrapping
    (json $writer:expr, $id:expr, $val:expr) => {
        if let Some(ref v) = $val {
            $writer.write_u16($id)?;
            JsonValue(v.clone()).write($writer)?;
        }
    };
    // variant for `Option<Vec<u8>>` payloads that must encode as a
    // MessagePack `bin8/bin16/bin32` blob. The generic
    // `<Vec<u8> as ToMessagePack>::write` writes an
    // `array<u8>` (length header + each byte as a `cc XX` u8) — that
    // round-trips on the same generic path but blows up the
    // `reader.read_binary()` decoder these fields use with
    // `InvalidMarker(0x94…)`. Calling `write_binary` directly keeps
    // encoder and decoder agreed on the `bin*` marker family.
    (binary $writer:expr, $id:expr, $val:expr) => {
        if let Some(ref v) = $val {
            $writer.write_u16($id)?;
            $writer.write_binary(v.as_slice())?;
        }
    };
}

impl zerompk::ToMessagePack for TextFields {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_map_len(self.present_field_count())?;

        write_opt_field!(writer, FID_AUTH, self.auth);
        write_opt_field!(writer, FID_SQL, self.sql);
        write_opt_field!(writer, FID_KEY, self.key);
        write_opt_field!(writer, FID_VALUE, self.value);
        write_opt_field!(writer, FID_COLLECTION, self.collection);
        write_opt_field!(writer, FID_DOCUMENT_ID, self.document_id);
        write_opt_field!(binary writer, FID_DATA, self.data);
        write_opt_field!(writer, FID_QUERY_VECTOR, self.query_vector);
        write_opt_field!(writer, FID_TOP_K, self.top_k);
        write_opt_field!(writer, FID_FIELD, self.field);
        write_opt_field!(writer, FID_LIMIT, self.limit);
        write_opt_field!(binary writer, FID_DELTA, self.delta);
        write_opt_field!(writer, FID_PEER_ID, self.peer_id);
        write_opt_field!(writer, FID_VECTOR_TOP_K, self.vector_top_k);
        write_opt_field!(writer, FID_EDGE_LABELS, self.edge_labels);
        write_opt_field!(writer, FID_DIRECTION, self.direction);
        write_opt_field!(writer, FID_EXPANSION_DEPTH, self.expansion_depth);
        write_opt_field!(writer, FID_FINAL_TOP_K, self.final_top_k);
        write_opt_field!(writer, FID_VECTOR_K, self.vector_k);
        write_opt_field!(writer, FID_GRAPH_K, self.graph_k);
        write_opt_field!(writer, FID_VECTOR_FIELD, self.vector_field);
        write_opt_field!(writer, FID_START_NODE, self.start_node);
        write_opt_field!(writer, FID_END_NODE, self.end_node);
        write_opt_field!(writer, FID_DEPTH, self.depth);
        write_opt_field!(writer, FID_FROM_NODE, self.from_node);
        write_opt_field!(writer, FID_TO_NODE, self.to_node);
        write_opt_field!(writer, FID_EDGE_TYPE, self.edge_type);
        write_opt_field!(json writer, FID_PROPERTIES, self.properties);
        write_opt_field!(writer, FID_QUERY_TEXT, self.query_text);
        write_opt_field!(writer, FID_VECTOR_WEIGHT, self.vector_weight);
        write_opt_field!(writer, FID_FUZZY, self.fuzzy);
        write_opt_field!(writer, FID_EF_SEARCH, self.ef_search);
        write_opt_field!(writer, FID_FIELD_NAME, self.field_name);
        write_opt_field!(binary writer, FID_LOWER_BOUND, self.lower_bound);
        write_opt_field!(binary writer, FID_UPPER_BOUND, self.upper_bound);
        write_opt_field!(writer, FID_MUTATION_ID, self.mutation_id);
        write_opt_field!(writer, FID_VECTORS, self.vectors);
        write_opt_field!(writer, FID_DOCUMENTS, self.documents);
        write_opt_field!(binary writer, FID_QUERY_GEOMETRY, self.query_geometry);
        write_opt_field!(writer, FID_SPATIAL_PREDICATE, self.spatial_predicate);
        write_opt_field!(writer, FID_DISTANCE_METERS, self.distance_meters);
        write_opt_field!(binary writer, FID_PAYLOAD, self.payload);
        write_opt_field!(writer, FID_FORMAT, self.format);
        write_opt_field!(writer, FID_TIME_RANGE_START, self.time_range_start);
        write_opt_field!(writer, FID_TIME_RANGE_END, self.time_range_end);
        write_opt_field!(writer, FID_BUCKET_INTERVAL, self.bucket_interval);
        write_opt_field!(writer, FID_TTL_MS, self.ttl_ms);
        write_opt_field!(binary writer, FID_CURSOR, self.cursor);
        write_opt_field!(writer, FID_MATCH_PATTERN, self.match_pattern);
        write_opt_field!(writer, FID_KEYS, self.keys);
        write_opt_field!(writer, FID_ENTRIES, self.entries);
        write_opt_field!(writer, FID_FIELDS, self.fields);
        write_opt_field!(writer, FID_INCR_DELTA, self.incr_delta);
        write_opt_field!(writer, FID_INCR_FLOAT_DELTA, self.incr_float_delta);
        write_opt_field!(binary writer, FID_EXPECTED, self.expected);
        write_opt_field!(binary writer, FID_NEW_VALUE, self.new_value);
        write_opt_field!(writer, FID_INDEX_NAME, self.index_name);
        write_opt_field!(writer, FID_SORT_COLUMNS, self.sort_columns);
        write_opt_field!(writer, FID_KEY_COLUMN, self.key_column);
        write_opt_field!(writer, FID_WINDOW_TYPE, self.window_type);
        write_opt_field!(
            writer,
            FID_WINDOW_TIMESTAMP_COLUMN,
            self.window_timestamp_column
        );
        write_opt_field!(writer, FID_WINDOW_START_MS, self.window_start_ms);
        write_opt_field!(writer, FID_WINDOW_END_MS, self.window_end_ms);
        write_opt_field!(writer, FID_TOP_K_COUNT, self.top_k_count);
        write_opt_field!(binary writer, FID_SCORE_MIN, self.score_min);
        write_opt_field!(binary writer, FID_SCORE_MAX, self.score_max);
        write_opt_field!(writer, FID_UPDATES, self.updates);
        write_opt_field!(binary writer, FID_FILTERS, self.filters);
        write_opt_field!(writer, FID_VECTOR, self.vector);
        write_opt_field!(writer, FID_VECTOR_ID, self.vector_id);
        write_opt_field!(json writer, FID_POLICY, self.policy);
        write_opt_field!(writer, FID_ALGORITHM, self.algorithm);
        write_opt_field!(writer, FID_MATCH_QUERY, self.match_query);
        write_opt_field!(json writer, FID_ALGO_PARAMS, self.algo_params);
        write_opt_field!(writer, FID_INDEX_PATHS, self.index_paths);
        write_opt_field!(writer, FID_SOURCE_COLLECTION, self.source_collection);
        write_opt_field!(writer, FID_FIELD_POSITION, self.field_position);
        write_opt_field!(writer, FID_BACKFILL, self.backfill);
        write_opt_field!(writer, FID_M, self.m);
        write_opt_field!(writer, FID_EF_CONSTRUCTION, self.ef_construction);
        write_opt_field!(writer, FID_METRIC, self.metric);
        write_opt_field!(writer, FID_INDEX_TYPE, self.index_type);
        write_opt_field!(writer, FID_VECTOR_DIM, self.vector_dim);
        write_opt_field!(writer, FID_DATABASE, self.database);
        write_opt_field!(writer, FID_SQL_PARAMS, self.sql_params);
        write_opt_field!(writer, FID_LIST_PATH, self.list_path);
        write_opt_field!(writer, FID_LIST_INDEX, self.list_index);
        write_opt_field!(writer, FID_LIST_FROM_INDEX, self.list_from_index);
        write_opt_field!(writer, FID_LIST_TO_INDEX, self.list_to_index);
        write_opt_field!(writer, FID_LIST_FIELDS_JSON, self.list_fields_json);
        write_opt_field!(writer, FID_TEXT_MODE, self.text_mode);
        write_opt_field!(writer, FID_ALLOWED_IDS, self.allowed_ids);

        Ok(())
    }
}
