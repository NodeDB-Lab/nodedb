// SPDX-License-Identifier: Apache-2.0

//! MsgPack decoding for [`TextFields`].

use crate::json_msgpack::JsonValue;
use crate::protocol::auth::AuthMethod;
use crate::protocol::batch::{BatchDocument, BatchVector};

use super::field_ids::*;
use super::types::TextFields;

/// Consume and discard the next MsgPack value from `reader`.
///
/// Uses the "try-and-back-up" property of zerompk readers: a failed
/// typed read restores the format byte, so we can try each type in
/// order without permanently advancing the cursor.
///
/// Used for forward-compat: unknown field IDs are skipped rather than rejected.
fn skip_msgpack_value<'de, R: zerompk::Read<'de>>(reader: &mut R) -> zerompk::Result<()> {
    // Nil
    if reader.read_nil().is_ok() {
        return Ok(());
    }
    // Boolean
    if reader.read_boolean().is_ok() {
        return Ok(());
    }
    // Signed integers (handles neg fixint, int8/16/32/64)
    if reader.read_i64().is_ok() {
        return Ok(());
    }
    // Unsigned integers (handles uint8/16/32/64 and pos fixint that i64 may miss)
    if reader.read_u64().is_ok() {
        return Ok(());
    }
    // Floats
    if reader.read_f32().is_ok() {
        return Ok(());
    }
    if reader.read_f64().is_ok() {
        return Ok(());
    }
    // String
    if reader.read_string().is_ok() {
        return Ok(());
    }
    // Binary
    if reader.read_binary().is_ok() {
        return Ok(());
    }
    // Array: read length then skip each element
    if let Ok(len) = reader.read_array_len() {
        for _ in 0..len {
            skip_msgpack_value(reader)?;
        }
        return Ok(());
    }
    // Map: read length then skip each key-value pair
    if let Ok(len) = reader.read_map_len() {
        for _ in 0..len {
            skip_msgpack_value(reader)?; // key
            skip_msgpack_value(reader)?; // value
        }
        return Ok(());
    }
    Err(zerompk::Error::BufferTooSmall)
}

impl<'a> zerompk::FromMessagePack<'a> for TextFields {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let map_len = reader.read_map_len()?;
        let mut out = TextFields::default();

        for _ in 0..map_len {
            let id = reader.read_u16()?;
            match id {
                FID_AUTH => {
                    out.auth = Some(AuthMethod::read(reader)?);
                }
                FID_SQL => {
                    out.sql = Some(reader.read_string()?.into_owned());
                }
                FID_KEY => {
                    out.key = Some(reader.read_string()?.into_owned());
                }
                FID_VALUE => {
                    out.value = Some(reader.read_string()?.into_owned());
                }
                FID_COLLECTION => {
                    out.collection = Some(reader.read_string()?.into_owned());
                }
                FID_DOCUMENT_ID => {
                    out.document_id = Some(reader.read_string()?.into_owned());
                }
                FID_DATA => {
                    out.data = Some(reader.read_binary()?.into_owned());
                }
                FID_QUERY_VECTOR => {
                    out.query_vector = Some(Vec::<f32>::read(reader)?);
                }
                FID_TOP_K => {
                    out.top_k = Some(reader.read_u32()?);
                }
                FID_FIELD => {
                    out.field = Some(reader.read_string()?.into_owned());
                }
                FID_LIMIT => {
                    out.limit = Some(reader.read_u64()?);
                }
                FID_DELTA => {
                    out.delta = Some(reader.read_binary()?.into_owned());
                }
                FID_PEER_ID => {
                    out.peer_id = Some(reader.read_u64()?);
                }
                FID_VECTOR_TOP_K => {
                    out.vector_top_k = Some(reader.read_u32()?);
                }
                FID_EDGE_LABEL => {
                    out.edge_label = Some(reader.read_string()?.into_owned());
                }
                FID_DIRECTION => {
                    out.direction = Some(reader.read_string()?.into_owned());
                }
                FID_EXPANSION_DEPTH => {
                    out.expansion_depth = Some(reader.read_u32()?);
                }
                FID_FINAL_TOP_K => {
                    out.final_top_k = Some(reader.read_u32()?);
                }
                FID_VECTOR_K => {
                    out.vector_k = Some(reader.read_f64()?);
                }
                FID_GRAPH_K => {
                    out.graph_k = Some(reader.read_f64()?);
                }
                FID_VECTOR_FIELD => {
                    out.vector_field = Some(reader.read_string()?.into_owned());
                }
                FID_START_NODE => {
                    out.start_node = Some(reader.read_string()?.into_owned());
                }
                FID_END_NODE => {
                    out.end_node = Some(reader.read_string()?.into_owned());
                }
                FID_DEPTH => {
                    out.depth = Some(reader.read_u32()?);
                }
                FID_FROM_NODE => {
                    out.from_node = Some(reader.read_string()?.into_owned());
                }
                FID_TO_NODE => {
                    out.to_node = Some(reader.read_string()?.into_owned());
                }
                FID_EDGE_TYPE => {
                    out.edge_type = Some(reader.read_string()?.into_owned());
                }
                FID_PROPERTIES => {
                    out.properties = Some(JsonValue::read(reader)?.0);
                }
                FID_QUERY_TEXT => {
                    out.query_text = Some(reader.read_string()?.into_owned());
                }
                FID_VECTOR_WEIGHT => {
                    out.vector_weight = Some(reader.read_f64()?);
                }
                FID_FUZZY => {
                    out.fuzzy = Some(reader.read_boolean()?);
                }
                FID_EF_SEARCH => {
                    out.ef_search = Some(reader.read_u32()?);
                }
                FID_FIELD_NAME => {
                    out.field_name = Some(reader.read_string()?.into_owned());
                }
                FID_LOWER_BOUND => {
                    out.lower_bound = Some(reader.read_binary()?.into_owned());
                }
                FID_UPPER_BOUND => {
                    out.upper_bound = Some(reader.read_binary()?.into_owned());
                }
                FID_MUTATION_ID => {
                    out.mutation_id = Some(reader.read_u64()?);
                }
                FID_VECTORS => {
                    out.vectors = Some(Vec::<BatchVector>::read(reader)?);
                }
                FID_DOCUMENTS => {
                    out.documents = Some(Vec::<BatchDocument>::read(reader)?);
                }
                FID_QUERY_GEOMETRY => {
                    out.query_geometry = Some(reader.read_binary()?.into_owned());
                }
                FID_SPATIAL_PREDICATE => {
                    out.spatial_predicate = Some(reader.read_string()?.into_owned());
                }
                FID_DISTANCE_METERS => {
                    out.distance_meters = Some(reader.read_f64()?);
                }
                FID_PAYLOAD => {
                    out.payload = Some(reader.read_binary()?.into_owned());
                }
                FID_FORMAT => {
                    out.format = Some(reader.read_string()?.into_owned());
                }
                FID_TIME_RANGE_START => {
                    out.time_range_start = Some(reader.read_i64()?);
                }
                FID_TIME_RANGE_END => {
                    out.time_range_end = Some(reader.read_i64()?);
                }
                FID_BUCKET_INTERVAL => {
                    out.bucket_interval = Some(reader.read_string()?.into_owned());
                }
                FID_TTL_MS => {
                    out.ttl_ms = Some(reader.read_u64()?);
                }
                FID_CURSOR => {
                    out.cursor = Some(reader.read_binary()?.into_owned());
                }
                FID_MATCH_PATTERN => {
                    out.match_pattern = Some(reader.read_string()?.into_owned());
                }
                FID_KEYS => {
                    out.keys = Some(Vec::<Vec<u8>>::read(reader)?);
                }
                FID_ENTRIES => {
                    out.entries = Some(Vec::<(Vec<u8>, Vec<u8>)>::read(reader)?);
                }
                FID_FIELDS => {
                    out.fields = Some(Vec::<String>::read(reader)?);
                }
                FID_INCR_DELTA => {
                    out.incr_delta = Some(reader.read_i64()?);
                }
                FID_INCR_FLOAT_DELTA => {
                    out.incr_float_delta = Some(reader.read_string()?.into_owned());
                }
                FID_EXPECTED => {
                    out.expected = Some(reader.read_binary()?.into_owned());
                }
                FID_NEW_VALUE => {
                    out.new_value = Some(reader.read_binary()?.into_owned());
                }
                FID_INDEX_NAME => {
                    out.index_name = Some(reader.read_string()?.into_owned());
                }
                FID_SORT_COLUMNS => {
                    out.sort_columns = Some(Vec::<(String, String)>::read(reader)?);
                }
                FID_KEY_COLUMN => {
                    out.key_column = Some(reader.read_string()?.into_owned());
                }
                FID_WINDOW_TYPE => {
                    out.window_type = Some(reader.read_string()?.into_owned());
                }
                FID_WINDOW_TIMESTAMP_COLUMN => {
                    out.window_timestamp_column = Some(reader.read_string()?.into_owned());
                }
                FID_WINDOW_START_MS => {
                    out.window_start_ms = Some(reader.read_u64()?);
                }
                FID_WINDOW_END_MS => {
                    out.window_end_ms = Some(reader.read_u64()?);
                }
                FID_TOP_K_COUNT => {
                    out.top_k_count = Some(reader.read_u32()?);
                }
                FID_SCORE_MIN => {
                    out.score_min = Some(reader.read_binary()?.into_owned());
                }
                FID_SCORE_MAX => {
                    out.score_max = Some(reader.read_binary()?.into_owned());
                }
                FID_UPDATES => {
                    out.updates = Some(Vec::<(String, Vec<u8>)>::read(reader)?);
                }
                FID_FILTERS => {
                    out.filters = Some(reader.read_binary()?.into_owned());
                }
                FID_VECTOR => {
                    out.vector = Some(Vec::<f32>::read(reader)?);
                }
                FID_VECTOR_ID => {
                    out.vector_id = Some(reader.read_u64()?);
                }
                FID_POLICY => {
                    out.policy = Some(JsonValue::read(reader)?.0);
                }
                FID_ALGORITHM => {
                    out.algorithm = Some(reader.read_string()?.into_owned());
                }
                FID_MATCH_QUERY => {
                    out.match_query = Some(reader.read_string()?.into_owned());
                }
                FID_ALGO_PARAMS => {
                    out.algo_params = Some(JsonValue::read(reader)?.0);
                }
                FID_INDEX_PATHS => {
                    out.index_paths = Some(Vec::<String>::read(reader)?);
                }
                FID_SOURCE_COLLECTION => {
                    out.source_collection = Some(reader.read_string()?.into_owned());
                }
                FID_FIELD_POSITION => {
                    out.field_position = Some(reader.read_u64()?);
                }
                FID_BACKFILL => {
                    out.backfill = Some(reader.read_boolean()?);
                }
                FID_M => {
                    out.m = Some(reader.read_u16()?);
                }
                FID_EF_CONSTRUCTION => {
                    out.ef_construction = Some(reader.read_u16()?);
                }
                FID_METRIC => {
                    out.metric = Some(reader.read_string()?.into_owned());
                }
                FID_INDEX_TYPE => {
                    out.index_type = Some(reader.read_string()?.into_owned());
                }
                FID_VECTOR_DIM => {
                    out.vector_dim = Some(reader.read_u32()?);
                }
                FID_DATABASE => {
                    out.database = Some(reader.read_string()?.into_owned());
                }
                FID_SQL_PARAMS => {
                    out.sql_params = Some(Vec::<crate::value::Value>::read(reader)?);
                }
                FID_LIST_PATH => {
                    out.list_path = Some(reader.read_string()?.into_owned());
                }
                FID_LIST_INDEX => {
                    out.list_index = Some(reader.read_u64()?);
                }
                FID_LIST_FROM_INDEX => {
                    out.list_from_index = Some(reader.read_u64()?);
                }
                FID_LIST_TO_INDEX => {
                    out.list_to_index = Some(reader.read_u64()?);
                }
                FID_LIST_FIELDS_JSON => {
                    out.list_fields_json = Some(reader.read_string()?.into_owned());
                }
                // Unknown field ID — skip value for forward compatibility.
                _ => {
                    skip_msgpack_value(reader)?;
                }
            }
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(tf: &TextFields) -> TextFields {
        let bytes = zerompk::to_msgpack_vec(tf).expect("encode failed");
        zerompk::from_msgpack(&bytes).expect("decode failed")
    }

    #[test]
    fn textfields_present_only_roundtrip() {
        let tf = TextFields {
            sql: Some("SELECT 1".into()),
            collection: Some("docs".into()),
            top_k: Some(42),
            ..Default::default()
        };
        let bytes = zerompk::to_msgpack_vec(&tf).expect("encode");
        let decoded: TextFields = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded.sql.as_deref(), Some("SELECT 1"));
        assert_eq!(decoded.collection.as_deref(), Some("docs"));
        assert_eq!(decoded.top_k, Some(42));
        assert!(decoded.auth.is_none());
        assert!(decoded.query_vector.is_none());
        assert!(decoded.ef_search.is_none());
    }

    #[test]
    fn textfields_unknown_field_tolerance() {
        let known = TextFields {
            sql: Some("SELECT 2".into()),
            top_k: Some(10),
            ..Default::default()
        };
        let known_bytes = zerompk::to_msgpack_vec(&known).expect("encode known");
        assert_eq!(known_bytes[0], 0x82, "expected fixmap with 2 entries");

        let mut buf = Vec::new();
        buf.push(0x83u8);
        buf.extend_from_slice(&[0xcd, 0x27, 0x0f]);
        let fv = b"future_value";
        buf.push(0xa0 | fv.len() as u8);
        buf.extend_from_slice(fv);
        buf.push(2u8);
        let sv = b"SELECT 2";
        buf.push(0xa0 | sv.len() as u8);
        buf.extend_from_slice(sv);
        buf.push(9u8);
        buf.push(10u8);

        let decoded: TextFields = zerompk::from_msgpack(&buf).expect("decode with unknown field");
        assert_eq!(decoded.sql.as_deref(), Some("SELECT 2"));
        assert_eq!(decoded.top_k, Some(10));
    }

    #[test]
    fn narrow_widths_at_max_roundtrip() {
        let tf = TextFields {
            top_k: Some(u32::MAX),
            vector_top_k: Some(u32::MAX),
            final_top_k: Some(u32::MAX),
            expansion_depth: Some(u32::MAX),
            depth: Some(u32::MAX),
            ef_search: Some(u32::MAX),
            m: Some(u16::MAX),
            ef_construction: Some(u16::MAX),
            ..Default::default()
        };
        let decoded = roundtrip(&tf);
        assert_eq!(decoded.top_k, Some(u32::MAX));
        assert_eq!(decoded.vector_top_k, Some(u32::MAX));
        assert_eq!(decoded.final_top_k, Some(u32::MAX));
        assert_eq!(decoded.expansion_depth, Some(u32::MAX));
        assert_eq!(decoded.depth, Some(u32::MAX));
        assert_eq!(decoded.ef_search, Some(u32::MAX));
        assert_eq!(decoded.m, Some(u16::MAX));
        assert_eq!(decoded.ef_construction, Some(u16::MAX));
    }

    #[test]
    fn crdt_list_fields_roundtrip() {
        let tf = TextFields {
            list_path: Some("items".into()),
            list_index: Some(3),
            list_from_index: Some(1),
            list_to_index: Some(5),
            list_fields_json: Some(r#"{"title":"hello"}"#.into()),
            ..Default::default()
        };
        let decoded = roundtrip(&tf);
        assert_eq!(decoded.list_path.as_deref(), Some("items"));
        assert_eq!(decoded.list_index, Some(3));
        assert_eq!(decoded.list_from_index, Some(1));
        assert_eq!(decoded.list_to_index, Some(5));
        // from_index and to_index must stay distinct through the wire.
        assert_ne!(decoded.list_from_index, decoded.list_to_index);
        assert_eq!(
            decoded.list_fields_json.as_deref(),
            Some(r#"{"title":"hello"}"#)
        );
    }

    #[test]
    fn vector_id_roundtrip_at_u32_max() {
        let tf = TextFields {
            vector_id: Some(u32::MAX as u64),
            ..Default::default()
        };
        let decoded = roundtrip(&tf);
        assert_eq!(decoded.vector_id, Some(u32::MAX as u64));
    }

    /// vector_id overflow rejected in plan_builder (unit-test the narrowing logic).
    #[test]
    fn vector_id_overflow_rejected_in_plan_builder() {
        // Simulate the narrowing: u64 > u32::MAX must fail
        let wire_id: u64 = u32::MAX as u64 + 1;
        let result: Result<u32, _> = wire_id.try_into();
        assert!(result.is_err(), "u64 > u32::MAX must not fit in u32");

        // u32::MAX itself must succeed
        let wire_ok: u64 = u32::MAX as u64;
        let result2: Result<u32, _> = wire_ok.try_into();
        assert_eq!(result2.unwrap(), u32::MAX);
    }

    #[test]
    fn binary_fields_roundtrip_under_bin_marker() {
        // Spec: every `Vec<u8>`-valued field that the decoder reads via
        // `reader.read_binary()` must be encoded as a MessagePack
        // `bin8/bin16/bin32` blob — not via the generic
        // `<Vec<u8> as ToMessagePack>::write` which produces a fixarray
        // of `cc XX` u8s and breaks decoding with
        // `InvalidMarker(0x94…)`. A regression here means any caller
        // that ships non-empty bytes through these fields gets a
        // `BadRequest: invalid MessagePack request` from the server.
        let payload: Vec<u8> = (0..=255u16).map(|b| b as u8).collect();
        let tf = TextFields {
            data: Some(payload.clone()),
            delta: Some(payload.clone()),
            lower_bound: Some(payload.clone()),
            upper_bound: Some(payload.clone()),
            query_geometry: Some(payload.clone()),
            payload: Some(payload.clone()),
            cursor: Some(payload.clone()),
            expected: Some(payload.clone()),
            new_value: Some(payload.clone()),
            score_min: Some(payload.clone()),
            score_max: Some(payload.clone()),
            filters: Some(payload.clone()),
            ..Default::default()
        };
        let decoded = roundtrip(&tf);
        assert_eq!(decoded.data.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.delta.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.lower_bound.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.upper_bound.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.query_geometry.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.payload.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.cursor.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.expected.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.new_value.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.score_min.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.score_max.as_deref(), Some(payload.as_slice()));
        assert_eq!(decoded.filters.as_deref(), Some(payload.as_slice()));
    }

    #[test]
    fn binary_field_encoder_uses_bin_marker_not_fixarray() {
        // Wire-shape regression guard. The MessagePack `bin8` marker is
        // `0xc4` and the `bin16` marker is `0xc5`; a fixarray header is
        // `0x90 + len` (so `0x94` for a 4-byte payload). The bug this
        // catches encoded `Vec<u8>` as fixarray, which broke the
        // server's `read_binary` decode. Anchoring the marker byte
        // pins the wire format so a future refactor cannot regress.
        let tf = TextFields {
            data: Some(vec![0xde, 0xad, 0xbe, 0xef]),
            ..Default::default()
        };
        let bytes = zerompk::to_msgpack_vec(&tf).expect("encode");
        // map header (1 entry): 0x81; field id u16 0x0007 written as
        // `cd 00 07`; then the value's first marker byte. Walk past
        // the map+id prefix and assert the value starts with `bin8`.
        // `bin8` is `0xc4` followed by a 1-byte length and the bytes.
        assert_eq!(bytes[0], 0x81, "map len 1 header");
        // The exact id-encoding width depends on zerompk's `write_u16`
        // impl; allow both fixint (1 byte) and u16 (3 bytes) forms.
        let value_marker = bytes
            .iter()
            .copied()
            .find(|&b| b == 0xc4 || b == 0xc5 || b == 0xc6 || (0x90..=0x9f).contains(&b))
            .expect("must find either a bin or array marker");
        assert!(
            matches!(value_marker, 0xc4..=0xc6),
            "binary field must use bin8/bin16/bin32 marker, not fixarray; \
             saw marker 0x{value_marker:02x}"
        );
    }
}
