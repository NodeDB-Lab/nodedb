//! Helpers for `array_cluster_exec`: agg-partial finalisation, error mappers,
//! and response-opcode → `VShardMessageType` lookup for the local-dispatch
//! fast path.

use nodedb_cluster::distributed_array::merge::ArrayAggPartial;
use nodedb_cluster::wire::VShardMessageType;

use crate::Error;

/// Raw scalar value for aggregate row map entries.
///
/// Writes as an untagged msgpack scalar — same wire shape as `AggCell` in
/// `data::executor::dispatch::array::aggregate` — so that
/// `decode_payload_to_json` produces clean JSON numbers and nulls.
pub(super) enum AggValue {
    Float(f64),
    Int(i64),
    Null,
}

impl zerompk::ToMessagePack for AggValue {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            AggValue::Float(f) => writer.write_f64(*f),
            AggValue::Int(i) => writer.write_i64(*i),
            AggValue::Null => writer.write_nil(),
        }
    }
}

/// Finalize merged `ArrayAggPartial`s into the same msgpack-map structure
/// the local `ArrayOp::Aggregate` path produces via `encode_agg_rows`.
///
/// For scalar (no group-by), returns a single `[{"result": f64}]`.
/// For group-by, returns one `{"group": i64, "result": f64}` per partial.
/// The map keys and value types match the local `AggCell` encoding so that
/// `decode_payload_to_json` produces identical JSON on both paths.
pub(super) fn finalize_agg_partials(
    partials: &[ArrayAggPartial],
    reducer: &crate::bridge::physical_plan::ArrayReducer,
    group_by_dim: i32,
) -> Vec<std::collections::BTreeMap<String, AggValue>> {
    use crate::bridge::physical_plan::ArrayReducer;

    let finalize = |p: &ArrayAggPartial| -> AggValue {
        if p.count == 0 {
            return AggValue::Null;
        }
        let v = match reducer {
            ArrayReducer::Sum => p.sum,
            ArrayReducer::Count => p.count as f64,
            ArrayReducer::Min => p.min,
            ArrayReducer::Max => p.max,
            ArrayReducer::Mean => p.welford_mean,
        };
        AggValue::Float(v)
    };

    let is_grouped = group_by_dim >= 0;

    partials
        .iter()
        .map(|p| {
            let mut row = std::collections::BTreeMap::new();
            if is_grouped {
                row.insert("group".to_string(), AggValue::Int(p.group_key));
            }
            row.insert("result".to_string(), finalize(p));
            row
        })
        .collect()
}

pub(super) fn cluster_err(e: nodedb_cluster::error::ClusterError) -> Error {
    Error::Internal {
        detail: format!("array cluster: {e}"),
    }
}

pub(super) fn encode_err(e: zerompk::Error) -> Error {
    Error::Serialization {
        format: "msgpack".into(),
        detail: format!("array cluster encode: {e}"),
    }
}

/// Map a numeric response opcode (81, 83, 85, 87, 89) to its `VShardMessageType`.
///
/// Used by the local-dispatch fast path to build the response envelope without
/// going through the QUIC transport layer.
pub(super) fn array_resp_msg_type(opcode: u16) -> Option<VShardMessageType> {
    match opcode {
        81 => Some(VShardMessageType::ArrayShardSliceResp),
        83 => Some(VShardMessageType::ArrayShardAggResp),
        85 => Some(VShardMessageType::ArrayShardPutResp),
        87 => Some(VShardMessageType::ArrayShardDeleteResp),
        89 => Some(VShardMessageType::ArrayShardSurrogateBitmapResp),
        _ => None,
    }
}
