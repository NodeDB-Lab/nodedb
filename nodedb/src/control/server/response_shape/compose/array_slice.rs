// SPDX-License-Identifier: BUSL-1.1

//! Shaping for an `ArrayOp::Slice` response.

use crate::control::sequence::SequenceAccess;
use crate::data::executor::response_codec::{ArraySliceResponse, decode_payload_value};

use super::super::redaction::RedactionCtx;
use super::super::schema::OutputSchema;
use super::super::types::ShapedRows;
use super::kernel::{empty_shaped, shape_decoded_rows};

/// NOTICE text for an `AS OF SYSTEM TIME` cutoff older than the oldest
/// retained tile version. This is the canonical definition, surfaced to
/// every protocol via [`ShapedRows::notice`].
pub(crate) const TRUNCATED_BEFORE_HORIZON_NOTICE: &str = "AS OF SYSTEM TIME cutoff is older than the oldest retained tile version; \
     results may be incomplete";

/// Shape an `ArrayOp::Slice` response: decode the `ArraySliceResponse`
/// envelope (a cluster slice answers its rows bare), project the rows to
/// `projection`, and surface `truncated_before_horizon` as a notice.
///
/// Each row holds one field per dim and per attr, keyed by name. The
/// projection the planner announces for the slice's relation names those
/// fields in schema order with their types, so the client receives typed
/// columns in that order. Redaction applies to the cells. A payload that
/// decodes to no value shapes as an empty result set.
pub(super) fn shape_array_slice(
    payload: &[u8],
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
    sequences: Option<&dyn SequenceAccess>,
    crs_set: &nodedb_crs::CustomCrsSet,
) -> crate::Result<ShapedRows> {
    if payload.is_empty() {
        return Ok(empty_shaped());
    }
    let (rows, truncated) = if let Ok(resp) = zerompk::from_msgpack::<ArraySliceResponse>(payload) {
        (
            decode_payload_value(&resp.rows_msgpack),
            resp.truncated_before_horizon,
        )
    } else {
        (decode_payload_value(payload), false)
    };
    let notice = truncated.then(|| TRUNCATED_BEFORE_HORIZON_NOTICE.to_string());

    let mut shaped = match rows {
        Ok(value) => shape_decoded_rows(value, projection, redaction, sequences, crs_set)?,
        Err(_) => empty_shaped(),
    };
    shaped.notice = notice;
    Ok(shaped)
}
