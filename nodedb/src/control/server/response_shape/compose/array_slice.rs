// SPDX-License-Identifier: BUSL-1.1

//! Shaping for an `ArrayOp::Slice` response.

use crate::data::executor::response_codec::{ArraySliceResponse, decode_payload_value};

use super::super::redaction::RedactionCtx;
use super::super::types::ShapedRows;
use super::kernel::{empty_shaped, shape_decoded_rows};

/// NOTICE text for an `AS OF SYSTEM TIME` cutoff older than the oldest
/// retained tile version. This is the canonical definition, surfaced to
/// every protocol via [`ShapedRows::notice`].
const TRUNCATED_BEFORE_HORIZON_NOTICE: &str = "AS OF SYSTEM TIME cutoff is older than the oldest retained tile version; \
     results may be incomplete";

/// Shape an `ArrayOp::Slice` response: decode the `ArraySliceResponse`
/// envelope (falling back to a plain payload decode for legacy shapes),
/// unwrap the row envelope, and surface `truncated_before_horizon` as a
/// notice.
///
/// Array slices never carry a SELECT-list projection, so `shape_decoded_rows`
/// is always called with a `None` projection and no sequence access here —
/// but redaction still applies to the cells. A payload that decodes to no
/// value shapes as an empty result set.
pub(super) fn shape_array_slice(
    payload: &[u8],
    redaction: Option<RedactionCtx<'_>>,
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
        Ok(value) => shape_decoded_rows(value, None, redaction, None)?,
        Err(_) => empty_shaped(),
    };
    shaped.notice = notice;
    Ok(shaped)
}
