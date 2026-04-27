//! Helper to build outbound [`ArrayRejectMsg`] frames.

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};

/// Build an [`ArrayRejectMsg`] for a rejected inbound op.
///
/// `op_hlc` is the HLC of the offending op. `reason` is a structured code.
/// `detail` is a human-readable explanation.
pub fn build_reject(
    array: &str,
    op_hlc: Hlc,
    reason: ArrayRejectReason,
    detail: impl Into<String>,
) -> ArrayRejectMsg {
    ArrayRejectMsg {
        array: array.to_owned(),
        op_hlc_bytes: op_hlc.to_bytes(),
        reason,
        detail: detail.into(),
    }
}
