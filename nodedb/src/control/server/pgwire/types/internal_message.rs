// SPDX-License-Identifier: BUSL-1.1

//! Client-facing message hygiene for rendered failures.
//!
//! An internal (`XX000`-class) failure describes server state — a manifest
//! path, an invariant, an unmapped bridge fault. That detail belongs in the
//! server log; the client receives a stable summary, because the class and
//! the numeric code already say everything the client can act on. Every other
//! class keeps its message: it names what the caller must change.

use nodedb_types::error::{ErrorCode, sqlstate};

use super::error_map::numeric_code_to_sqlstate;

/// The client-facing text for a failure rendered at a protocol edge.
///
/// Codes the mapper sends to `XX000` (`INTERNAL`, `BRIDGE`, `DISPATCH`)
/// become one generic summary and the detail is logged with its code. Any
/// other class passes through unchanged.
pub fn shaping_error_message(code: ErrorCode, message: impl Into<String>) -> String {
    let message = message.into();
    if numeric_code_to_sqlstate(code) == sqlstate::INTERNAL_ERROR {
        tracing::error!(%code, detail = %message, "internal failure rendered to a client");
        "internal error while shaping the response".to_owned()
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_class_detail_is_replaced() {
        let message = shaping_error_message(
            ErrorCode::INTERNAL,
            "manifest at /var/lib/nodedb/segment-42 is corrupt",
        );

        assert_eq!(message, "internal error while shaping the response");
        assert!(!message.contains("segment-42"));
    }

    #[test]
    fn client_class_message_passes_through() {
        let message = shaping_error_message(
            ErrorCode::SERIALIZATION,
            "cell \"ts\" holds an integer where a timestamp is required",
        );

        assert!(message.contains("timestamp"));
    }
}
