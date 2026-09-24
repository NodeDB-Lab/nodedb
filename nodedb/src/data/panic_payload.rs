// SPDX-License-Identifier: BUSL-1.1

//! Rendering a caught panic's payload for an error report.

/// Best-effort conversion of a panic payload to a human-readable string.
/// Tries the two common payload types (`&'static str` and `String`); falls
/// back to `"<non-string panic payload>"` for anything else.
pub(crate) fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}
