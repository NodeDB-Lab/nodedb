// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the CONVERT COLLECTION family.

use super::super::super::result::DdlError;

/// Build a protocol-neutral [`DdlError`] with the given SQLSTATE and message.
///
/// `message` takes anything convertible to `String`, so a `format!` result
/// moves straight in without a borrow and a second allocation.
pub(super) fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}
