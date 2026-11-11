// SPDX-License-Identifier: BUSL-1.1

//! The client error for a write the Data Plane refused.
//!
//! A refusal reaches a handler two ways: the staging gate's `Rejected`, and
//! an `Ok(Response)` whose status is `Error`. The dispatch funnel reports
//! the dispatch itself as a success and puts the verdict in the response, so
//! a handler that drops the response answers a refusal with a success tag.
//! Every neutral handler maps both routes here, so one verdict reaches the
//! client as one error.

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;

/// The client error for a refusal carrying `code`: the SQLSTATE and message
/// the SQL surfaces render, with the code and details a client classifies
/// by. The SQLSTATE alone does not name the collection.
pub(crate) fn coded_refusal(code: &ErrorCode) -> DdlError {
    let (_, sqlstate, message) = error_code_to_sqlstate(code);
    let public = nodedb_types::NodeDbError::from(crate::Error::DataPlane(code.clone()));
    DdlError::from_public(sqlstate, message, &public)
}

/// The client error for a refusal carrying `code`. `None` means the Data
/// Plane reported an error with no code. That is a bug in whatever produced
/// it, so it answers an internal error, never a success.
pub(crate) fn refusal_to_ddl(code: Option<&ErrorCode>) -> DdlError {
    match code {
        Some(code) => coded_refusal(code),
        None => DdlError::internal("unknown data plane error"),
    }
}

/// The verdict `response` carries: its refusal when its status is `Error`,
/// else `Ok`.
pub(crate) fn response_verdict(response: &Response) -> Result<(), DdlError> {
    if response.status != Status::Error {
        return Ok(());
    }
    Err(refusal_to_ddl(response.error_code.as_deref()))
}
