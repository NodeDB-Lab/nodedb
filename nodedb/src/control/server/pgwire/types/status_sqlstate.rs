// SPDX-License-Identifier: BUSL-1.1

//! Pgwire notices, and the SQLSTATE of a Data Plane response status.

use nodedb_types::error::sqlstate;

use crate::bridge::envelope::{ErrorCode, Status};

/// Create a notice response (WARNING level).
pub fn notice_warning(message: &str) -> pgwire::messages::response::NoticeResponse {
    pgwire::messages::response::NoticeResponse::from(pgwire::error::ErrorInfo::new(
        "WARNING".to_owned(),
        sqlstate::WARNING.to_owned(),
        message.to_owned(),
    ))
}

/// Map a Data Plane response status + error code to a SQLSTATE triple.
pub fn response_status_to_sqlstate(
    status: Status,
    error_code: Option<&ErrorCode>,
) -> Option<(&'static str, &'static str, String)> {
    match status {
        Status::Ok | Status::Partial => None,
        Status::Error => {
            if let Some(code) = error_code {
                Some(crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(code))
            } else {
                Some((
                    "ERROR",
                    sqlstate::INTERNAL_ERROR,
                    "unknown data plane error".into(),
                ))
            }
        }
    }
}
