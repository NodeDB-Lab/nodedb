// SPDX-License-Identifier: BUSL-1.1

//! RESP error shape: a simple-error string with a typed prefix.

use super::gateway_map::GatewayErrorMap;
use super::remote_code::remote_code_to_resp_prefix;
use crate::Error;

impl GatewayErrorMap {
    /// Map a gateway error into a RESP simple-error string.
    ///
    /// Follows Redis error format: `ERR <message>` for generic errors, or
    /// a typed prefix (`WRONGTYPE`, `NOTFOUND`, etc.) where applicable.
    pub fn to_resp(err: &Error) -> String {
        match err {
            Error::NotLeader { leader_addr, .. } => {
                format!("MOVED 0 {leader_addr}")
            }
            Error::DeadlineExceeded { .. } => "TIMEOUT query deadline exceeded".into(),
            Error::CollectionNotFound { collection, .. } => {
                format!("NOTFOUND collection \"{collection}\" does not exist")
            }
            Error::RejectedAuthz { .. } => format!("NOPERM {}", err),
            Error::BadRequest { detail } | Error::PlanError { detail } => {
                format!("ERR {detail}")
            }
            Error::RejectedConstraint { detail, .. } => format!("CONSTRAINT {detail}"),
            Error::TypeMismatch { detail, .. } => format!("WRONGTYPE {detail}"),
            Error::RetryableSchemaChanged { descriptor } => {
                format!("ERR schema changed ({descriptor}); please retry")
            }
            Error::RemoteTyped { code, message } => {
                format!("{} {message}", remote_code_to_resp_prefix(*code))
            }
            Error::DataPlane(_) => {
                let public = crate::error_classify::classify(err);
                format!(
                    "{} {}",
                    remote_code_to_resp_prefix(public.code()),
                    public.message()
                )
            }
            _ => format!("ERR {err}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_fixtures::{authz, deadline, internal, not_found, not_leader};
    use super::*;

    #[test]
    fn resp_not_leader() {
        let msg = GatewayErrorMap::to_resp(&not_leader());
        assert!(msg.starts_with("MOVED"));
    }

    #[test]
    fn resp_deadline() {
        let msg = GatewayErrorMap::to_resp(&deadline());
        assert!(msg.starts_with("TIMEOUT"));
    }

    #[test]
    fn resp_not_found() {
        let msg = GatewayErrorMap::to_resp(&not_found());
        assert!(msg.starts_with("NOTFOUND"));
    }

    #[test]
    fn resp_authz() {
        let msg = GatewayErrorMap::to_resp(&authz());
        assert!(msg.starts_with("NOPERM"));
    }

    #[test]
    fn resp_internal() {
        let msg = GatewayErrorMap::to_resp(&internal());
        assert!(msg.starts_with("ERR"));
    }

    #[test]
    fn resp_type_mismatch_is_wrongtype() {
        let err = Error::TypeMismatch {
            collection: "c".into(),
            key: "k".into(),
            detail: "key holds a bare value, not a hash".into(),
        };
        let msg = GatewayErrorMap::to_resp(&err);
        assert!(msg.starts_with("WRONGTYPE "), "{msg}");
    }

    #[test]
    fn to_resp_remote_typed_is_wired_to_helper() {
        use nodedb_types::error::ErrorCode;
        let err = Error::RemoteTyped {
            code: ErrorCode::CONSTRAINT_VIOLATION,
            message: "unique key clash".into(),
        };
        let msg = GatewayErrorMap::to_resp(&err);
        assert_eq!(msg, "CONSTRAINT unique key clash");
    }
}
