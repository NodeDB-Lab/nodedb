// SPDX-License-Identifier: BUSL-1.1

//! Why an OTLP receiver refused a request's authentication.

/// A refused OTLP request. The `Display` text is the body the client receives.
#[derive(Debug, thiserror::Error)]
pub enum OtelAuthError {
    #[error("missing Authorization: Bearer <token> header")]
    MissingHeader,

    #[error("invalid authorization header encoding")]
    HeaderEncoding,

    #[error("invalid authorization header")]
    MalformedHeader,

    #[error("invalid bearer token")]
    InvalidToken,

    /// The TLS policy refused the request's transport.
    #[error(transparent)]
    Transport(Box<crate::Error>),
}

impl From<OtelAuthError> for crate::Error {
    fn from(error: OtelAuthError) -> Self {
        match error {
            OtelAuthError::Transport(cause) => *cause,
            refusal @ (OtelAuthError::MissingHeader
            | OtelAuthError::HeaderEncoding
            | OtelAuthError::MalformedHeader
            | OtelAuthError::InvalidToken) => crate::Error::BadRequest {
                detail: format!("OTLP authentication refused: {refusal}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_client_text_stays_the_same() {
        assert_eq!(
            OtelAuthError::MissingHeader.to_string(),
            "missing Authorization: Bearer <token> header"
        );
        assert_eq!(
            OtelAuthError::InvalidToken.to_string(),
            "invalid bearer token"
        );
    }

    #[test]
    fn a_transport_refusal_keeps_its_cause_in_the_central_error() {
        let refusal = OtelAuthError::Transport(Box::new(crate::Error::BadRequest {
            detail: "cleartext refused".into(),
        }));
        assert_eq!(
            refusal.to_string(),
            crate::Error::BadRequest {
                detail: "cleartext refused".into(),
            }
            .to_string()
        );
        assert!(matches!(
            crate::Error::from(refusal),
            crate::Error::BadRequest { .. }
        ));
    }
}
