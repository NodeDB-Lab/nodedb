// SPDX-License-Identifier: BUSL-1.1

//! The plaintext of a credential: what a sealed credential opens to. It
//! never leaves the server: no SHOW, describe or error prints it, and its
//! `Debug` form is redacted.

use zeroize::Zeroize;

/// The secret fields of one credential, by type.
#[derive(Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum CredentialSecret {
    /// AWS keys for `s3://` sources.
    S3 {
        access_key_id: String,
        secret_access_key: String,
        region: String,
        session_token: Option<String>,
        /// An S3-compatible endpoint in place of AWS.
        endpoint: Option<String>,
    },
    /// An `Authorization: Bearer` token.
    Bearer { token: String },
    /// A shared access signature appended to the URL query.
    Sas { token: String },
}

impl CredentialSecret {
    /// The `TYPE` keyword.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::S3 { .. } => "s3",
            Self::Bearer { .. } => "bearer",
            Self::Sas { .. } => "sas",
        }
    }
}

impl std::fmt::Debug for CredentialSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CredentialSecret::{}(<redacted>)", self.type_name())
    }
}

impl Drop for CredentialSecret {
    fn drop(&mut self) {
        match self {
            Self::S3 {
                access_key_id,
                secret_access_key,
                session_token,
                ..
            } => {
                access_key_id.zeroize();
                secret_access_key.zeroize();
                if let Some(token) = session_token {
                    token.zeroize();
                }
            }
            Self::Bearer { token } | Self::Sas { token } => token.zeroize(),
        }
    }
}
