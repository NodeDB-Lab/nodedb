// SPDX-License-Identifier: Apache-2.0

//! `CREATE CREDENTIAL`: the secrets a remote `COPY` source authenticates
//! with. A secret never prints: its `Debug` form is redacted.

/// Secret text. `Debug` prints `<redacted>`.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretText(String);

impl SecretText {
    /// Wrap `text`.
    pub fn new(text: String) -> Self {
        Self(text)
    }

    /// The secret.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

/// What a credential holds, by `TYPE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialSpec {
    /// `TYPE 's3'`: AWS keys for `s3://` sources.
    S3 {
        access_key_id: SecretText,
        secret_access_key: SecretText,
        region: String,
        session_token: Option<SecretText>,
        /// An S3-compatible endpoint URL in place of AWS.
        endpoint: Option<String>,
    },
    /// `TYPE 'bearer'`: an `Authorization: Bearer` token, such as an
    /// Earthdata Login token.
    Bearer { token: SecretText },
    /// `TYPE 'sas'`: a shared access signature appended to the URL query.
    Sas { token: SecretText },
}

impl CredentialSpec {
    /// The `TYPE` keyword.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::S3 { .. } => "s3",
            Self::Bearer { .. } => "bearer",
            Self::Sas { .. } => "sas",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_the_secret() {
        let spec = CredentialSpec::Bearer {
            token: SecretText::new("tok-123".into()),
        };
        let text = format!("{spec:?}");
        assert!(!text.contains("tok-123"), "{text}");
        assert!(text.contains("<redacted>"), "{text}");
        assert_eq!(spec.type_name(), "bearer");
    }
}
