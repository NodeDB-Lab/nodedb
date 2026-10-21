// SPDX-License-Identifier: BUSL-1.1

//! `[egress]` and `[credential_encryption]`: what an outbound request of the
//! server may reach, and the key `COPY` source credentials are sealed under.
//!
//! One `[egress]` policy governs every outbound request: remote `COPY`
//! sources, JWKS fetches, change-stream and alert webhooks, SIEM export,
//! OTLP trace and metrics export, and the Vault key provider. A URL the
//! operator configures (SIEM, OTLP, Vault) needs its host on `allow_hosts`
//! when it is plain `http://` or resolves to a private address.
//!
//! ```toml
//! [egress]
//! # Every relaxation names hosts. http:// is allowed only for these hosts,
//! # and only these hosts may resolve into allow_cidrs.
//! allow_http  = false
//! allow_hosts = ["mirror.internal", "keycloak.internal", "otel-collector"]
//! allow_cidrs = ["10.42.0.0/16"]
//!
//! [credential_encryption]
//! # 32 bytes, mode 0600, owned by the server's user. Every node of a
//! # cluster needs the same file. Generate with:
//! #   head -c 32 /dev/urandom > /etc/nodedb/keys/credential.key
//! key_path = "/etc/nodedb/keys/credential.key"
//! ```
//!
//! With no `[egress]` section the policy is strict: `https://` only, DNS
//! names only, and every resolved address must be global unicast. With no
//! `[credential_encryption]` section, `CREATE CREDENTIAL` is refused.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::control::security::egress::{EgressError, EgressPolicy};

/// The `[egress]` allow-list of every outbound request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EgressSettings {
    /// Allow `http://` for the hosts in `allow_hosts`.
    #[serde(default)]
    pub allow_http: bool,
    /// Hosts that may use `http://` when `allow_http` is set, and may
    /// resolve into `allow_cidrs`.
    #[serde(default)]
    pub allow_hosts: Vec<String>,
    /// Address blocks `allow_hosts` may resolve into beyond global unicast,
    /// such as `"10.42.0.0/16"` or `"127.0.0.0/8"` for a local mirror.
    #[serde(default)]
    pub allow_cidrs: Vec<String>,
}

impl EgressSettings {
    /// The policy these settings describe.
    pub fn policy(&self) -> Result<EgressPolicy, EgressError> {
        EgressPolicy::from_parts(self.allow_http, &self.allow_hosts, &self.allow_cidrs)
    }

    /// The policy, or a config error that names the section.
    pub fn checked_policy(&self) -> crate::Result<EgressPolicy> {
        self.policy().map_err(|e| crate::Error::Config {
            detail: format!("[egress] is invalid: {e}"),
        })
    }
}

/// The `[credential_encryption]` key file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialEncryptionSettings {
    /// Path to the 32-byte key that seals credential secrets.
    pub key_path: PathBuf,
}

/// Check the `[egress]` allow-list at startup.
pub(super) fn validate_egress(settings: &EgressSettings) -> crate::Result<()> {
    settings.checked_policy().map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_section_is_strict_and_a_bad_cidr_is_refused() {
        assert_eq!(
            EgressSettings::default().policy(),
            Ok(EgressPolicy::strict())
        );
        let bad = EgressSettings {
            allow_cidrs: vec!["10.0.0.0/99".into()],
            ..Default::default()
        };
        assert!(validate_egress(&bad).is_err());
        let toml =
            "allow_http = true\nallow_hosts = [\"localhost\"]\nallow_cidrs = [\"127.0.0.0/8\"]\n";
        let parsed: EgressSettings = toml::from_str(toml).expect("parses");
        assert!(parsed.policy().expect("valid").allow_http);
    }
}
