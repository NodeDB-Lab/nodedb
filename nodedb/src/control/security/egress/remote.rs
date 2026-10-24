// SPDX-License-Identifier: BUSL-1.1

//! The server's `[egress]` policy, the outbound client built on it, and the
//! key remote `COPY` source credentials are sealed under.

use std::sync::Arc;

use super::client::outbound_client;
use super::policy::EgressPolicy;
use crate::control::security::source_credential::CredentialKek;

/// The `[egress]` policy with the shared outbound client built on it, and
/// the `[credential_encryption]` key. The client and the policy are paired
/// at construction, so no request goes out under another policy.
#[derive(Debug, Clone)]
pub struct RemoteSources {
    egress: Arc<EgressPolicy>,
    http: Arc<reqwest::Client>,
    /// The key `CREATE CREDENTIAL` seals secrets under. `None` refuses
    /// `CREATE CREDENTIAL` and every `COPY ... CREDENTIAL`.
    pub credential_kek: Option<Arc<CredentialKek>>,
}

impl RemoteSources {
    /// `egress` with its outbound client, and no credential key.
    pub fn new(egress: Arc<EgressPolicy>) -> Result<Self, reqwest::Error> {
        let http = Arc::new(outbound_client(Arc::clone(&egress))?);
        Ok(Self {
            egress,
            http,
            credential_kek: None,
        })
    }

    /// What an outbound request may reach. Strict by default.
    pub fn egress(&self) -> &Arc<EgressPolicy> {
        &self.egress
    }

    /// The outbound client built on [`Self::egress`]. Alert webhooks, SIEM
    /// export and OTLP export share it. Each sender checks its URL with
    /// [`EgressPolicy::check_url`] first.
    pub fn http(&self) -> &Arc<reqwest::Client> {
        &self.http
    }
}
