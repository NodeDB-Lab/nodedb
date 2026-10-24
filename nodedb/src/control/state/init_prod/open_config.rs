// SPDX-License-Identifier: BUSL-1.1

//! What `SharedState::open` reads from the server configuration.

use std::sync::Arc;

use crate::config::auth::AuthConfig;
use crate::control::security::egress::EgressPolicy;

/// The configuration sections `SharedState::open` builds from.
pub struct OpenConfig<'a> {
    /// The `[auth]` section.
    pub auth: &'a AuthConfig,
    /// The `[egress]` policy. The shared outbound client, the SIEM exporter
    /// and every remote source go through it.
    pub egress: Arc<EgressPolicy>,
}

impl<'a> OpenConfig<'a> {
    /// `auth` under the strict egress policy, the policy of a config with no
    /// `[egress]` section.
    pub fn strict(auth: &'a AuthConfig) -> Self {
        Self {
            auth,
            egress: Arc::new(EgressPolicy::strict()),
        }
    }
}
