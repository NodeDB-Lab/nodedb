// SPDX-License-Identifier: BUSL-1.1

//! The `JwksRegistry` struct and the intermediate `DecodedToken` shared by
//! every other file in this package.

use std::sync::Arc;

use crate::config::auth::{JwtAuthConfig, JwtProviderConfig};
use crate::control::security::egress::EgressPolicy;
use crate::control::security::jwks::cache::JwksCache;
use crate::control::security::jwt::JwtClaims;

use super::header::JwtHeader;
use super::targets::RefreshTargets;

/// Multi-provider JWKS registry.
///
/// Manages providers, caches keys, and validates JWT tokens.
/// Lives on the Control Plane (Send + Sync).
pub struct JwksRegistry {
    pub(super) providers: Vec<JwtProviderConfig>,
    pub(super) cache: Arc<JwksCache>,
    pub(super) config: JwtAuthConfig,
    pub(super) policy: Arc<EgressPolicy>,
    /// The endpoints the refresh task fetches: static providers and every
    /// catalog provider registered on this node.
    pub(super) targets: Arc<parking_lot::RwLock<RefreshTargets>>,
    /// Background refresh task handle.
    pub(super) _refresh_handle: Option<tokio::task::JoinHandle<()>>,
}

/// JWT broken into its three base64url-encoded parts plus the decoded
/// header and payload. Produced by [`JwksRegistry::decode_unverified`].
///
/// The `parts` slices borrow from the original token string and are reused
/// when reconstructing the signing input for signature verification — no
/// re-split, no re-decode.
pub(super) struct DecodedToken<'a> {
    pub(super) parts: [&'a str; 3],
    pub(super) header: JwtHeader,
    pub(super) claims: JwtClaims,
}
