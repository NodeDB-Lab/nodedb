// SPDX-License-Identifier: BUSL-1.1

//! Authentication helpers shared across protocol handlers.
//!
//! - [`identity`] — resolve an identity from a TLS cert, an API key, or
//!   trust mode.
//! - [`bearer_jwt`] — verify a bearer JWT against the configured `[auth.jwt]`
//!   providers and the stateful JWT policy.
//! - [`native`] — the native-protocol JSON `authenticate` dispatcher and
//!   the constant-time failure floor.
//! - [`context`] — build and enrich `AuthContext` from an identity, plus
//!   per-query `ON DENY` extraction.
//! - [`guards`] — post-identity blacklist, transport-security, and rate-limit
//!   checks.
//! - [`admission`] — the composed entry point over `guards`: internal-service
//!   exemption, blacklist, account status, and rate limit, in order.

pub mod admission;
pub mod admitted_database;
pub mod bearer_jwt;
pub mod context;
pub mod guards;
pub mod identity;
pub mod native;

pub use admission::{check_blacklist_and_status, check_request_admission};
pub use bearer_jwt::authenticate_bearer_jwt;
pub use context::{
    apply_per_query_on_deny, build_auth_context, enrich_auth_context_with_scopes,
    extract_and_apply_on_deny, extract_on_deny, session_on_deny_override,
};
pub use guards::{check_blacklist, check_rate_limit, check_transport_security};
pub use identity::{
    configured_trust_identity, resolve_certificate_identity, trust_identity,
    verify_api_key_identity,
};
pub use native::{AUTH_FLOOR, authenticate};
