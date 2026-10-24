// SPDX-License-Identifier: BUSL-1.1

//! Multi-provider JWKS registry: routes JWT tokens to the correct provider,
//! fetches keys on demand, and validates signatures.
//!
//! All three public entry points (`validate`, `validate_with_claims`,
//! `validate_with_catalog_provider`) share the same token-decoding,
//! signature-verification, and time-claim-validation pipeline; they differ
//! only in how the verification key is resolved. The shared pipeline lives
//! in [`JwksRegistry::decode_unverified`] and
//! [`JwksRegistry::verify_signature_and_time`] (in `pipeline`).
//!
//! Module layout:
//! - `state` — the `JwksRegistry` struct + `DecodedToken`, the fields and
//!   type every other file in this package builds on.
//! - `init` — construction: fetch-all-providers-on-startup + refresh task.
//! - `pipeline` — shared token decode + signature/time verification.
//! - `routing` — match a token's `iss`/`aud` to a configured provider.
//! - `refetch` — verification-key resolution, including rate-limited
//!   on-demand re-fetch for an unknown `kid`.
//! - `validate` — the three public entry points + the claim-policy gate.
//! - `claims` — post-verification claim checks + identity construction.
//! - `cache_identity` — collision-free cache keys for static/catalog providers.
//! - `catalog` — register and unregister catalog OIDC providers.
//! - `targets` — the endpoints the periodic refresh task fetches.
//! - `header` — JWT header (first token segment) parsing.
//! - `verified` — `VerifiedJwtClaims`, the opaque post-verification proof.

pub mod cache_identity;
pub mod catalog;
pub mod claims;
pub mod header;
pub mod init;
pub mod pipeline;
pub mod refetch;
pub mod routing;
pub mod state;
pub mod targets;
pub mod validate;
pub mod verified;

pub use state::JwksRegistry;
pub use verified::VerifiedJwtClaims;
