//! Opaque session handle store: maps CSPRNG handles to cached `AuthContext`.
//!
//! Allows connection poolers and stateless clients to authenticate once via
//! `POST /api/auth/session`, receive a handle, then attach it to pgwire
//! connections via `SET LOCAL nodedb.auth_session = '<handle>'`.
//!
//! The handle resolves to a full `AuthContext` without re-validating the JWT
//! on every query. Handles expire after a configurable TTL.
//!
//! ## Hygiene layers
//!
//! - **Client fingerprint binding** — each handle captures the creating
//!   client's `(tenant_id, ip)` at `create()`. On `resolve()`, the caller's
//!   current fingerprint must match under the configured mode (strict /
//!   subnet / disabled). Mismatch → `None` + `SessionHandleFingerprintMismatch`
//!   audit event. Defends against handle theft across origins.
//! - **Per-connection rate limit** — sliding-window attempt counter on the
//!   resolve path, so a misconfigured client or enumeration probe can't
//!   hammer the resolver silently. Exceed → fatal pgwire error + connection
//!   close (enforced at the pgwire `SET LOCAL` handler).
//! - **Miss counter + spike audit** — tenant-tagged miss counter for
//!   Grafana; `SessionHandleResolveMissSpike` audit event when miss rate
//!   on a single connection crosses the threshold. Fingerprint-rejected
//!   resolutions also count as misses — a stolen-handle probe must not be
//!   silent just because the handle exists.

pub mod fingerprint;
pub mod miss_tracker;
pub mod rate_limit;
pub mod store;

pub use fingerprint::{ClientFingerprint, FingerprintMode};
pub use rate_limit::RateLimitDecision;
pub use store::{ResolveOutcome, SessionHandleStore};
