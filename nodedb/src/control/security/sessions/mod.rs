// SPDX-License-Identifier: BUSL-1.1

//! Active session tracking: registry, cap enforcement, revocation, idle sweep.
//!
//! ## Lifecycle
//!
//! `SessionRegistry` is constructed once during server startup and shared
//! via `SharedState::session_registry`. Every long-lived authenticated
//! connection and stream (native, pgwire, WebSocket, RESP, sync, ILP, SSE and
//! NDJSON) holds a [`SessionBinding`] from `bind_session`. The binding registers
//! the session and its drop unregisters it. A plain HTTP request authenticates
//! per request and holds no binding.
//!
//! A kill stops a session at its next frame, chunk, or statement boundary. A
//! statement in flight finishes, because a statement is not safe to drop midway.
//! A streamed read ends at once, the way a client disconnect ends it.
//!
//! `spawn_idle_sweep_loop` MUST be called exactly once at server startup,
//! after `SharedState` is fully built. It registers a Tokio task with the
//! shared `loop_registry` (so it drains on shutdown) and ticks every five
//! seconds, signalling `KillReason::IdleTimeout` or `KillReason::TokenExpired`
//! to sessions whose per-database idle cap or OIDC token expiry has elapsed.
//! Calling it more than once would spawn duplicate sweepers — there is no
//! internal idempotency guard, so the call site (currently
//! `nodedb::bootstrap::background_loops`) is responsible for the
//! exactly-once invariant.
//!
//! Callers never invoke `unregister` from the sweep path; the sweep loop
//! only sends on `kill_tx`, and the session's own drop path removes the
//! row.

pub mod binding;
pub mod idle_sweep;
pub mod kill;
pub mod registry;
pub mod stream_end;

pub use binding::{
    SessionBindRequest, SessionBinding, bind_session, kill_signal_ended, session_ended,
};
pub use idle_sweep::spawn_idle_sweep_loop;
pub use registry::{
    AUTH_METHOD_OIDC, KillReason, SessionCapExceeded, SessionInfo, SessionParams, SessionRegistry,
    SweepEntry,
};
pub use stream_end::stream_until_ended;
