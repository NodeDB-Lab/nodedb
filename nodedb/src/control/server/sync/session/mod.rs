//! Sync session: handles one WebSocket connection from a NodeDB-Lite
//! client. Processes incoming frames (handshake, delta push, vector
//! clock sync, token refresh, ping/pong) and sends responses. Each
//! session is authenticated via JWT and scoped to a single tenant.
//!
//! The file split isolates:
//! - `state.rs` — `SyncSession` struct + lifecycle (`new`,
//!   `with_rate_limit`, `uptime_secs`, `idle_secs`).
//! - `handshake.rs` — `handle_handshake` + `check_fork_detection`.
//! - `delta.rs` — `handle_delta_push` (rate limit, CRC32C, RLS, dedup).
//! - `clock_ping.rs` — `handle_vector_clock_sync` + `handle_ping`.
//! - `token.rs` — `handle_token_refresh`.
//! - `dispatch.rs` — `process_frame` (match on `msg_type`, route).

pub mod clock_ping;
pub mod delta;
pub mod dispatch;
pub mod handshake;
pub mod state;
pub mod token;

#[cfg(test)]
mod tests;

pub use state::SyncSession;
