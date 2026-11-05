// SPDX-License-Identifier: BUSL-1.1

//! Idle-session sweep loop (Control Plane, Tokio).
//!
//! `idle_sweep_loop` runs on the Tokio runtime at a 5-second tick.  Each tick
//! it calls `sweep_once`, which:
//!
//! 1. Takes a lock-free snapshot of every session via `sweep_snapshot`.
//! 2. For each session, computes the earliest close deadline from:
//!    - The per-database idle timeout (from the session's cached
//!      `idle_timeout_secs`, which mirrors the `IdleTimeoutCache`).
//!    - The OIDC token expiry (`token_expiry_ms`), if set.
//!    - The global idle timeout stored in `SharedState::idle_timeout_secs`.
//! 3. If `now_ms >= deadline`, sends `KillReason::IdleTimeout` or
//!    `KillReason::TokenExpired` (whichever is earlier) to that session.
//!
//! The session's own drop path calls `unregister` — the sweep loop only signals
//! via the `kill_tx`; it never calls `unregister` itself.

use std::sync::Arc;
use std::time::Duration;

use crate::control::security::sessions::{KillReason, SessionRegistry};
use crate::control::security::time::now_ms;
use crate::control::state::SharedState;

/// Compute the next close deadline (milliseconds since epoch) for a session,
/// given:
/// - `token_exp_ms`: OIDC token expiry in ms (0 = no token expiry).
/// - `last_active_ms`: last-active timestamp in milliseconds.
/// - `idle_cap_secs`: idle timeout in seconds (0 = no idle cap).
///
/// Returns `None` if neither deadline applies.
/// Returns the earlier of the two deadlines when both apply.
pub fn next_close_deadline_ms(
    token_exp_ms: u64,
    last_active_ms: u64,
    idle_cap_secs: u64,
) -> Option<u64> {
    let idle_deadline = if idle_cap_secs > 0 {
        Some(last_active_ms.saturating_add(idle_cap_secs * 1_000))
    } else {
        None
    };
    let token_deadline = if token_exp_ms > 0 {
        Some(token_exp_ms)
    } else {
        None
    };
    match (idle_deadline, token_deadline) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Determine the kill reason for a deadline breach, preferring
/// `TokenExpired` when the token deadline is earlier than the idle deadline.
fn kill_reason_for(
    token_exp_ms: u64,
    last_active_ms: u64,
    idle_cap_secs: u64,
    now_ms: u64,
) -> KillReason {
    // Evaluate which deadline fired first.
    let token_fired = token_exp_ms > 0 && now_ms >= token_exp_ms;
    let idle_fired =
        idle_cap_secs > 0 && now_ms >= last_active_ms.saturating_add(idle_cap_secs * 1_000);

    match (token_fired, idle_fired) {
        (true, false) => KillReason::TokenExpired,
        (false, true) => KillReason::IdleTimeout,
        (true, true) => {
            // Both fired: prefer the one with the earlier deadline.
            let token_dl = token_exp_ms;
            let idle_dl = last_active_ms.saturating_add(idle_cap_secs * 1_000);
            if token_dl <= idle_dl {
                KillReason::TokenExpired
            } else {
                KillReason::IdleTimeout
            }
        }
        (false, false) => KillReason::Alive,
    }
}

/// Single sweep pass.
///
/// Exported for testing; production code calls this via `idle_sweep_loop`.
pub fn sweep_once(session_registry: &Arc<SessionRegistry>, global_idle_secs: u64) {
    let now = now_ms();
    let entries = session_registry.sweep_snapshot();
    for entry in entries {
        // Per-session cap: the session's cached `idle_timeout_secs` (loaded from
        // `IdleTimeoutCache` at session start and on cache invalidation).
        // Falls back to the global cap when the per-database value is 0.
        let idle_secs = if entry.idle_timeout_secs > 0 {
            entry.idle_timeout_secs
        } else {
            global_idle_secs
        };
        // last_active is stored in seconds; convert to ms for deadline arithmetic.
        let last_active_ms = entry.last_active_secs * 1_000;
        // A busy session is not idle, so only its token expiry applies.
        let idle_secs = if entry.busy { 0 } else { idle_secs };
        let deadline = next_close_deadline_ms(entry.token_expiry_ms, last_active_ms, idle_secs);
        let Some(dl) = deadline else {
            continue;
        };
        if now >= dl {
            let reason = kill_reason_for(entry.token_expiry_ms, last_active_ms, idle_secs, now);
            if reason != KillReason::Alive {
                let _ = session_registry.kill_session_by_id(&entry.session_id, reason);
            }
        }
    }
}

/// Copy each session's database idle cap from the cache into its row, so an
/// `ALTER DATABASE` reaches open sessions at the next tick.
fn refresh_idle_caps(state: &SharedState) {
    for entry in state.session_registry.sweep_snapshot() {
        state.session_registry.set_idle_timeout_secs(
            &entry.session_id,
            state.idle_timeout_cache.get(entry.current_database),
        );
    }
}

/// Spawn the idle sweep loop on the Tokio runtime.
///
/// Runs at a 5-second tick.  Respects `SharedState::shutdown` for graceful
/// termination.  Registered in `loop_registry` for drain-on-shutdown.
pub fn spawn_idle_sweep_loop(shared: &Arc<SharedState>) {
    let shared_sweep = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "idle_session_sweep",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                refresh_idle_caps(&shared_sweep);
                let global_idle = shared_sweep.idle_timeout_secs();
                sweep_once(&shared_sweep.session_registry, global_idle);
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_deadline_when_both_zero() {
        assert_eq!(next_close_deadline_ms(0, 1_000_000, 0), None);
    }

    #[test]
    fn idle_deadline_only() {
        // last_active_ms = 1000s, cap = 300s → deadline = 1300s in ms
        let dl = next_close_deadline_ms(0, 1_000_000, 300);
        assert_eq!(dl, Some(1_300_000));
    }

    #[test]
    fn token_deadline_only() {
        let dl = next_close_deadline_ms(9_999_000, 1_000_000, 0);
        assert_eq!(dl, Some(9_999_000));
    }

    #[test]
    fn picks_earlier_idle_over_token() {
        // idle fires at 1_300_000, token at 9_999_000 → idle wins
        let dl = next_close_deadline_ms(9_999_000, 1_000_000, 300);
        assert_eq!(dl, Some(1_300_000));
    }

    #[test]
    fn picks_earlier_token_over_idle() {
        // token fires at 500_000, idle at 1_300_000 → token wins
        let dl = next_close_deadline_ms(500_000, 1_000_000, 300);
        assert_eq!(dl, Some(500_000));
    }

    #[test]
    fn kill_reason_token_when_token_earlier() {
        let now = 600_000u64;
        let reason = kill_reason_for(500_000, 1_000_000, 300, now);
        assert_eq!(reason, KillReason::TokenExpired);
    }

    #[test]
    fn kill_reason_idle_when_idle_fires() {
        let now = 1_400_000u64;
        // idle deadline = 1_300_000, token at 9_999_000 → idle
        let reason = kill_reason_for(9_999_000, 1_000_000, 300, now);
        assert_eq!(reason, KillReason::IdleTimeout);
    }

    #[test]
    fn alive_when_not_expired() {
        let now = 100_000u64;
        let reason = kill_reason_for(9_999_000, 1_000_000, 300, now);
        assert_eq!(reason, KillReason::Alive);
    }

    fn registered(
        registry: &SessionRegistry,
        session_id: &str,
    ) -> tokio::sync::watch::Receiver<KillReason> {
        registry
            .register(
                session_id,
                &crate::control::security::sessions::SessionParams {
                    user_id: 1,
                    username: "u".to_string(),
                    db_user: "u".to_string(),
                    peer_addr: "127.0.0.1:1".to_string(),
                    protocol: "test".to_string(),
                    auth_method: "password".to_string(),
                    tenant_id: 1,
                    credential_version: 0,
                    current_database: None,
                    token_expiry_ms: None,
                    api_key_id: None,
                },
            )
            .expect("register")
    }

    #[test]
    fn the_sweep_ends_an_expired_token_session_and_skips_nothing_else() {
        let registry = Arc::new(SessionRegistry::new());
        let expired = registered(&registry, "expired");
        let current = registered(&registry, "current");
        registry.set_token_expiry("expired", 1);

        sweep_once(&registry, 0);

        assert_eq!(*expired.borrow(), KillReason::TokenExpired);
        assert_eq!(*current.borrow(), KillReason::Alive);
    }

    /// The last-active clock has one-second steps, so the idle case waits it out.
    #[test]
    fn the_sweep_ends_an_idle_session_and_spares_a_busy_one() {
        let registry = Arc::new(SessionRegistry::new());
        let idle = registered(&registry, "idle");
        let busy = registered(&registry, "busy");
        registry.set_current_statement("busy", "stream".to_string());

        std::thread::sleep(Duration::from_millis(2_100));
        sweep_once(&registry, 1);

        assert_eq!(*idle.borrow(), KillReason::IdleTimeout);
        assert_eq!(*busy.borrow(), KillReason::Alive);
    }

    #[test]
    fn a_database_idle_cap_overrides_the_global_cap() {
        let registry = Arc::new(SessionRegistry::new());
        let capped = registered(&registry, "capped");
        registry.set_idle_timeout_secs("capped", 1);

        std::thread::sleep(Duration::from_millis(2_100));
        sweep_once(&registry, 3_600);

        assert_eq!(*capped.borrow(), KillReason::IdleTimeout);
    }
}
