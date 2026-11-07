// SPDX-License-Identifier: BUSL-1.1

//! Mirror observer loop: lag monitoring and status-transition logic.
//!
//! # Status transitions
//!
//! The lag is the wall-clock age of the newest cut every mirror group applied:
//!
//!   `lag_ms = now_ms - applied_cut_ms`
//!
//! The bounded-staleness read gate in `read.rs` uses the same basis, so the
//! gate and the metric never disagree. No clock-skew correction applies.
//!
//! Transitions:
//!
//! - `Following`  →  `Degraded`      when `lag_ms > LAG_DEGRADED_MS`
//! - `Degraded`   →  `Disconnected`  when no frame arrives for `LAG_DISCONNECTED_MS`
//! - `Degraded` or `Disconnected` → `Following` once lag drops to the threshold
//!
//! `Bootstrapping`, `Promoting`, `Failed` and `Promoted` change only by an
//! explicit step. A truncated source log stops the link at `Failed`, because
//! a rebuild removes the readable copy and an operator asks for it.
//!
//! # Metric
//!
//! `nodedb_database_mirror_lag_ms{database="..."}` is updated on every
//! call to [`update_lag_status`] by writing to the per-database counter
//! in `DatabaseMetricsRegistry`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nodedb_types::{DatabaseId, MirrorStatus};
use tracing::{info, warn};

use crate::control::metrics::DatabaseMetricsRegistry;
use crate::control::security::catalog::SystemCatalog;

/// Lag threshold for `Following → Degraded`. Mirrors lag more than this
/// are considered unhealthy but still connected. Locked as a constant so
/// the rejection path and the status machine use the same wall-clock basis.
pub const LAG_DEGRADED_MS: u64 = 5_000;

/// Inactivity threshold for `Degraded → Disconnected`. No source frame
/// received for this duration triggers disconnected status. Locked as a
/// constant for the same reason as `LAG_DEGRADED_MS`.
pub const LAG_DISCONNECTED_MS: u64 = 30_000;

/// Return current wall-clock milliseconds since UNIX epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

/// Outcome of a lag evaluation for a single mirror database.
///
/// Exhaustive matches are required — no `_ =>` arms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LagTransition {
    /// Status unchanged; no catalog mutation needed.
    Unchanged,
    /// Status changes to `Following`.
    BecomeFollowing,
    /// Status changes to `Degraded`.
    BecomeDegraded,
    /// Status changes to `Disconnected`.
    BecomeDisconnected,
}

/// Compute the lag transition for a mirror database given its current status
/// and its lag.
///
/// `last_entry_received_ms` is the wall-clock time the last source frame
/// arrived. An entry can arrive long before its cut applies, so the disconnect
/// timer runs on the receive side.
///
/// `current_status` is the `MirrorStatus` field from `MirrorOrigin` as
/// stored in the `DatabaseDescriptor`.
pub fn compute_lag_transition(
    current_status: &MirrorStatus,
    lag_ms: u64,
    last_entry_received_ms: u64,
) -> LagTransition {
    let now = now_ms();

    match current_status {
        MirrorStatus::Promoted
        | MirrorStatus::Promoting { .. }
        | MirrorStatus::Failed { .. }
        | MirrorStatus::Bootstrapping { .. } => LagTransition::Unchanged,

        MirrorStatus::Following => {
            if lag_ms > LAG_DEGRADED_MS {
                LagTransition::BecomeDegraded
            } else {
                LagTransition::Unchanged
            }
        }

        MirrorStatus::Degraded => {
            let no_entry_duration = now.saturating_sub(last_entry_received_ms);
            if no_entry_duration > LAG_DISCONNECTED_MS {
                LagTransition::BecomeDisconnected
            } else if lag_ms <= LAG_DEGRADED_MS {
                LagTransition::BecomeFollowing
            } else {
                LagTransition::Unchanged
            }
        }

        MirrorStatus::Disconnected => {
            if lag_ms <= LAG_DEGRADED_MS {
                LagTransition::BecomeFollowing
            } else {
                LagTransition::Unchanged
            }
        }
    }
}

/// Update the lag metric and evaluate whether the mirror status needs to
/// transition based on the database's cut state.
///
/// Returns the new `MirrorStatus` that the caller persists, or `None` if no
/// change is needed.
///
/// `last_entry_received_ms` is the wall-clock time of the most recent inbound
/// frame from the source, as tracked by the link registry. With no link in
/// the registry, the age of the applied cut stands in. Frames drive cuts, so
/// the disconnect timer keeps running.
///
/// Side effect: updates `nodedb_database_mirror_lag_ms{database=db_name}`
/// in the metrics registry.
pub fn update_lag_status(
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    db_name: &str,
    current_status: &MirrorStatus,
    last_entry_received_ms: Option<u64>,
    db_metrics: &DatabaseMetricsRegistry,
) -> Option<MirrorStatus> {
    let cut_state = match catalog.get_mirror_cut_state(db_id) {
        Ok(state) => state,
        Err(e) => {
            warn!(
                db_id = db_id.as_u64(),
                error = %e,
                "mirror observer: failed to read mirror_cut_state; skipping transition"
            );
            return None;
        }
    };
    // With no applied cut the mirror has no lag to judge: it stays as it is.
    let applied_cut_ms = cut_state.as_ref()?.applied_cut_ms()?;

    let lag_ms = now_ms().saturating_sub(applied_cut_ms);
    db_metrics.set_mirror_lag_ms(db_id, lag_ms);

    let last_received = last_entry_received_ms.unwrap_or(applied_cut_ms);

    let transition = compute_lag_transition(current_status, lag_ms, last_received);

    match transition {
        LagTransition::Unchanged => None,
        LagTransition::BecomeFollowing => {
            info!(
                db = db_name,
                lag_ms, "mirror observer: lag recovered — Following"
            );
            Some(MirrorStatus::Following)
        }
        LagTransition::BecomeDegraded => {
            info!(
                db = db_name,
                lag_ms, "mirror observer: lag exceeded threshold — Degraded"
            );
            Some(MirrorStatus::Degraded)
        }
        LagTransition::BecomeDisconnected => {
            warn!(
                db = db_name,
                "mirror observer: no entries received for 30s — Disconnected"
            );
            Some(MirrorStatus::Disconnected)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn following_to_degraded_when_lag_exceeds_threshold() {
        let now = now_ms();
        // Simulate lag above 5 s.
        let lag = LAG_DEGRADED_MS + 1;
        let t = compute_lag_transition(&MirrorStatus::Following, lag, now);
        assert_eq!(t, LagTransition::BecomeDegraded);
    }

    #[test]
    fn following_unchanged_when_lag_below_threshold() {
        let now = now_ms();
        let t = compute_lag_transition(&MirrorStatus::Following, 100, now);
        assert_eq!(t, LagTransition::Unchanged);
    }

    #[test]
    fn degraded_to_disconnected_after_30s_no_entry() {
        // Simulate last_entry_received 31 s ago.
        let old_received = now_ms().saturating_sub(LAG_DISCONNECTED_MS + 1_000);
        let lag = LAG_DEGRADED_MS + 1;
        let t = compute_lag_transition(&MirrorStatus::Degraded, lag, old_received);
        assert_eq!(t, LagTransition::BecomeDisconnected);
    }

    #[test]
    fn degraded_stays_while_lag_moves_above_the_threshold() {
        let now = now_ms();
        let t = compute_lag_transition(&MirrorStatus::Degraded, LAG_DEGRADED_MS + 9, now);
        assert_eq!(t, LagTransition::Unchanged);
    }

    #[test]
    fn degraded_recovers_to_following_when_lag_drops() {
        let now = now_ms();
        let t = compute_lag_transition(&MirrorStatus::Degraded, 100, now);
        assert_eq!(t, LagTransition::BecomeFollowing);
    }

    #[test]
    fn disconnected_recovers_to_following_when_lag_drops() {
        let now = now_ms();
        let t = compute_lag_transition(&MirrorStatus::Disconnected, 100, now);
        assert_eq!(t, LagTransition::BecomeFollowing);
    }

    #[test]
    fn disconnected_never_rebuilds_on_its_own() {
        let now = now_ms();
        let t = compute_lag_transition(&MirrorStatus::Disconnected, LAG_DEGRADED_MS + 1, now);
        assert_eq!(t, LagTransition::Unchanged);
    }

    #[test]
    fn explicit_statuses_never_move_on_lag() {
        let now = now_ms();
        for status in [
            MirrorStatus::Promoted,
            MirrorStatus::Promoting { final_cut: 3 },
            MirrorStatus::Failed {
                failure: nodedb_types::MirrorFailure::LogTruncated {
                    group: 1,
                    first_available: 2,
                },
            },
            MirrorStatus::Bootstrapping {
                bytes_done: 0,
                bytes_total: 0,
            },
        ] {
            assert_eq!(
                compute_lag_transition(&status, 999_999, now),
                LagTransition::Unchanged,
                "{status:?}"
            );
        }
    }
}
