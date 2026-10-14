// SPDX-License-Identifier: BUSL-1.1

//! Mirror observer loop: lag monitoring and status-transition logic.
//!
//! # Status transitions
//!
//! The observer apply loop computes a wall-clock lag as:
//!
//!   `lag_ms = now_ms - last_apply_ms`
//!
//! Wall-clock comparison is intentional: the bounded-staleness read rejection
//! path in `read.rs` also uses wall-clock, so both sides are consistent.
//! No clock-skew correction is applied — doing so would create an asymmetry
//! where the read gate and the lag metric disagree, which is more confusing
//! than a modest ± offset between machines in the same region.
//!
//! Transitions:
//!
//! - `Following`  →  `Degraded { lag_ms }`  when `lag_ms > LAG_DEGRADED_MS`
//! - `Degraded`   →  `Disconnected`         when no AppendEntries for `LAG_DISCONNECTED_MS`
//! - `Disconnected` → `Bootstrapping` if LSN gap exceeds source log window,
//!   → `Following` once entries resume and lag drops below floor
//!
//! # Metric
//!
//! `nodedb_database_mirror_lag_ms{database="..."}` is updated on every
//! call to [`update_lag_status`] by writing to the per-database counter
//! in `DatabaseMetricsRegistry`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nodedb_types::{DatabaseId, Lsn, MirrorLagRecord, MirrorStatus};
use tracing::{info, warn};

use crate::control::metrics::DatabaseMetricsRegistry;
use crate::control::security::catalog::SystemCatalog;

/// Lag threshold for `Following → Degraded`. Mirrors lag more than this
/// are considered unhealthy but still connected. Locked as a constant so
/// the rejection path and the status machine use the same wall-clock basis.
pub const LAG_DEGRADED_MS: u64 = 5_000;

/// Inactivity threshold for `Degraded → Disconnected`. No AppendEntries
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
    /// Status should change to `Following`.
    BecomeFollowing,
    /// Status should change to `Degraded { lag_ms }`.
    BecomeDegraded { lag_ms: u64 },
    /// Status should change to `Disconnected`.
    BecomeDisconnected,
    /// Status should change back to `Bootstrapping` (LSN gap too large).
    BecomeBootstrapping { bytes_done: u64, bytes_total: u64 },
}

/// Compute the lag transition for a mirror database given its current status
/// and the `MirrorLagRecord` from `_system.mirror_lag`.
///
/// `last_entry_received_ms` is the wall-clock time the last AppendEntries
/// was received from the source.  This is distinct from `last_apply_ms`
/// in the lag record: an entry may be received but not yet applied (apply
/// queue backlog). The disconnect timer runs on the receive side.
///
/// `current_status` is the `MirrorStatus` field from `MirrorOrigin` as
/// stored in the `DatabaseDescriptor`. Only `Following` and `Degraded`
/// transitions are computed here; `Bootstrapping` and `Disconnected` are
/// set by the apply loop when it detects an LSN gap or timeout.
pub fn compute_lag_transition(
    current_status: &MirrorStatus,
    lag_ms: u64,
    last_entry_received_ms: u64,
    needs_fresh_snapshot: bool,
) -> LagTransition {
    let now = now_ms();

    match current_status {
        // A promoted mirror is a normal database — no transition applies.
        MirrorStatus::Promoted => LagTransition::Unchanged,

        MirrorStatus::Following => {
            if lag_ms > LAG_DEGRADED_MS {
                LagTransition::BecomeDegraded { lag_ms }
            } else {
                LagTransition::Unchanged
            }
        }

        MirrorStatus::Degraded {
            lag_ms: current_lag,
        } => {
            let no_entry_duration = now.saturating_sub(last_entry_received_ms);
            if no_entry_duration > LAG_DISCONNECTED_MS {
                LagTransition::BecomeDisconnected
            } else if lag_ms <= LAG_DEGRADED_MS {
                LagTransition::BecomeFollowing
            } else if *current_lag != lag_ms {
                // Lag changed but still above threshold — update the value.
                LagTransition::BecomeDegraded { lag_ms }
            } else {
                LagTransition::Unchanged
            }
        }

        MirrorStatus::Disconnected => {
            // Check if we can reconnect without a fresh snapshot.
            if needs_fresh_snapshot {
                LagTransition::BecomeBootstrapping {
                    bytes_done: 0,
                    bytes_total: 0,
                }
            } else if lag_ms <= LAG_DEGRADED_MS {
                LagTransition::BecomeFollowing
            } else {
                LagTransition::Unchanged
            }
        }

        MirrorStatus::Bootstrapping { .. } => LagTransition::Unchanged,
    }
}

/// Update the lag metric and evaluate whether the mirror status needs to
/// transition based on the current `mirror_lag` catalog entry.
///
/// Returns the new `MirrorStatus` that the caller should persist, or `None`
/// if no change is needed.
///
/// `last_entry_received_ms` is the wall-clock time of the most recent inbound
/// frame from the source, as tracked by the link registry. When `None` (link
/// not yet registered, e.g. before the cluster layer has reconnected after a
/// restart) the function falls back to `lag_record.last_apply_ms`: applies
/// drive forward only when frames arrive, so `last_apply_ms` is a sound — if
/// slightly conservative — proxy. Falling back to `now_ms()` here would be
/// wrong: it would silently disable the `Degraded → Disconnected` transition.
///
/// Side effect: updates `nodedb_database_mirror_lag_ms{database=db_name}`
/// in the metrics registry.
pub fn update_lag_status(
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    db_name: &str,
    current_status: &MirrorStatus,
    last_entry_received_ms: Option<u64>,
    needs_fresh_snapshot: bool,
    db_metrics: &DatabaseMetricsRegistry,
) -> Option<MirrorStatus> {
    // Read the current lag record.
    let lag_record = match catalog.get_mirror_lag(db_id) {
        Ok(Some(r)) => r,
        Ok(None) => {
            // No lag record yet — mirror has not applied any entries.
            // Treat as maximally stale: do not transition to Following.
            return None;
        }
        Err(e) => {
            warn!(
                db_id = db_id.as_u64(),
                error = %e,
                "mirror observer: failed to read mirror_lag; skipping transition"
            );
            return None;
        }
    };

    let now = now_ms();
    let lag_ms = now.saturating_sub(lag_record.last_apply_ms);

    // Publish the metric: now_ms - last_apply_ms.
    db_metrics.set_mirror_lag_ms(db_id, db_name, lag_ms);

    // Fall back to apply-time when the registry has no entry.
    let last_received = last_entry_received_ms.unwrap_or(lag_record.last_apply_ms);

    let transition =
        compute_lag_transition(current_status, lag_ms, last_received, needs_fresh_snapshot);

    match transition {
        LagTransition::Unchanged => None,
        LagTransition::BecomeFollowing => {
            info!(
                db = db_name,
                lag_ms, "mirror observer: lag recovered — Following"
            );
            Some(MirrorStatus::Following)
        }
        LagTransition::BecomeDegraded { lag_ms } => {
            info!(
                db = db_name,
                lag_ms, "mirror observer: lag exceeded threshold — Degraded"
            );
            Some(MirrorStatus::Degraded { lag_ms })
        }
        LagTransition::BecomeDisconnected => {
            warn!(
                db = db_name,
                "mirror observer: no entries received for 30s — Disconnected"
            );
            Some(MirrorStatus::Disconnected)
        }
        LagTransition::BecomeBootstrapping {
            bytes_done,
            bytes_total,
        } => {
            warn!(
                db = db_name,
                "mirror observer: LSN gap requires fresh snapshot — Bootstrapping"
            );
            Some(MirrorStatus::Bootstrapping {
                bytes_done,
                bytes_total,
            })
        }
    }
}

/// Write an updated `MirrorLagRecord` for the given mirror database,
/// advancing `last_applied_lsn` and `last_apply_ms`.
///
/// This is the hot path: called on every AppendEntries apply.
pub fn record_apply(
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    lsn: Lsn,
) -> crate::Result<MirrorLagRecord> {
    let apply_ms = now_ms();
    let record = MirrorLagRecord {
        last_applied_lsn: lsn,
        last_apply_ms: apply_ms,
    };
    catalog.put_mirror_lag(db_id, &record)?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn following_to_degraded_when_lag_exceeds_threshold() {
        let now = now_ms();
        // Simulate lag above 5 s.
        let lag = LAG_DEGRADED_MS + 1;
        let t = compute_lag_transition(&MirrorStatus::Following, lag, now, false);
        assert_eq!(t, LagTransition::BecomeDegraded { lag_ms: lag });
    }

    #[test]
    fn following_unchanged_when_lag_below_threshold() {
        let now = now_ms();
        let t = compute_lag_transition(&MirrorStatus::Following, 100, now, false);
        assert_eq!(t, LagTransition::Unchanged);
    }

    #[test]
    fn degraded_to_disconnected_after_30s_no_entry() {
        // Simulate last_entry_received 31 s ago.
        let old_received = now_ms().saturating_sub(LAG_DISCONNECTED_MS + 1_000);
        let lag = LAG_DEGRADED_MS + 1;
        let t = compute_lag_transition(
            &MirrorStatus::Degraded { lag_ms: lag },
            lag,
            old_received,
            false,
        );
        assert_eq!(t, LagTransition::BecomeDisconnected);
    }

    #[test]
    fn degraded_recovers_to_following_when_lag_drops() {
        let now = now_ms();
        let t = compute_lag_transition(
            &MirrorStatus::Degraded { lag_ms: 8_000 },
            100, // now below threshold
            now,
            false,
        );
        assert_eq!(t, LagTransition::BecomeFollowing);
    }

    #[test]
    fn disconnected_triggers_bootstrap_on_lsn_gap() {
        let now = now_ms();
        let t = compute_lag_transition(&MirrorStatus::Disconnected, 0, now, true);
        assert_eq!(
            t,
            LagTransition::BecomeBootstrapping {
                bytes_done: 0,
                bytes_total: 0
            }
        );
    }

    #[test]
    fn disconnected_recovers_to_following_without_lsn_gap() {
        let now = now_ms();
        let t = compute_lag_transition(&MirrorStatus::Disconnected, 100, now, false);
        assert_eq!(t, LagTransition::BecomeFollowing);
    }

    #[test]
    fn promoted_is_always_unchanged() {
        let now = now_ms();
        let t = compute_lag_transition(&MirrorStatus::Promoted, 999_999, now, true);
        assert_eq!(t, LagTransition::Unchanged);
    }

    #[test]
    fn bootstrapping_is_always_unchanged() {
        let now = now_ms();
        let t = compute_lag_transition(
            &MirrorStatus::Bootstrapping {
                bytes_done: 0,
                bytes_total: 0,
            },
            100,
            now,
            false,
        );
        assert_eq!(t, LagTransition::Unchanged);
    }
}
