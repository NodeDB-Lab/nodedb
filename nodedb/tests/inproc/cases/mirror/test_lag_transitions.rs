// SPDX-License-Identifier: BUSL-1.1

//! `mirror_lag_transitions`: verifies the lag status machine transitions.
//!
//! - `Following` (lag < 5 s) → `Degraded` when lag > 5 s
//! - `Degraded` → `Disconnected` when no source frame arrives for 30 s
//! - `Disconnected` → `Following` once frames resume and lag < 5 s
//! - A truncated source log never starts a rebuild here: the link stops at
//!   `Failed`, and an operator asks for the rebuild.

use nodedb::control::mirror::{
    LAG_DEGRADED_MS, LAG_DISCONNECTED_MS, LagTransition, compute_lag_transition,
};
use nodedb_types::{MirrorFailure, MirrorStatus};

use super::helpers::now_ms;

#[test]
fn mirror_lag_transitions_following_to_degraded() {
    let now = now_ms();
    let lag = LAG_DEGRADED_MS + 500;
    let t = compute_lag_transition(&MirrorStatus::Following, lag, now);
    assert_eq!(
        t,
        LagTransition::BecomeDegraded,
        "Following with lag > {LAG_DEGRADED_MS} ms must transition to Degraded"
    );
}

#[test]
fn mirror_lag_transitions_following_unchanged_below_threshold() {
    let now = now_ms();
    let t = compute_lag_transition(&MirrorStatus::Following, 100, now);
    assert_eq!(
        t,
        LagTransition::Unchanged,
        "Following with lag < {LAG_DEGRADED_MS} ms must be Unchanged"
    );
}

#[test]
fn mirror_lag_transitions_degraded_to_disconnected_after_30s() {
    // last_entry_received > 30 s ago
    let old_received = now_ms().saturating_sub(LAG_DISCONNECTED_MS + 2_000);
    let lag = LAG_DEGRADED_MS + 1;
    let t = compute_lag_transition(&MirrorStatus::Degraded, lag, old_received);
    assert_eq!(
        t,
        LagTransition::BecomeDisconnected,
        "Degraded with no entry for 30+ s must become Disconnected"
    );
}

#[test]
fn mirror_lag_transitions_degraded_recovers_when_lag_drops() {
    let now = now_ms();
    let t = compute_lag_transition(&MirrorStatus::Degraded, 50, now);
    assert_eq!(
        t,
        LagTransition::BecomeFollowing,
        "Degraded with lag below threshold must recover to Following"
    );
}

#[test]
fn mirror_lag_transitions_disconnected_to_following_when_lag_ok() {
    let now = now_ms();
    let t = compute_lag_transition(&MirrorStatus::Disconnected, 100, now);
    assert_eq!(
        t,
        LagTransition::BecomeFollowing,
        "Disconnected with low lag must return to Following"
    );
}

#[test]
fn mirror_lag_transitions_failed_stays_failed() {
    let now = now_ms();
    let status = MirrorStatus::Failed {
        failure: MirrorFailure::LogTruncated {
            group: 1,
            first_available: 500,
        },
    };
    assert_eq!(
        compute_lag_transition(&status, 100, now),
        LagTransition::Unchanged,
        "a failed link moves only by an operator's statement"
    );
}

#[test]
fn mirror_lag_transitions_exact_threshold_boundary() {
    let now = now_ms();
    // Exactly at the threshold — must NOT degrade.
    let t = compute_lag_transition(&MirrorStatus::Following, LAG_DEGRADED_MS, now);
    assert_eq!(
        t,
        LagTransition::Unchanged,
        "Lag exactly at {LAG_DEGRADED_MS} ms must not trigger Degraded (strict >)"
    );

    // One millisecond over — must degrade.
    let lag_over = LAG_DEGRADED_MS + 1;
    let t2 = compute_lag_transition(&MirrorStatus::Following, lag_over, now);
    assert_eq!(
        t2,
        LagTransition::BecomeDegraded,
        "Lag at {lag_over} ms must trigger Degraded"
    );
}
