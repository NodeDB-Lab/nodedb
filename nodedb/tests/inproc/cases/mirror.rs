// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the mirror database subsystem.

#[path = "mirror/helpers.rs"]
#[allow(dead_code)]
mod helpers;
#[path = "mirror/test_clone_from_mirror_rejected.rs"]
mod test_clone_from_mirror_rejected;
#[path = "mirror/test_eventual_read_serves_local.rs"]
mod test_eventual_read_serves_local;
#[path = "mirror/test_fresh_draw_doors.rs"]
mod test_fresh_draw_doors;
#[path = "mirror/test_full_lifecycle.rs"]
mod test_full_lifecycle;
#[path = "mirror/test_lag_transitions.rs"]
mod test_lag_transitions;
#[path = "mirror/test_promotion_durability.rs"]
mod test_promotion_durability;
#[path = "mirror/test_read_consistency_selector.rs"]
mod test_read_consistency_selector;
#[path = "mirror/test_read_only_protocols.rs"]
mod test_read_only_protocols;
#[path = "mirror/test_recovery_refusals.rs"]
mod test_recovery_refusals;
#[path = "mirror/test_restart_resume.rs"]
mod test_restart_resume;
#[path = "mirror/test_simulated_latency.rs"]
mod test_simulated_latency;
#[path = "mirror/test_strong_read_rejected.rs"]
mod test_strong_read_rejected;
#[path = "mirror/test_sync_write_paths.rs"]
mod test_sync_write_paths;
