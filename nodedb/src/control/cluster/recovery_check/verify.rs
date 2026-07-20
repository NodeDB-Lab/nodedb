// SPDX-License-Identifier: BUSL-1.1

//! Top-level pipeline invoked at the `CatalogSanityCheck`
//! startup phase.
//!
//! Runs the three sub-checks in order:
//!
//! 1. Applied-index gate — local `MetadataCache.applied_index`
//!    against the current `AppliedIndexWatcher` watermark.
//! 2. redb cross-table integrity repair — deterministically heals missing or
//!    dangling user references and leaves unknown violations fatal.
//! 3. Registry ⇔ redb verifier — re-load every in-memory registry made stale
//!    by catalog repair.
//!
//! Returns a [`VerifyReport`] with per-phase outcomes. The
//! caller (main.rs) checks `report.is_acceptable()` and
//! either advances the phase or calls
//! `shared.startup.fail()` + aborts startup.

use std::time::Instant;

use crate::control::state::SharedState;

use super::applied_index::check_applied_index;
use super::integrity::verify_redb_integrity;
use super::registry_verify::verify_registries;
use super::repair_integrity::heal_orphan_rows;
use super::report::VerifyReport;

/// Run the full catalog sanity check pipeline against the
/// shared state. Never panics. Repairs recoverable redb integrity violations,
/// then refreshes divergent in-memory registries.
pub async fn verify_and_repair(shared: &SharedState) -> crate::Result<VerifyReport> {
    let start = Instant::now();

    // ── 1. Applied-index gate ──────────────────────────
    let gate = check_applied_index(shared);
    if !gate.is_ok() {
        tracing::error!(
            cache_applied = gate.cache_applied,
            watcher_current = gate.watcher_current,
            gap = gate.gap,
            "catalog sanity check: applied_index gap — metadata replay incomplete"
        );
    }

    // ── 2. redb integrity repair, then registry verification ──
    //
    // Integrity repair writes catalog rows directly, so registry verification
    // runs afterwards and reloads any cache made stale by those repairs. The
    // loop reaches a fixed point because reconstructing a missing owner row can
    // expose a second-order dangling-user reference on the next verification.
    let (registry_outcome, integrity, integrity_healed) = {
        let catalog = shared.credentials.catalog();
        let mut integrity = verify_redb_integrity(catalog);
        let mut total_healed = 0usize;

        loop {
            let (_, healed) = heal_orphan_rows(catalog, integrity);
            total_healed += healed;
            integrity = verify_redb_integrity(catalog);
            if healed == 0 {
                break;
            }
        }

        if total_healed > 0 {
            tracing::info!(
                healed = total_healed,
                remaining = integrity.len(),
                "catalog sanity check: integrity self-heal pass completed"
            );
        }
        let reg = verify_registries(shared, catalog)?;
        (Some(reg), integrity, total_healed)
    };

    // ── 3. Assemble report ─────────────────────────────
    let (registry_divergences, all_repairs_ok) = match registry_outcome {
        Some(o) => {
            // Emit labeled metrics: one observation per registry.
            if let Some(metrics) = shared.system_metrics.as_deref() {
                for (registry, count) in &o.counts {
                    let outcome = if count.detected == 0 {
                        "ok"
                    } else if count.repaired == count.detected {
                        "warning"
                    } else {
                        "error"
                    };
                    metrics.record_catalog_sanity_check(registry, outcome);
                }
            }
            (o.counts, o.all_repairs_ok)
        }
        None => (Default::default(), true),
    };

    Ok(VerifyReport {
        applied_index_ok: gate.is_ok(),
        applied_index_gap: gate.gap,
        integrity_violations: integrity,
        integrity_repaired: integrity_healed,
        registry_divergences,
        all_repairs_ok,
        elapsed: start.elapsed(),
    })
}
