// SPDX-License-Identifier: BUSL-1.1

//! Read consistency enforcement for mirror databases.
//!
//! A mirror database is a continuously-updated read-only replica of a source
//! database. The three `ReadConsistency` levels behave as follows on a mirror:
//!
//! - `Strong`: Mirrors cannot serve strong-consistency reads because they are
//!   not the Raft leader for the source's log. A strong read on a mirror would
//!   require forwarding to the source cluster, which is a different process.
//!   This function returns `STALE_READ_NOT_LEADER` immediately with the source
//!   cluster endpoint as a hint so the client can redirect.
//!
//! - `BoundedStaleness(d)`: Reads the applied cut's time from
//!   `_system.mirror_cut_state` and compares it to the current wall clock. If
//!   `now - applied_cut_ms > d`, the mirror is too stale and the function
//!   returns `STALE_READ_NOT_LEADER` with the actual lag in the error detail.
//!   Otherwise the read is served locally.
//!
//! - `Eventual`: Reads are served from the local replica. This is the
//!   lowest-latency option and is correct for use cases that accept CRDT-style
//!   monotonic convergence (e.g. mobile/edge workloads).
//!
//! A mirror still taking its first capture serves no level.

use nodedb_types::error::sqlstate;
use nodedb_types::{DatabaseId, MirrorOrigin, MirrorStatus};

use crate::control::security::catalog::SystemCatalog;
use crate::types::ReadConsistency;

/// Outcome of a mirror read consistency check.
///
/// Exhaustive matches are required — no `_ =>` arms.
#[derive(Debug)]
pub enum MirrorReadOutcome {
    /// Proceed: serve the read from the local mirror replica.
    ServeLocally,
    /// Reject: the consistency level cannot be satisfied by this mirror.
    /// Contains the SQLSTATE code and human-readable error message.
    Reject {
        sqlstate_code: &'static str,
        message: String,
    },
}

/// Check whether a read with the given `consistency` level can be served
/// from a mirror database identified by `mirror_db_id`.
///
/// `mirror_origin` must be `Some` for any database that reaches this check;
/// the caller is responsible for routing non-mirror databases past this gate.
///
/// `promoted` is true when the mirror has been promoted and the database
/// is now a writable primary — in that case all consistency levels are
/// served locally (no longer a mirror).
/// `now_ms` is the current wall-clock time (ms since epoch), supplied by the
/// caller rather than read internally so the staleness comparison is a pure
/// function of its inputs — this keeps `BoundedStaleness` boundary tests
/// deterministic instead of racing the wall clock between lag injection and
/// the check.
pub fn check_mirror_read_consistency(
    catalog: &SystemCatalog,
    mirror_db_id: DatabaseId,
    mirror_origin: &MirrorOrigin,
    consistency: ReadConsistency,
    now_ms: u64,
) -> MirrorReadOutcome {
    // A promoted mirror is a normal writable database — serve all reads locally.
    if matches!(mirror_origin.status, MirrorStatus::Promoted) {
        return MirrorReadOutcome::ServeLocally;
    }

    // A first capture lands in pieces, so what it holds is no state the source
    // ever had. Every level waits for the first cut.
    if matches!(mirror_origin.status, MirrorStatus::Bootstrapping { .. }) {
        return MirrorReadOutcome::Reject {
            sqlstate_code: sqlstate::STALE_READ_NOT_LEADER,
            message: format!(
                "mirror database is taking its first capture and serves no reads until it \
                 finishes; read source cluster '{}' or retry later",
                mirror_origin.source_cluster
            ),
        };
    }

    match consistency {
        ReadConsistency::Strong => {
            // Strong reads cannot be served by a mirror: the mirror is never
            // the Raft leader for the source's commit log. The client must
            // redirect to the source cluster for linearizable reads.
            MirrorReadOutcome::Reject {
                sqlstate_code: sqlstate::STALE_READ_NOT_LEADER,
                message: format!(
                    "database is a mirror and cannot serve strong-consistency reads; \
                     redirect to source cluster '{}'",
                    mirror_origin.source_cluster
                ),
            }
        }

        ReadConsistency::BoundedStaleness(max_lag) => {
            // With no applied cut (e.g. still bootstrapping) the lag is unbounded.
            let cut_state = match catalog.get_mirror_cut_state(mirror_db_id) {
                Ok(cut_state) => cut_state,
                Err(_) => {
                    return MirrorReadOutcome::Reject {
                        sqlstate_code: sqlstate::STALE_READ_NOT_LEADER,
                        message: format!(
                            "mirror cut state unavailable; cannot verify staleness bound. \
                             Source cluster: '{}'",
                            mirror_origin.source_cluster
                        ),
                    };
                }
            };
            let Some(actual_lag_ms) = cut_state.and_then(|state| state.lag_ms(now_ms)) else {
                return MirrorReadOutcome::Reject {
                    sqlstate_code: sqlstate::STALE_READ_NOT_LEADER,
                    message: format!(
                        "mirror database has not yet applied any source cut; \
                         lag is unbounded (bootstrapping). Source cluster: '{}'",
                        mirror_origin.source_cluster
                    ),
                };
            };
            let max_lag_ms = max_lag.as_millis() as u64;

            if actual_lag_ms > max_lag_ms {
                MirrorReadOutcome::Reject {
                    sqlstate_code: sqlstate::STALE_READ_NOT_LEADER,
                    message: format!(
                        "mirror replication lag {actual_lag_ms} ms exceeds the \
                         requested bound {max_lag_ms} ms; source cluster: '{}'",
                        mirror_origin.source_cluster
                    ),
                }
            } else {
                MirrorReadOutcome::ServeLocally
            }
        }

        ReadConsistency::Eventual => {
            // Eventual reads are served unconditionally from the local replica.
            // The caller accepts CRDT-style monotonic convergence semantics.
            MirrorReadOutcome::ServeLocally
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nodedb_types::{DatabaseId, MirrorCutState, MirrorMode, MirrorOrigin, MirrorStatus};
    use tempfile::TempDir;

    use super::*;
    use crate::control::security::catalog::SystemCatalog;

    fn open_tmp_catalog(tmp: &TempDir) -> SystemCatalog {
        let path: PathBuf = tmp.path().join("system.redb");
        SystemCatalog::open(&path).expect("open catalog")
    }

    fn sample_origin(status: MirrorStatus) -> MirrorOrigin {
        MirrorOrigin {
            source_cluster: "prod-us".to_string(),
            source_database: DatabaseId::new(0),
            source_database_name: "source_db".to_string(),
            mode: MirrorMode::Async,
            status,
        }
    }

    fn applied_at(cut_ms: u64) -> MirrorCutState {
        MirrorCutState {
            ready_cut: 1,
            applied_cut: 1,
            applied_cut_hlc_ns: cut_ms.saturating_mul(1_000_000),
            ..MirrorCutState::default()
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64
    }

    #[test]
    fn strong_on_mirror_returns_reject() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1024);
        let origin = sample_origin(MirrorStatus::Following);

        match check_mirror_read_consistency(
            &catalog,
            db_id,
            &origin,
            ReadConsistency::Strong,
            now_ms(),
        ) {
            MirrorReadOutcome::Reject {
                sqlstate_code,
                message,
            } => {
                assert_eq!(sqlstate_code, sqlstate::STALE_READ_NOT_LEADER);
                assert!(
                    message.contains("prod-us"),
                    "error message should mention source cluster: {message}"
                );
            }
            MirrorReadOutcome::ServeLocally => panic!("Strong should be rejected on a mirror"),
        }
    }

    /// A first capture lands in pieces, so no level reads it.
    #[test]
    fn a_bootstrapping_mirror_serves_no_level() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let origin = sample_origin(MirrorStatus::Bootstrapping {
            bytes_done: 0,
            bytes_total: 0,
        });
        for level in [
            ReadConsistency::Strong,
            ReadConsistency::Eventual,
            ReadConsistency::BoundedStaleness(Duration::from_secs(600)),
        ] {
            assert!(
                matches!(
                    check_mirror_read_consistency(
                        &catalog,
                        DatabaseId::new(1030),
                        &origin,
                        level,
                        now_ms()
                    ),
                    MirrorReadOutcome::Reject { .. }
                ),
                "{level:?}"
            );
        }
    }

    #[test]
    fn eventual_on_mirror_serves_locally() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1025);
        let origin = sample_origin(MirrorStatus::Following);

        match check_mirror_read_consistency(
            &catalog,
            db_id,
            &origin,
            ReadConsistency::Eventual,
            now_ms(),
        ) {
            MirrorReadOutcome::ServeLocally => {}
            MirrorReadOutcome::Reject { message, .. } => {
                panic!("Eventual should serve locally, got reject: {message}")
            }
        }
    }

    #[test]
    fn bounded_staleness_fresh_mirror_serves_locally() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1026);
        let origin = sample_origin(MirrorStatus::Following);

        catalog
            .raise_mirror_cut_state(db_id, &applied_at(now_ms()))
            .unwrap();

        let bound = ReadConsistency::BoundedStaleness(Duration::from_secs(10));
        match check_mirror_read_consistency(&catalog, db_id, &origin, bound, now_ms()) {
            MirrorReadOutcome::ServeLocally => {}
            MirrorReadOutcome::Reject { message, .. } => {
                panic!("Fresh mirror should serve locally, got reject: {message}")
            }
        }
    }

    #[test]
    fn bounded_staleness_stale_mirror_returns_reject() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1027);
        let origin = sample_origin(MirrorStatus::Degraded);

        // The applied cut is 60 seconds old.
        let stale_ms = now_ms().saturating_sub(60_000);
        catalog
            .raise_mirror_cut_state(db_id, &applied_at(stale_ms))
            .unwrap();

        // Bound = 5 seconds; actual lag ≈ 60 seconds.
        let bound = ReadConsistency::BoundedStaleness(Duration::from_secs(5));
        match check_mirror_read_consistency(&catalog, db_id, &origin, bound, now_ms()) {
            MirrorReadOutcome::Reject {
                sqlstate_code,
                message,
            } => {
                assert_eq!(sqlstate_code, sqlstate::STALE_READ_NOT_LEADER);
                assert!(
                    message.contains("5000") || message.contains("ms"),
                    "error message should contain lag info: {message}"
                );
            }
            MirrorReadOutcome::ServeLocally => panic!("Stale mirror should be rejected"),
        }
    }

    /// A ready cut that no group applied yet gives no staleness bound.
    #[test]
    fn bounded_staleness_with_no_applied_cut_returns_reject() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1029);
        let origin = sample_origin(MirrorStatus::Following);
        catalog
            .raise_mirror_cut_state(
                db_id,
                &MirrorCutState {
                    ready_cut: 1,
                    ..MirrorCutState::default()
                },
            )
            .unwrap();
        let bound = ReadConsistency::BoundedStaleness(Duration::from_secs(5));
        assert!(matches!(
            check_mirror_read_consistency(&catalog, db_id, &origin, bound, now_ms()),
            MirrorReadOutcome::Reject { .. }
        ));
    }

    /// A stopped link keeps its readable copy, under the same consistency
    /// rules as a following mirror.
    #[test]
    fn a_failed_mirror_still_serves_reads() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1030);
        let origin = sample_origin(MirrorStatus::Failed {
            failure: nodedb_types::MirrorFailure::LogTruncated {
                group: 1,
                first_available: 9,
            },
        });
        catalog
            .raise_mirror_cut_state(db_id, &applied_at(now_ms()))
            .unwrap();
        for consistency in [
            ReadConsistency::Eventual,
            ReadConsistency::BoundedStaleness(Duration::from_secs(60)),
        ] {
            assert!(matches!(
                check_mirror_read_consistency(&catalog, db_id, &origin, consistency, now_ms()),
                MirrorReadOutcome::ServeLocally
            ));
        }
    }

    #[test]
    fn promoted_mirror_always_serves_locally() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1028);
        let origin = sample_origin(MirrorStatus::Promoted);

        for consistency in [
            ReadConsistency::Strong,
            ReadConsistency::Eventual,
            ReadConsistency::BoundedStaleness(Duration::from_secs(1)),
        ] {
            match check_mirror_read_consistency(&catalog, db_id, &origin, consistency, now_ms()) {
                MirrorReadOutcome::ServeLocally => {}
                MirrorReadOutcome::Reject { message, .. } => {
                    panic!("Promoted mirror should always serve locally, got: {message}")
                }
            }
        }
    }
}
