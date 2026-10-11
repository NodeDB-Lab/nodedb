// SPDX-License-Identifier: BUSL-1.1

//! Keeps an entry a snapshot install covers off the Data Plane.
//!
//! Entries wait in the apply channel and the group lanes after the Raft tick
//! handed them off. A snapshot can install for the group meanwhile. Each
//! entry therefore takes the group's apply gate before it starts, and is
//! skipped when the gate reports an installed snapshot at or above its index.
//! The permit is held until the entry's write reached its core. An install
//! waits for it, so a core always receives the write before the restore.

use std::future::Future;
use std::pin::Pin;

use nodedb_cluster::{ApplyPermit, GroupApplyGates};

/// Whether an entry can start.
pub(super) enum EntryAdmission {
    /// Start the entry. The permit, when gates are wired, must be held until
    /// the entry's write reached its core.
    Start(Option<ApplyPermit>),
    /// An installed snapshot already holds the entry's effects: the entry is
    /// at or below the snapshot's Raft index.
    Covered,
    /// An installed snapshot holds the entry's effects: the entry is above
    /// the snapshot's Raft index and at or below the applied index the
    /// snapshot was cut at.
    CoveredByCut,
    /// An install holds or awaits the gate. Retry once it releases.
    Installing,
}

/// An entry's admission and the gate state it read.
pub(super) struct Admission {
    pub decision: EntryAdmission,
    /// The highest snapshot index an install adopted for the group, read
    /// under the gate. `0` with no gates, or while an install holds the gate.
    pub installed_through: u64,
}

/// Admit entry `log_index` of `group_id`. `gates` is `None` before
/// `start_raft` installs them, when no snapshot installs. `covered_through`
/// is the cut of the last snapshot this node installed for the group.
pub(super) fn admit_entry(
    gates: Option<&GroupApplyGates>,
    group_id: u64,
    log_index: u64,
    covered_through: u64,
) -> Admission {
    let (permit, installed_through) = match gates {
        None => (None, 0),
        Some(gates) => match gates.try_apply(group_id) {
            None => {
                return Admission {
                    decision: EntryAdmission::Installing,
                    installed_through: 0,
                };
            }
            Some(permit) => {
                let installed_through = permit.installed_through();
                if log_index <= installed_through {
                    return Admission {
                        decision: EntryAdmission::Covered,
                        installed_through,
                    };
                }
                (Some(permit), installed_through)
            }
        },
    };
    let decision = if log_index <= covered_through {
        EntryAdmission::CoveredByCut
    } else {
        EntryAdmission::Start(permit)
    };
    Admission {
        decision,
        installed_through,
    }
}

/// `work`, holding `permit` until it completes.
pub(super) fn hold_permit<'a, T: 'a>(
    permit: Option<ApplyPermit>,
    work: Pin<Box<dyn Future<Output = T> + Send + 'a>>,
) -> Pin<Box<dyn Future<Output = T> + Send + 'a>> {
    match permit {
        None => work,
        Some(permit) => Box::pin(async move {
            let output = work.await;
            drop(permit);
            output
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn a_node_without_raft_starts_every_entry() {
        assert!(matches!(
            admit_entry(None, 1, 7, 0).decision,
            EntryAdmission::Start(None)
        ));
    }

    /// Entries queued before an install skip what the snapshot covers and
    /// start the rest.
    #[tokio::test]
    async fn queued_entries_at_or_below_an_installed_snapshot_are_covered() {
        let gates = Arc::new(GroupApplyGates::new());
        gates.mount(1);
        assert!(matches!(
            admit_entry(Some(&gates), 1, 5, 0).decision,
            EntryAdmission::Start(Some(_))
        ));

        let mut install = gates.install(1).await;
        assert!(matches!(
            admit_entry(Some(&gates), 1, 5, 0).decision,
            EntryAdmission::Installing
        ));
        install.adopted(5);
        drop(install);

        assert!(matches!(
            admit_entry(Some(&gates), 1, 4, 0).decision,
            EntryAdmission::Covered
        ));
        assert!(matches!(
            admit_entry(Some(&gates), 1, 5, 0).decision,
            EntryAdmission::Covered
        ));
        assert!(matches!(
            admit_entry(Some(&gates), 1, 6, 0).decision,
            EntryAdmission::Start(Some(_))
        ));
        assert!(matches!(
            admit_entry(Some(&gates), 2, 5, 0).decision,
            EntryAdmission::Start(Some(_))
        ));
    }

    /// A snapshot cut at applied index 12 and sent at Raft index 9 holds the
    /// effects of entries 10 through 12. They reach the follower from the log
    /// after the install and never start, so none applies twice. Entry 13
    /// starts.
    #[tokio::test]
    async fn entries_between_the_raft_index_and_the_cut_never_start() {
        let gates = Arc::new(GroupApplyGates::new());
        gates.mount(1);
        gates.install(1).await.adopted(9);
        let cut = 12;
        assert!(matches!(
            admit_entry(Some(&gates), 1, 9, cut).decision,
            EntryAdmission::Covered
        ));
        for log_index in 10..=cut {
            assert!(
                matches!(
                    admit_entry(Some(&gates), 1, log_index, cut).decision,
                    EntryAdmission::CoveredByCut
                ),
                "entry {log_index} is in the installed state and must not apply again"
            );
        }
        assert!(matches!(
            admit_entry(Some(&gates), 1, 13, cut).decision,
            EntryAdmission::Start(Some(_))
        ));
        assert!(matches!(
            admit_entry(None, 1, 11, cut).decision,
            EntryAdmission::CoveredByCut
        ));
    }

    /// Every admission after an install reports the adopted index, so the
    /// lane covers it before any later entry starts.
    #[tokio::test]
    async fn an_admission_reports_the_installed_index() {
        let gates = Arc::new(GroupApplyGates::new());
        gates.mount(1);
        assert_eq!(admit_entry(Some(&gates), 1, 3, 0).installed_through, 0);
        gates.install(1).await.adopted(7);
        assert_eq!(admit_entry(Some(&gates), 1, 6, 0).installed_through, 7);
        assert_eq!(admit_entry(Some(&gates), 1, 9, 12).installed_through, 7);
        assert_eq!(admit_entry(None, 1, 9, 0).installed_through, 0);
    }

    /// An install cannot start while a started write still holds its permit.
    #[tokio::test]
    async fn an_install_waits_for_a_started_write() {
        let gates = Arc::new(GroupApplyGates::new());
        gates.mount(1);
        let EntryAdmission::Start(permit) = admit_entry(Some(&gates), 1, 6, 0).decision else {
            panic!("open gate must start the entry");
        };
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let write = hold_permit(
            permit,
            Box::pin(async move {
                let _ = released.await;
            }),
        );
        let write = tokio::spawn(write);

        let install = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move { gates.install(1).await.adopted(6) })
        };
        tokio::task::yield_now().await;
        assert!(!install.is_finished(), "install must wait for the write");

        let _ = release.send(());
        write.await.expect("write task");
        install.await.expect("install task");
        assert!(matches!(
            admit_entry(Some(&gates), 1, 6, 0).decision,
            EntryAdmission::Covered
        ));
    }
}
