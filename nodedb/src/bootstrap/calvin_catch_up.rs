// SPDX-License-Identifier: BUSL-1.1

//! Calvin scheduler catch-up gate.
//!
//! Each Calvin scheduler starts with a rebuild target: the highest epoch its
//! recovery found applied for the vShard. Until its fully-applied watermark
//! reaches that target, the vShard re-applies epochs from the sequencer log.
//! A gateway opened meanwhile serves a vShard whose Calvin state is behind
//! what this node acknowledged before the restart.
//!
//! This gate holds startup until every scheduler running on this node reports
//! caught up. A scheduler with nothing to rebuild reports at once, and a node
//! that runs no scheduler passes at once. A vShard whose scheduler halted can
//! never catch up, so its halt fails the wait at once. The wait shares the
//! data-group recovery bound and fails closed when the bound expires.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info};

use crate::bootstrap::data_group_recovery::DATA_GROUP_RECOVERY_TIMEOUT;
use crate::control::cluster::CalvinApplyHalt;
use crate::control::state::SharedState;

/// How often the caught-up registry is re-read while waiting. Catch-up drains
/// run on a scheduler tick of about a second, so a coarse poll costs nothing.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Most lagging vShards one error message names. The count names the rest.
const MAX_NAMED_VSHARDS: usize = 16;

/// Why the scheduler catch-up wait cannot pass yet.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CatchUpWait {
    /// These vShards' schedulers are behind their rebuild target.
    Lagging(Vec<u32>),
    /// A lagging vShard's scheduler halted, so it will never catch up.
    Halted(CalvinApplyHalt),
}

/// The wait state for `lagging` vShards, given the node's first apply halt.
fn catch_up_wait(lagging: Vec<u32>, halt: Option<&CalvinApplyHalt>) -> Option<CatchUpWait> {
    if lagging.is_empty() {
        return None;
    }
    if let Some(halt) = halt
        && lagging.binary_search(&halt.vshard_id).is_ok()
    {
        return Some(CatchUpWait::Halted(halt.clone()));
    }
    Some(CatchUpWait::Lagging(lagging))
}

/// Name at most [`MAX_NAMED_VSHARDS`] of `lagging`, then count the rest.
fn describe_lagging(lagging: &[u32]) -> String {
    let named = lagging
        .iter()
        .take(MAX_NAMED_VSHARDS)
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    match lagging.len().checked_sub(MAX_NAMED_VSHARDS) {
        Some(rest) if rest > 0 => format!("vShards {named} and {rest} more"),
        _ => format!("vShards {named}"),
    }
}

/// Hold startup until every Calvin scheduler on this node reaches its rebuild
/// target.
pub async fn await_calvin_catch_up(shared: &Arc<SharedState>) -> anyhow::Result<()> {
    let registry = &shared.calvin.caught_up;
    let deadline = Instant::now() + DATA_GROUP_RECOVERY_TIMEOUT;

    loop {
        let halt = shared.sequencer_halt.apply_halt().report();
        match catch_up_wait(registry.lagging(), halt) {
            None => {
                info!("every calvin scheduler reached its rebuild target");
                return Ok(());
            }
            Some(CatchUpWait::Halted(halt)) => {
                return Err(anyhow::anyhow!(
                    "calvin scheduler for vShard {} halted at epoch {} position {} \
                     ({}, step {}): {}; it cannot reach its rebuild target",
                    halt.vshard_id,
                    halt.epoch,
                    halt.position,
                    halt.reason,
                    halt.step,
                    halt.error
                ));
            }
            Some(CatchUpWait::Lagging(lagging)) => {
                if Instant::now() >= deadline {
                    return Err(anyhow::anyhow!(
                        "calvin scheduler catch-up timeout after \
                         {DATA_GROUP_RECOVERY_TIMEOUT:?}: {} behind their rebuild target",
                        describe_lagging(&lagging)
                    ));
                }
                debug!(
                    lagging = lagging.len(),
                    "waiting for calvin schedulers to reach their rebuild target"
                );
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn halt(vshard_id: u32) -> CalvinApplyHalt {
        CalvinApplyHalt {
            vshard_id,
            epoch: 9,
            position: 1,
            reason: "flush_failed",
            step: "flush",
            error: "flush returned Error".to_string(),
        }
    }

    #[test]
    fn no_lagging_scheduler_passes() {
        assert_eq!(catch_up_wait(Vec::new(), None), None);
        assert_eq!(catch_up_wait(Vec::new(), Some(&halt(3))), None);
    }

    #[test]
    fn a_lagging_scheduler_waits() {
        assert_eq!(
            catch_up_wait(vec![2, 5], None),
            Some(CatchUpWait::Lagging(vec![2, 5]))
        );
    }

    #[test]
    fn a_halt_on_another_vshard_keeps_waiting() {
        assert_eq!(
            catch_up_wait(vec![2, 5], Some(&halt(3))),
            Some(CatchUpWait::Lagging(vec![2, 5]))
        );
    }

    #[test]
    fn a_halted_lagging_scheduler_fails_at_once() {
        assert_eq!(
            catch_up_wait(vec![2, 5], Some(&halt(5))),
            Some(CatchUpWait::Halted(halt(5)))
        );
    }

    #[test]
    fn a_long_lagging_list_names_the_first_vshards_and_counts_the_rest() {
        let lagging: Vec<u32> = (0..20).collect();
        let text = describe_lagging(&lagging);
        assert!(text.starts_with("vShards 0, 1, 2"), "{text}");
        assert!(text.ends_with("15 and 4 more"), "{text}");
        assert_eq!(describe_lagging(&[7]), "vShards 7");
    }

    /// A node that runs no scheduler, as a fresh single node does before its
    /// first sequenced write, passes at once.
    #[tokio::test]
    async fn a_node_with_no_scheduler_passes_at_once() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal = Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("catch-up.wal"))
                .expect("open wal"),
        );
        let (dispatcher, _data_sides) = crate::bridge::Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("build shared state");
        await_calvin_catch_up(&shared)
            .await
            .expect("no scheduler holds the gate");
    }
}
