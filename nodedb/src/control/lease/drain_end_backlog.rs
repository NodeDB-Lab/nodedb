// SPDX-License-Identifier: BUSL-1.1

//! Drain ends that wait outside the release queue.
//!
//! A drain has no expiry, so a drain end must not depend on a queue slot. A
//! full queue hands the end to this set, and the releaser task drains it.
//!
//! The set is keyed by descriptor and owner. A second end of one drain adds
//! nothing, so the set holds at most one entry per drain this node abandoned.
//! That is the bound, and it is why a set replaces a larger queue.

use std::collections::HashSet;
use std::sync::Mutex;

use nodedb_cluster::{DescriptorId, DrainOwner};
use tokio::sync::Notify;

/// One drain to end.
pub(super) type DrainEnd = (DescriptorId, DrainOwner);

#[derive(Default)]
struct State {
    ends: HashSet<DrainEnd>,
    /// The releaser task swept the set for the last time.
    closed: bool,
}

/// The drain ends waiting for the releaser task.
#[derive(Default)]
pub(super) struct DrainEndBacklog {
    state: Mutex<State>,
    wake: Notify,
}

impl DrainEndBacklog {
    /// Keep `end` for the releaser task. `Err` returns it once the task swept
    /// the set for the last time: no task will propose it.
    pub(super) fn hold(&self, end: DrainEnd) -> Result<(), DrainEnd> {
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.closed {
                return Err(end);
            }
            state.ends.insert(end);
        }
        self.wake.notify_one();
        Ok(())
    }

    /// Every waiting end. The set is left empty.
    pub(super) fn take(&self) -> HashSet<DrainEnd> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut state.ends)
    }

    /// Every waiting end, and refuse later ones.
    pub(super) fn close(&self) -> HashSet<DrainEnd> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.closed = true;
        std::mem::take(&mut state.ends)
    }

    /// Resolves once an end was held since the last wake. Cancel-safe: a
    /// dropped wait loses no wake.
    pub(super) async fn woken(&self) {
        self.wake.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::DescriptorKind;

    use super::*;

    fn end(name: &str) -> DrainEnd {
        (
            DescriptorId::new(0, 7, DescriptorKind::Collection, name),
            DrainOwner::Ddl,
        )
    }

    #[test]
    fn one_drain_holds_one_entry() {
        let backlog = DrainEndBacklog::default();
        backlog.hold(end("a")).expect("open");
        backlog.hold(end("a")).expect("open");
        backlog.hold(end("b")).expect("open");
        assert_eq!(backlog.take().len(), 2);
        assert!(backlog.take().is_empty());
    }

    #[test]
    fn a_closed_backlog_refuses_and_returns_the_end() {
        let backlog = DrainEndBacklog::default();
        backlog.hold(end("a")).expect("open");
        assert_eq!(backlog.close().len(), 1);
        assert_eq!(backlog.hold(end("b")), Err(end("b")));
        assert!(backlog.take().is_empty());
    }

    #[tokio::test]
    async fn a_held_end_wakes_a_later_wait() {
        let backlog = DrainEndBacklog::default();
        backlog.hold(end("a")).expect("open");
        tokio::time::timeout(std::time::Duration::from_secs(5), backlog.woken())
            .await
            .expect("the wake is kept for the next wait");
    }
}
