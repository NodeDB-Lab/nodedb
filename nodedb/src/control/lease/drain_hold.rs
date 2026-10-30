// SPDX-License-Identifier: BUSL-1.1

//! Ownership of started descriptor drains until something else ends them.
//!
//! A drain has no expiry, so a holder that gives up must end it explicitly.
//! [`DrainHolds`] records each drain a caller started. The caller calls
//! [`DrainHolds::keep`] once the entry whose apply ends the drains is
//! committed, or a record that ends them on cancel exists. Every other exit
//! ends them: [`DrainHolds::end`] proposes each end and waits for it, and a
//! drop (an early return, a cancelled future) hands each end to the
//! background lease releaser. Ending a drain that is not active is a no-op on
//! every node, so ending one twice is harmless.

use nodedb_cluster::{DescriptorId, DrainOwner};

use crate::control::state::SharedState;

use super::releaser::ReleaseRequest;

/// Drains a caller started and still owns.
#[must_use = "dropping the holds ends their drains"]
pub(crate) struct DrainHolds<'a> {
    shared: &'a SharedState,
    held: Vec<(DescriptorId, DrainOwner)>,
}

impl<'a> DrainHolds<'a> {
    pub(crate) fn new(shared: &'a SharedState) -> Self {
        Self {
            shared,
            held: Vec::new(),
        }
    }

    /// Own `owner`'s drain on `descriptor_id`. Call it before proposing the
    /// drain start: a start whose outcome is unknown can still apply.
    pub(crate) fn hold(&mut self, descriptor_id: DescriptorId, owner: DrainOwner) {
        self.held.push((descriptor_id, owner));
    }

    /// Stop owning the drains. Another path ends them.
    pub(crate) fn keep(mut self) {
        self.held.clear();
    }

    /// End every held drain and wait until each end applied here. An end
    /// that does not apply goes to the background releaser, which retries it
    /// and records a drain it never ends.
    pub(crate) async fn end(mut self) {
        for (descriptor_id, owner) in std::mem::take(&mut self.held) {
            if let Err(error) =
                super::end_drain_async(self.shared, descriptor_id.clone(), owner.clone()).await
            {
                tracing::debug!(
                    ?descriptor_id,
                    ?owner,
                    %error,
                    "descriptor drain end failed; the background releaser retries it"
                );
                submit_end(self.shared, descriptor_id, owner);
            }
        }
    }
}

impl Drop for DrainHolds<'_> {
    fn drop(&mut self) {
        for (descriptor_id, owner) in std::mem::take(&mut self.held) {
            submit_end(self.shared, descriptor_id, owner);
        }
    }
}

fn submit_end(shared: &SharedState, descriptor_id: DescriptorId, owner: DrainOwner) {
    shared
        .lease_runtime
        .releaser
        .submit(ReleaseRequest::DrainEnd {
            descriptor_id,
            owner,
        });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nodedb_cluster::DescriptorKind;

    use super::*;

    /// Holds dropped without `keep` or `end`, as an early return or a
    /// cancelled future drops them, end their drains in the background.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_holds_end_their_drains() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let id = DescriptorId::new(0, 7, DescriptorKind::Collection, "orders");
        super::super::drain_for_ddl_async(
            state,
            id.clone(),
            3,
            crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT,
            0,
        )
        .await
        .expect("an unleased descriptor drains at once");
        assert!(state.lease_drain.is_draining(&id, 3));

        let mut holds = DrainHolds::new(state);
        holds.hold(id.clone(), DrainOwner::Ddl);
        drop(holds);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while state.lease_drain.is_draining(&id, 3) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "a dropped hold left its drain in force"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cluster.shutdown().await;
    }

    /// Kept holds leave their drains to whatever ends them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kept_holds_leave_their_drains() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let id = DescriptorId::new(0, 7, DescriptorKind::Collection, "kept");
        super::super::drain_for_ddl_async(
            state,
            id.clone(),
            3,
            crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT,
            0,
        )
        .await
        .expect("an unleased descriptor drains at once");

        let mut holds = DrainHolds::new(state);
        holds.hold(id.clone(), DrainOwner::Ddl);
        holds.keep();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            state.lease_drain.is_draining(&id, 3),
            "a kept hold must not end its drain"
        );
        cluster.shutdown().await;
    }
}
