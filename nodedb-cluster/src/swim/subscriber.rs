// SPDX-License-Identifier: BUSL-1.1

//! `MembershipSubscriber` — hook fired whenever SWIM observes a
//! member state transition.
//!
//! The failure detector invokes every registered subscriber *after*
//! applying an update to the [`MembershipList`](super::membership::MembershipList)
//! and dissemination queue, so subscribers see the post-merge view.
//!
//! Subscribers are synchronous and must not block — they typically do
//! cheap in-memory bookkeeping (e.g. clearing a routing leader hint).
//! Heavier work belongs on a dedicated task the subscriber spawns
//! itself.
//!
//! ## Lifecycle
//!
//! - `old = None` means "first time we've seen this node" (insert).
//! - `old = Some(state)` means the member existed and transitioned to
//!   a strictly different `new` state. The detector never calls the
//!   hook for no-op reapplies.
//! - `Left` is terminal — after it fires once the member is gone.

use nodedb_types::NodeId;

use super::member::MemberState;

/// Hook trait for observers that react to SWIM membership changes.
pub trait MembershipSubscriber: Send + Sync {
    /// Called after the membership list has accepted a state change
    /// for `node_id`. `old` is `None` on first-time insert.
    fn on_state_change(&self, node_id: &NodeId, old: Option<MemberState>, new: MemberState);

    /// Called with the incarnation the transition landed at, immediately
    /// after [`on_state_change`](Self::on_state_change). Hooks that need a
    /// fencing value (lease-holder liveness) implement this; the default
    /// keeps every existing hook unchanged.
    fn on_state_change_with_incarnation(
        &self,
        _node_id: &NodeId,
        _new: MemberState,
        _incarnation: u64,
    ) {
    }
}
