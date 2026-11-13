// SPDX-License-Identifier: BUSL-1.1

//! Per-session server-push channels and their registration state.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::time::Instant;

use crate::control::array_sync::OriginArrayInbound;
use crate::control::array_sync::outbound::ArrayFrame;
use crate::control::server::sync::definition_fanout::DefinitionFrame;
use crate::control::server::sync::presence::PresenceFrame;
use crate::event::crdt_sync::types::{ControlFrame, OutboundDelta};

use super::outbound_queue::{OutboundQueue, QueueWake};

/// Outbound delivery channels a session registers for after authenticating,
/// plus the lazily-built inbound array engine.
///
/// Every registration is gated on the session having authenticated, so this
/// starts fully empty and fills in as the handshake completes.
#[derive(Default)]
pub(super) struct SessionChannels {
    pub crdt_delivery: OutboundQueue<OutboundDelta>,
    pub crdt_control: OutboundQueue<ControlFrame>,
    pub crdt_registered: bool,

    /// Definition notices. Registered only while the principal is a
    /// superuser or tenant admin.
    pub admin_notices: OutboundQueue<OutboundDelta>,
    pub admin_notices_registered: bool,

    pub presence: OutboundQueue<PresenceFrame>,
    pub presence_registered: bool,

    /// Built lazily after the handshake establishes the session tenant.
    pub array_inbound: Option<Arc<OriginArrayInbound>>,
    pub array_delivery: OutboundQueue<ArrayFrame>,
    pub array_delivery_registered: bool,

    pub definition_sync: OutboundQueue<DefinitionFrame>,
    pub definition_sync_registered: bool,

    /// Per collection, the row-queue position its last collection resync
    /// covers. A row push queued before it was queued before the resync's
    /// snapshot read began, so the snapshot holds its write, and it does not
    /// go out again.
    pub resync_fences: HashMap<String, u64>,
    /// Per definition family, the admin-queue position its last resync
    /// covers, in the same sense.
    pub admin_fences: HashMap<String, u64>,
    /// When each collection resync of the recent window went out, oldest
    /// first.
    pub recent_resyncs: VecDeque<Instant>,
    /// When the loop wakes to retry resyncs this wake had no room for.
    pub resync_retry_at: Option<Instant>,
}

impl SessionChannels {
    /// Whether every outbound queue is empty.
    pub(super) fn outbound_empty(&self) -> bool {
        self.crdt_delivery.is_empty()
            && self.crdt_control.is_empty()
            && self.admin_notices.is_empty()
            && self.presence.is_empty()
            && self.array_delivery.is_empty()
            && self.definition_sync.is_empty()
    }

    /// Wait until any outbound queue has an item, or one closes, or a
    /// resync retry is due. The item stays held on its queue for the pump.
    pub(super) async fn wait_outbound(&mut self) -> QueueWake {
        let retry_at = self.resync_retry_at;
        tokio::select! {
            () = retry_due(retry_at) => QueueWake::Ready,
            wake = self.crdt_delivery.wait() => wake,
            wake = self.crdt_control.wait() => wake,
            wake = self.admin_notices.wait() => wake,
            wake = self.presence.wait() => wake,
            wake = self.array_delivery.wait() => wake,
            wake = self.definition_sync.wait() => wake,
        }
    }
}

/// Resolve at `retry_at`. Pending forever when no retry is scheduled.
async fn retry_due(retry_at: Option<Instant>) {
    match retry_at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Whether the session loop keeps running after a step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Flow {
    Continue,
    Break,
}

impl Flow {
    /// `Break` when the send failed, `Continue` otherwise.
    pub(super) fn from_send(sent: bool) -> Self {
        if sent { Self::Continue } else { Self::Break }
    }
}
