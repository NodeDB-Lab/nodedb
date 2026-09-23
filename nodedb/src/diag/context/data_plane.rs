// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for Data-Plane and cross-shard capture sites.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// What had already happened to the write whose response the Data Plane
/// could not deliver. A stable class, never a request id, so a saturated
/// response ring collapses into one report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LostResponseWrite {
    /// The batch transaction committed before the response was lost, so the
    /// write is durable while the caller will only ever see a deadline.
    Committed,
    /// The batch transaction was rolled back, so nothing was applied and the
    /// caller's deadline matches reality — only the answer itself is gone.
    RolledBack,
}

impl LostResponseWrite {
    pub(in crate::diag) fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolledBack => "rolled_back",
        }
    }
}

/// A Data-Plane core finished a write but could not hand its response back to
/// the Control Plane, because the bounded response ring refused the push.
pub(in crate::diag) struct DataPlaneResponseLost {
    /// Core whose response ring refused the push.
    pub core_id: usize,
    pub write: LostResponseWrite,
}

impl DomainContext for DataPlaneResponseLost {
    fn domain_kind(&self) -> &'static str {
        "nodedb.data_plane_response_lost"
    }

    fn grouping_key(&self) -> String {
        // Write's fate names the bug; core id is the occurrence, not the key.
        format!("write={}", self.write.as_str())
    }

    fn to_json(&self) -> Value {
        json!({
            "core_id": self.core_id,
            "write": self.write.as_str(),
            "why_fatal": "the response ring is the only channel a Data-Plane core has to \
                          report an outcome. A dropped response leaves the caller waiting \
                          until its deadline and then reporting a timeout — and when the \
                          batch had already committed, that timeout names a write which \
                          IS durable, so a client that retries on timeout double-applies \
                          it and a client that compensates erases a committed row",
            "operator_action": "the ring only refuses a push when the Control Plane stopped \
                                 draining it: look for a stalled response poller or a \
                                 disconnected bridge consumer on the named core, not for a \
                                 storage fault",
        })
    }
}

/// A Calvin cross-shard transaction's completion wait timed out with no
/// signal for why the transaction never completed.
pub(in crate::diag) struct CalvinCompletionTimeout {
    pub epoch: u64,
    pub position: u32,
    pub participants: usize,
    pub timeout_secs: u64,
}

impl DomainContext for CalvinCompletionTimeout {
    fn domain_kind(&self) -> &'static str {
        "nodedb.calvin_completion_timeout"
    }

    fn grouping_key(&self) -> String {
        // Coarse and constant: every occurrence is the same bug shape, so
        // epoch/position/participants must not enter the key.
        "completion_timeout".to_owned()
    }

    fn to_json(&self) -> Value {
        json!({
            "epoch": self.epoch,
            "position": self.position,
            "participants": self.participants,
            "timeout_secs": self.timeout_secs,
            "why_fatal": "this timeout is the only signal a Calvin-routed write ever \
                          produces for a completion ack that never arrived; the caller \
                          sees a generic internal error with no indication of which \
                          participant or stage stalled, and the write's outcome is \
                          unknown to the client",
            "operator_action": "check the sequencer-group leader and the listed \
                                 participant shards for a stalled scheduler, a lost \
                                 CompletionAck proposal, or a network partition between \
                                 them",
        })
    }
}

/// A Calvin scheduler held a sequenced txn unapplied and halted its vShard,
/// because a replica-local error left the txn's effect on this replica
/// unknown or missing.
pub(in crate::diag) struct CalvinApplyHalted<'a> {
    pub vshard_id: u32,
    pub epoch: u64,
    pub position: u32,
    /// Halt reason label (`dispatch_refused`, `flush_failed`, ...).
    pub reason: &'a str,
    /// Sub-operation of the txn that failed (`stage`, `flush`, ...).
    pub step: &'a str,
    pub error: &'a str,
}

impl DomainContext for CalvinApplyHalted<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.calvin_apply_halted"
    }

    fn grouping_key(&self) -> String {
        // Reason + step name the bug; the vShard and txn are the occurrence.
        format!("reason={};step={}", self.reason, self.step)
    }

    fn to_json(&self) -> Value {
        json!({
            "vshard_id": self.vshard_id,
            "epoch": self.epoch,
            "position": self.position,
            "reason": self.reason,
            "step": self.step,
            "error": self.error,
            "why_fatal": "every replica must apply every sequenced txn. Marking this \
                          position applied would skip it on this replica alone, so the \
                          scheduler holds it unapplied with its locks and reads no new \
                          sequenced input for this vShard. Calvin writes that touch the \
                          vShard stop completing on this node",
            "operator_action": "fix the named cause (Data Plane core, WAL disk, surrogate \
                                 catalog), then restart the node. The sequencer log stays \
                                 retained, and restart replay applies the held position \
                                 and every later one",
        })
    }
}

/// A Data-Plane core fail-stopped because its state is unknown: a rollback
/// failed part way, or the work owed after a committed record's install
/// failed.
pub(in crate::diag) struct CoreFailStopped<'a> {
    pub core_id: usize,
    /// Cause label (`rollback_failed`, `post_install_failed`).
    pub cause: &'a str,
    pub detail: &'a str,
}

impl DomainContext for CoreFailStopped<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.data_plane_core_fail_stopped"
    }

    fn grouping_key(&self) -> String {
        // The cause names the bug; the core is the occurrence.
        format!("cause={}", self.cause)
    }

    fn to_json(&self) -> Value {
        json!({
            "core_id": self.core_id,
            "cause": self.cause,
            "detail": self.detail,
            "why_fatal": "the core's live state no longer matches what restart replay \
                          rebuilds from the WAL and its published artifacts. Serving it \
                          would answer reads and take writes against state no replica \
                          and no restart reproduces, so the core refuses every request",
            "operator_action": "fix the named cause (the failing undo entry, or the disk \
                                 behind the flush or artifact that failed), then restart \
                                 the node. Restart replay rebuilds the core from the WAL",
        })
    }
}
