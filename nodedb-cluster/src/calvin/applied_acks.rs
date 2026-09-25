// SPDX-License-Identifier: BUSL-1.1

//! Completion acks this node applied from the sequencer log, with their
//! Raft index.
//!
//! A participant's scheduler on the vShard leader proposes a `CompletionAck`
//! once it applied a transaction. Every other replica of that vShard applies
//! the transaction in its own time. A node that serves authorization state
//! from its replicas needs to know, for each ack in the log, whether its own
//! replica applied the transaction too. The sequencer state machine records
//! each ack here as it applies it. The host crate drains the log and settles
//! each ack against its local schedulers.
//!
//! The log records nothing until the host enables it, so a node that never
//! drains it holds no entries.

use std::collections::VecDeque;
use std::sync::Mutex;

use super::completion::TxnId;

/// One `CompletionAck` applied from the sequencer log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedCompletionAck {
    /// Raft index of the ack in the sequencer group.
    pub index: u64,
    pub txn: TxnId,
    pub vshard_id: u32,
}

/// Applied acks not yet drained by the host. `None` until enabled.
#[derive(Debug, Default)]
pub struct AppliedAckLog {
    acks: Mutex<Option<VecDeque<AppliedCompletionAck>>>,
}

impl AppliedAckLog {
    /// Start recording applied acks.
    pub fn enable(&self) {
        let mut acks = self.acks.lock().unwrap_or_else(|p| p.into_inner());
        if acks.is_none() {
            *acks = Some(VecDeque::new());
        }
    }

    /// Record an ack the sequencer state machine applied at `index`.
    pub fn record(&self, ack: AppliedCompletionAck) {
        if let Some(acks) = self.acks.lock().unwrap_or_else(|p| p.into_inner()).as_mut() {
            acks.push_back(ack);
        }
    }

    /// Take every recorded ack, in log order.
    pub fn drain(&self) -> Vec<AppliedCompletionAck> {
        self.acks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_mut()
            .map(|acks| acks.drain(..).collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ack(index: u64) -> AppliedCompletionAck {
        AppliedCompletionAck {
            index,
            txn: TxnId::new(index, 0),
            vshard_id: 3,
        }
    }

    #[test]
    fn nothing_is_recorded_until_enabled() {
        let log = AppliedAckLog::default();
        log.record(ack(1));
        assert!(log.drain().is_empty());
        log.enable();
        log.record(ack(2));
        log.record(ack(3));
        assert_eq!(log.drain(), vec![ack(2), ack(3)]);
        assert!(log.drain().is_empty());
    }
}
