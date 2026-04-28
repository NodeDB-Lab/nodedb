//! Audit log entries for bootstrap join-token lifecycle events.
//!
//! Every accept/reject/expire transition that happens inside the
//! bootstrap listener writes an `AuditEvent` here. In production the
//! `AuditWriter` trait is implemented by whatever audit-WAL the host
//! crate provides. The cluster crate supplies a no-op implementation
//! for tests and a `VecAuditWriter` for unit verification.
//!
//! **Persist-before-respond contract:** callers MUST call
//! [`AuditWriter::append`] and await/unwrap the result before sending
//! any response to the joiner. This ensures audit records are durable
//! even when the process crashes immediately after sending the response.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Outcome of a bootstrap join attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinOutcome {
    /// Token verified; bundle sent; state transitioned to `InFlight`.
    Accepted,
    /// Token MAC invalid or token not registered.
    Rejected { reason: String },
    /// Token's expiry timestamp has passed.
    TokenExpired,
    /// Token already consumed — replay attempt.
    Replayed,
    /// Bundle delivered and joiner ACK received; state → `Consumed`.
    Consumed,
    /// In-flight dead-man timer fired; state reverted to `Issued`.
    InFlightTimeout,
}

/// One audit record written per bootstrap join event.
#[derive(Debug, Clone)]
pub struct AuditEvent {
    /// Unix milliseconds at event time.
    pub ts_ms: u64,
    /// SHA-256 of the token (never the raw token).
    pub token_hash: [u8; 32],
    /// Remote address of the connecting joiner (may be `None` for
    /// internally-generated events like timeouts).
    pub joiner_addr: Option<SocketAddr>,
    /// Node id the joiner claimed in the request.
    pub claimed_node_id: u64,
    pub outcome: JoinOutcome,
}

impl AuditEvent {
    pub fn new(
        token_hash: [u8; 32],
        joiner_addr: Option<SocketAddr>,
        claimed_node_id: u64,
        outcome: JoinOutcome,
    ) -> Self {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            ts_ms,
            token_hash,
            joiner_addr,
            claimed_node_id,
            outcome,
        }
    }
}

/// Abstraction over where audit events are persisted.
///
/// Implementations MUST be synchronous and durable before returning so
/// callers can safely respond to joiners knowing the audit record is on
/// disk. The contract is: `append` returns only when the event is fsync'd
/// (or the underlying log has accepted it under its own durability
/// guarantee).
pub trait AuditWriter: Send + Sync + 'static {
    fn append(&self, event: AuditEvent);
}

/// No-op writer used when no audit log is configured.
#[derive(Default, Clone)]
pub struct NoopAuditWriter;

impl AuditWriter for NoopAuditWriter {
    fn append(&self, _event: AuditEvent) {}
}

/// In-memory writer for tests — accumulates events in a `Vec` behind
/// a `Mutex` so tests can inspect what was logged.
#[derive(Default, Clone)]
pub struct VecAuditWriter {
    events: Arc<Mutex<Vec<AuditEvent>>>,
}

impl VecAuditWriter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drain all events for inspection.
    pub fn drain(&self) -> Vec<AuditEvent> {
        let mut guard = self.events.lock().expect("audit lock poisoned");
        std::mem::take(&mut *guard)
    }

    /// Return a snapshot of all events without draining.
    pub fn snapshot(&self) -> Vec<AuditEvent> {
        self.events.lock().expect("audit lock poisoned").clone()
    }
}

impl AuditWriter for VecAuditWriter {
    fn append(&self, event: AuditEvent) {
        self.events.lock().expect("audit lock poisoned").push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vec_writer_accumulates_and_drains() {
        let w = VecAuditWriter::new();
        w.append(AuditEvent::new([1u8; 32], None, 1, JoinOutcome::Accepted));
        w.append(AuditEvent::new(
            [2u8; 32],
            None,
            2,
            JoinOutcome::Rejected {
                reason: "bad mac".into(),
            },
        ));
        let events = w.drain();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].outcome, JoinOutcome::Accepted);
        assert!(matches!(events[1].outcome, JoinOutcome::Rejected { .. }));
        // Drain empties the buffer.
        assert!(w.drain().is_empty());
    }

    #[test]
    fn noop_writer_does_not_panic() {
        let w = NoopAuditWriter;
        w.append(AuditEvent::new([0u8; 32], None, 0, JoinOutcome::Consumed));
    }
}
