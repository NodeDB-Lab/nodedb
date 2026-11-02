// SPDX-License-Identifier: BUSL-1.1

//! Durable producer fencing for Lite handshakes.

use std::sync::Arc;

use tracing::warn;

use crate::control::state::SharedState;

use super::super::wire::*;
use super::state::SyncSession;

/// Decision returned by the durable fencing logic before building the ack.
pub(super) enum FencingDecision {
    /// Accept with the given producer_id and accepted_epoch.
    Accept {
        producer_id: u64,
        accepted_epoch: u64,
    },
    /// Reject: stale epoch from a cloned / forked device. PERMANENT — the
    /// client's epoch is behind the durable record; it must regenerate its
    /// LiteId. Surfaced to the client as `fork_detected = true`.
    Reject,
    /// Reject: a transient server-side error (registry I/O, Raft propose
    /// failure / leader mid-election). NOT a fork — the client retries
    /// the handshake. Surfaced as `success = false, fork_detected = false`
    /// so the client never wipes its state over a momentary server hiccup.
    RejectTransient,
}

#[cfg(test)]
fn producer_owner_matches(
    registration: &crate::control::sync_producer::ProducerRegistration,
    tenant_id: u64,
    user_id: u64,
) -> bool {
    registration.tenant_id == tenant_id && registration.user_id == user_id
}

impl SyncSession {
    /// Attempt to make a durable fencing decision via `SyncProducerRegistry`.
    ///
    /// Returns `Some(FencingDecision)` when the msg is a Lite handshake
    /// (`!lite_id.is_empty() && epoch > 0`) and a registry is available via
    /// `shared`.  Returns `None` when:
    ///
    /// * The msg is not a Lite handshake (no Lite identity).
    /// * No `SharedState` is present (unit-test path) — handshake proceeds
    ///   with no fencing.
    /// * `shared` has no `producer_registry` — handshake proceeds with no fencing.
    ///
    /// On registry operation errors the decision is `Reject` (fail-closed) rather
    /// than silently accepting.
    pub(super) async fn durable_fencing_decision(
        &self,
        msg: &HandshakeMsg,
        shared: Option<&Arc<SharedState>>,
        tenant_id: u64,
        user_id: u64,
    ) -> Option<FencingDecision> {
        if msg.lite_id.is_empty() || msg.epoch == 0 {
            return None;
        }

        let registry = shared.and_then(|s| s.producer_registry.as_deref());

        match registry {
            Some(reg) => {
                // `shared` is always `Some` when `registry` is `Some` (the
                // registry was obtained via `shared.and_then(...)`); the `?`
                // is only to recover the handle.
                let shared_ref = shared?;

                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                let existing = match reg.get_or_register(
                    &msg.lite_id,
                    tenant_id,
                    user_id,
                    msg.epoch,
                    now_ms,
                ) {
                    Ok((registration, _created)) => registration,
                    Err(crate::Error::BadRequest { .. }) => {
                        warn!(
                            session = %self.session_id,
                            lite_id = %msg.lite_id,
                            authenticated_tenant = tenant_id,
                            authenticated_user = user_id,
                            "sync producer owner mismatch"
                        );
                        return Some(FencingDecision::Reject);
                    }
                    Err(e) => {
                        warn!(
                            session = %self.session_id,
                            lite_id = %msg.lite_id,
                            error = %e,
                            "sync handshake: atomic producer registration failed; rejecting as retryable"
                        );
                        return Some(FencingDecision::RejectTransient);
                    }
                };

                // Propose on both creation and retry. This closes the crash/error
                // window between the local durable row and Raft replication;
                // duplicate identical registrations are apply-idempotent.
                if let Err(e) = crate::control::metadata_proposer::propose_sync_producer_register(
                    shared_ref.as_ref(),
                    &msg.lite_id,
                    existing.producer_id,
                    existing.tenant_id,
                    existing.user_id,
                    existing.current_epoch,
                    existing.created_ms,
                )
                .await
                {
                    warn!(
                        session = %self.session_id,
                        lite_id = %msg.lite_id,
                        error = %e,
                        "sync handshake: propose_sync_producer_register failed; rejecting as retryable"
                    );
                    return Some(FencingDecision::RejectTransient);
                }

                if msg.epoch < existing.current_epoch {
                    warn!(
                        session = %self.session_id,
                        lite_id = %msg.lite_id,
                        client_epoch = msg.epoch,
                        current_epoch = existing.current_epoch,
                        "FORK DETECTED: client epoch is behind persisted epoch"
                    );
                    return Some(FencingDecision::Reject);
                }

                if msg.epoch > existing.current_epoch
                    && let Err(e) = reg.fence(&msg.lite_id, msg.epoch)
                {
                    warn!(
                        session = %self.session_id,
                        lite_id = %msg.lite_id,
                        error = %e,
                        "sync handshake: registry.fence failed; rejecting as retryable"
                    );
                    return Some(FencingDecision::RejectTransient);
                }

                // Re-propose even when the requested epoch already matches the
                // local row. A prior proposal can have failed after the local
                // fence was persisted; the idempotent max-wins Raft entry must
                // reach followers before this node accepts the retry.
                if let Err(e) = crate::control::metadata_proposer::propose_sync_producer_fence(
                    shared_ref.as_ref(),
                    &msg.lite_id,
                    msg.epoch,
                )
                .await
                {
                    warn!(
                        session = %self.session_id,
                        lite_id = %msg.lite_id,
                        error = %e,
                        "sync handshake: propose_sync_producer_fence failed; rejecting as retryable"
                    );
                    return Some(FencingDecision::RejectTransient);
                }

                Some(FencingDecision::Accept {
                    producer_id: existing.producer_id,
                    accepted_epoch: msg.epoch,
                })
            }
            // No registry available: no fencing decision — handshake proceeds.
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::producer_owner_matches;
    use crate::control::security::catalog::SystemCatalog;
    use crate::control::sync_producer::registry::SyncProducerRegistry;

    fn open_registry(dir: &std::path::Path) -> SyncProducerRegistry {
        let catalog = Arc::new(
            SystemCatalog::open(&dir.join("system.redb")).expect("open test system catalog"),
        );
        SyncProducerRegistry::open(catalog).expect("open producer registry")
    }

    /// Uses the registry directly (not via SharedState) to exercise
    /// `durable_fencing_decision` in isolation.
    #[test]
    fn registry_new_lite_id_assigns_producer_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let reg = open_registry(dir.path());

        // Simulate what durable_fencing_decision does for a new lite_id.
        let r = reg.register("device-a", 1, 99, 10, 0).expect("register");
        assert!(r.producer_id > 0);
        assert_eq!(r.current_epoch, 10);
    }

    #[test]
    fn producer_owner_binding_rejects_cross_tenant_and_cross_user_reuse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let reg = open_registry(dir.path());
        let registration = reg.register("owned-device", 7, 11, 1, 0).expect("register");

        assert!(producer_owner_matches(&registration, 7, 11));
        assert!(!producer_owner_matches(&registration, 8, 11));
        assert!(!producer_owner_matches(&registration, 7, 12));
    }

    #[test]
    fn registry_same_epoch_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let reg = open_registry(dir.path());

        let first = reg.register("device-b", 1, 99, 5, 0).expect("register");
        let loaded = reg
            .get("device-b")
            .expect("registry read")
            .expect("registration exists");
        assert_eq!(loaded.producer_id, first.producer_id);
        assert_eq!(loaded.current_epoch, 5);
    }

    #[test]
    fn registry_higher_epoch_fences() {
        let dir = tempfile::tempdir().expect("tempdir");
        let reg = open_registry(dir.path());

        let first = reg.register("device-c", 1, 99, 3, 0).expect("register");
        reg.fence("device-c", 7).expect("fence");
        let loaded = reg
            .get("device-c")
            .expect("registry read")
            .expect("registration exists");
        assert_eq!(loaded.producer_id, first.producer_id);
        assert_eq!(loaded.current_epoch, 7);
    }

    #[test]
    fn registry_lower_epoch_is_stale() {
        let dir = tempfile::tempdir().expect("tempdir");
        let reg = open_registry(dir.path());

        reg.register("device-d", 1, 99, 9, 0).expect("register");
        let loaded = reg
            .get("device-d")
            .expect("registry read")
            .expect("registration exists");
        // A client presenting epoch < current_epoch (9) must be rejected.
        assert!(
            3 < loaded.current_epoch,
            "epoch 3 is stale vs {}",
            loaded.current_epoch
        );
    }
}
