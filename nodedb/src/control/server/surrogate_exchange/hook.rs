// SPDX-License-Identifier: BUSL-1.1

//! `RegistryAssignRemoteSurrogate` bridges the cluster `AssignSurrogate`
//! request to the node-local surrogate authority.
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the assign logic
//! lives here and is exposed to the transport via the
//! [`nodedb_cluster::AssignRemoteSurrogate`] hook. The `RaftLoop` is built
//! `with_assign_remote_surrogate(Arc::new(RegistryAssignRemoteSurrogate { .. }))`.
//!
//! # What it does
//!
//! This handler only ever runs on the home vShard's LEADER. The coordinator
//! routes each batch of keys there, so the assign is local to the keys' home.
//!
//! The handler works to the request's own `deadline_remaining_ms`, never to
//! this node's default budget. A request whose budget is spent is refused
//! before it runs, with a definite `ExpiredBeforeExecution`.
//!
//! A request with `lookup_only` set takes the READ-ONLY branch:
//! `SurrogateAssigner::lookup_bound_many`, which never allocates and never
//! writes. A miss answers `None` with no error. The key names no existing
//! row, which is an answer, not a failure.
//!
//! Otherwise the leader runs `authority::assign_many_at_home`:
//! - A binding its catalog holds is the key's answer.
//! - Every other key is minted in one `SurrogateBind` entry through the home
//!   vShard's Raft log, and its winner is read back after the apply.
//!
//! The leader leads the keys' collection home, so each value is the
//! AUTHORITATIVE surrogate every owner stores: first-wins and idempotent. An
//! error crosses as its typed wire class, never a silent drop.
//!
//! # Plane discipline
//!
//! This runs on the leader's Control Plane (the Tokio transport reactor). It
//! neither touches storage I/O or io_uring directly nor spawns a Data-Plane
//! task.

use std::sync::Arc;

use nodedb_cluster::{AssignSurrogateRequest, AssignSurrogateResponse, TypedClusterError};

use crate::control::cluster::execution_error_wire::execution_error_to_typed;
use crate::control::cluster::forwarded_budget::hook_deadline;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};

/// `nodedb`-side implementation of [`nodedb_cluster::AssignRemoteSurrogate`].
///
/// Holds the node's [`SharedState`] so it can reach the `SurrogateAssigner`.
/// The coordinator only routes here when this node leads the keys' home
/// vShard, so the local value is the authoritative one.
pub struct RegistryAssignRemoteSurrogate {
    /// Shared node state, the source of the local `SurrogateAssigner`.
    state: Arc<SharedState>,
}

impl RegistryAssignRemoteSurrogate {
    /// Build an assigner hook over `state`.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    /// One answer per key of `req`, in its order, or the typed error.
    async fn answer(
        &self,
        req: AssignSurrogateRequest,
    ) -> Result<Vec<Option<u32>>, TypedClusterError> {
        let deadline = leader_deadline(&req)?;
        let tenant_id = TenantId::new(req.tenant_id);
        // The request carries the bare catalog name.
        let key = nodedb_types::CollectionKey::from_bare(
            DatabaseId::from(req.database_id),
            &req.collection,
        );
        let pks: Vec<&[u8]> = req.pks.iter().map(|pk| pk.as_slice()).collect();

        if req.lookup_only {
            let found = self
                .state
                .surrogate_assigner
                .lookup_bound_many(key, tenant_id, &pks)
                .map_err(execution_error_to_typed)?;
            return Ok(found
                .into_iter()
                .map(|hit| hit.map(|surrogate| surrogate.as_u32()))
                .collect());
        }

        let bound = super::authority::assign_many_at_home(
            &self.state,
            VShardId::new(req.vshard_id),
            key,
            tenant_id,
            &pks,
            deadline,
        )
        .await
        .map_err(execution_error_to_typed)?;
        Ok(bound
            .into_iter()
            .map(|surrogate| Some(surrogate.as_u32()))
            .collect())
    }
}

/// The deadline the leader works to for `req`: the budget the coordinator
/// sent, measured from its arrival here. A spent budget is a definite
/// refusal.
fn leader_deadline(
    req: &AssignSurrogateRequest,
) -> Result<tokio::time::Instant, TypedClusterError> {
    hook_deadline(req.deadline_remaining_ms)
}

#[async_trait::async_trait]
impl nodedb_cluster::AssignRemoteSurrogate for RegistryAssignRemoteSurrogate {
    async fn on_assign_surrogate(&self, req: AssignSurrogateRequest) -> AssignSurrogateResponse {
        match self.answer(req).await {
            Ok(surrogates) => AssignSurrogateResponse {
                surrogates,
                error: None,
            },
            Err(error) => AssignSurrogateResponse {
                surrogates: Vec::new(),
                error: Some(error),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nodedb_types::WireBytes;

    use super::*;
    use crate::bridge::envelope::ErrorCode;

    fn request(deadline_remaining_ms: u64) -> AssignSurrogateRequest {
        AssignSurrogateRequest {
            vshard_id: 3,
            database_id: 1,
            tenant_id: 2,
            collection: "people".into(),
            pks: vec![WireBytes::from(b"alice".to_vec())],
            deadline_remaining_ms,
            trace_id: [0u8; 16],
            lookup_only: false,
        }
    }

    /// The leader's deadline is the request's budget, not the leader's own
    /// default. A 50 ms budget ends within 50 ms of its arrival.
    #[tokio::test]
    async fn the_leader_deadline_is_the_request_budget() {
        let before = tokio::time::Instant::now();
        let deadline = leader_deadline(&request(50)).expect("a live budget");
        assert!(deadline >= before + Duration::from_millis(50));
        assert!(deadline <= tokio::time::Instant::now() + Duration::from_millis(50));
    }

    /// A long budget is kept as it arrived, past the transport's short RPC
    /// timeout.
    #[tokio::test]
    async fn a_long_request_budget_is_not_shortened() {
        let before = tokio::time::Instant::now();
        let deadline = leader_deadline(&request(120_000)).expect("a live budget");
        assert!(deadline >= before + Duration::from_secs(120));
    }

    /// A spent budget is refused before the leader runs anything.
    #[test]
    fn a_spent_request_budget_is_refused() {
        let refusal = leader_deadline(&request(0)).expect_err("a spent budget is refused");
        let crate::Error::DataPlane(code) = crate::Error::from(refusal) else {
            panic!("expected a Data-Plane verdict");
        };
        assert_eq!(code, ErrorCode::ExpiredBeforeExecution);
    }
}
