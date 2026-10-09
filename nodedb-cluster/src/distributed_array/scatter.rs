// SPDX-License-Identifier: BUSL-1.1

//! Fan-out helpers for array distributed operations.
//!
//! Two entry points:
//! - `fan_out` — broadcast one request to all listed shards (slice, agg,
//!   delete, surrogate scan).
//! - `fan_out_partitioned` — send a different payload to each shard
//!   (used by `coord_put` where cells are routed by Hilbert prefix).
//!
//! Both functions dispatch concurrently via `FuturesUnordered`, apply a
//! per-shard `tokio::time::timeout`, and wrap each call in the cluster
//! `CircuitBreaker`. Any shard failure is propagated as `Err` — partial
//! results are not silently dropped.

use std::sync::Arc;

use futures::StreamExt;

use crate::circuit_breaker::CircuitBreaker;
use crate::error::{ClusterError, Result};
use crate::wire::{VShardEnvelope, VShardMessageType};

/// Dispatch one shard call with one automatic retry on `WrongOwner`.
///
/// When a shard returns `WrongOwner` it means the coordinator's routing table
/// was stale at the time the request was built. The `ShardRpcDispatch`
/// implementor holds a live reference to the `RoutingTable` (updated by the
/// `CacheApplier` on every committed `RoutingChange`), so re-issuing the same
/// envelope lets the implementor route it to the current owner without any
/// explicit routing-refresh API call on the coordinator side. A second
/// `WrongOwner` propagates as an error — we do not loop.
async fn call_with_wrong_owner_retry(
    dispatch: &Arc<dyn ShardRpcDispatch>,
    env: VShardEnvelope,
    timeout_ms: u64,
) -> std::result::Result<VShardEnvelope, ClusterError> {
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        dispatch.call(env.clone(), timeout_ms),
    )
    .await;

    match result {
        Ok(Ok(resp)) => return Ok(resp),
        Ok(Err(ClusterError::WrongOwner { .. })) => {
            // Retry once: the dispatch impl will re-read the live routing table.
        }
        Ok(Err(e)) => return Err(e),
        Err(_elapsed) => {
            return Err(ClusterError::ShardTimeout {
                vshard_id: env.vshard_id,
                elapsed_ms: timeout_ms,
            });
        }
    }

    // Second attempt — propagate whatever error arises, including a second WrongOwner.
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        dispatch.call(env.clone(), timeout_ms),
    )
    .await;

    match result {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(e)) => Err(e),
        Err(_elapsed) => Err(ClusterError::ShardTimeout {
            vshard_id: env.vshard_id,
            elapsed_ms: timeout_ms,
        }),
    }
}

use super::rpc::ShardRpcDispatch;

/// Classify whether a shard error should count against the peer circuit breaker.
///
/// `WrongOwner` is a transient routing/ownership-churn signal, not a peer-health
/// failure: during a rebalance or leadership cut-over a perfectly healthy shard
/// answers `WrongOwner` until the coordinator's routing table catches up (the
/// single retry in `call_with_wrong_owner_retry` already re-reads the live
/// table). Counting it as a liveness failure would open the shared breaker and
/// then fast-fail healthy shards' slice/put/agg/delete with `CircuitOpen`.
/// `WrongOwner` and a typed verdict from a shard that answered are excluded.
/// Every genuine transport/timeout/unreachable error still counts, mirroring
/// `RetryPolicy::is_retryable`'s conservative policy.
fn counts_against_breaker(err: &ClusterError) -> bool {
    match err {
        ClusterError::WrongOwner { .. } => false,
        // A typed verdict comes from a healthy shard that answered.
        ClusterError::DataPlane { .. }
        | ClusterError::ShardExecution { .. }
        | ClusterError::StreamTerminal { .. } => false,
        // An unresponsive peer is exactly what the breaker exists to shed
        // load from, so a shard timeout counts like any other liveness failure.
        ClusterError::ShardTimeout { .. } | ClusterError::Unanswered { .. } => true,
        ClusterError::Raft(_)
        | ClusterError::VShardNotMapped { .. }
        | ClusterError::GroupNotFound { .. }
        | ClusterError::LearnerNotCaughtUp { .. }
        | ClusterError::MigrationInProgress { .. }
        | ClusterError::MigrationPauseBudgetExceeded { .. }
        | ClusterError::NodeUnreachable { .. }
        | ClusterError::GhostNotFound { .. }
        | ClusterError::Transport { .. }
        | ClusterError::Storage { .. }
        | ClusterError::Codec { .. }
        | ClusterError::UnsupportedWireVersion { .. }
        | ClusterError::CircuitOpen { .. }
        | ClusterError::JoinGroupDisappeared { .. }
        | ClusterError::JoinCommitTimeout { .. }
        | ClusterError::ReadIndexNotLeader { .. }
        | ClusterError::ReadIndexTimeout { .. }
        | ClusterError::Config { .. }
        | ClusterError::MigrationCheckpoint(_)
        | ClusterError::MigrationRecovery(_)
        | ClusterError::Calvin(_)
        | ClusterError::SnapshotCrcMismatch { .. }
        | ClusterError::SnapshotOffsetRegression { .. }
        | ClusterError::PartialSnapshotCorrupt { .. }
        | ClusterError::PartialSnapshotCleanupFailed { .. }
        | ClusterError::SnapshotApplyFailed { .. }
        | ClusterError::Mirror(_)
        | ClusterError::BspBarrier(_)
        | ClusterError::VectorGather(_)
        | ClusterError::SpatialGather(_)
        | ClusterError::Bm25Gather(_)
        | ClusterError::TsGather(_)
        | ClusterError::ShufflePush(_)
        | ClusterError::RemoteUntyped { .. } => true,
    }
}

/// Parameters governing a single fan-out round.
pub struct FanOutParams {
    /// Shard IDs to contact (broadcast target list).
    pub shard_ids: Vec<u32>,
    /// Per-shard RPC timeout in milliseconds.
    pub timeout_ms: u64,
    /// Source node ID (used to tag outgoing envelopes).
    pub source_node: u64,
}

/// Parameters for a partitioned fan-out where each shard receives a
/// different payload. `per_shard` entries are `(vshard_id, payload_bytes)`.
pub struct FanOutPartitionedParams {
    /// Per-shard RPC timeout in milliseconds.
    pub timeout_ms: u64,
    /// Source node ID (used to tag outgoing envelopes).
    pub source_node: u64,
}

/// Send `req_bytes` to every shard listed in `params` and collect responses.
///
/// Each shard RPC runs concurrently via `FuturesUnordered`. Per-shard
/// timeouts are enforced by `tokio::time::timeout`. The circuit breaker
/// is checked before each call and updated on success/failure. Any shard
/// error causes the whole fan-out to return `Err` — the coordinator
/// decides whether to retry.
///
/// Returns `Vec<(shard_id, response_payload_bytes)>` in arrival order.
pub async fn fan_out(
    params: &FanOutParams,
    opcode: u32,
    req_bytes: &[u8],
    dispatch: &Arc<dyn ShardRpcDispatch>,
    circuit_breaker: &CircuitBreaker,
) -> Result<Vec<(u32, Vec<u8>)>> {
    if params.shard_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Build one future per shard and collect via FuturesUnordered for
    // true concurrency (no sequential .await loop).
    let mut futs = futures::stream::FuturesUnordered::new();

    for &shard_id in &params.shard_ids {
        // Circuit-breaker gate: treat shard_id as the peer identifier.
        let admission = circuit_breaker.check(shard_id as u64)?;

        let env = VShardEnvelope::new(
            msg_type_from_opcode(opcode)?,
            params.source_node,
            0, // target_node resolved by the dispatch impl
            shard_id,
            req_bytes.to_vec(),
        );
        let timeout_ms = params.timeout_ms;
        let dispatch = Arc::clone(dispatch);
        let cb_shard = shard_id;

        futs.push(async move {
            match call_with_wrong_owner_retry(&dispatch, env, timeout_ms).await {
                Ok(resp) => Ok((cb_shard, admission, resp.payload)),
                Err(e) => Err((cb_shard, admission, e)),
            }
        });
    }

    let mut results = Vec::with_capacity(params.shard_ids.len());
    while let Some(outcome) = futs.next().await {
        match outcome {
            Ok((shard_id, admission, payload)) => {
                circuit_breaker.record_success(shard_id as u64, admission);
                results.push((shard_id, payload));
            }
            Err((shard_id, admission, e)) => {
                if counts_against_breaker(&e) {
                    circuit_breaker.record_failure(shard_id as u64, admission);
                }
                return Err(e);
            }
        }
    }

    Ok(results)
}

/// Send a distinct payload to each shard and collect responses.
///
/// `per_shard` — `(vshard_id, payload_bytes)` pairs, one per target shard.
/// Returns `(shard_id, response_payload_bytes)` in arrival order.
pub async fn fan_out_partitioned(
    params: &FanOutPartitionedParams,
    opcode: u32,
    per_shard: &[(u32, Vec<u8>)],
    dispatch: &Arc<dyn ShardRpcDispatch>,
    circuit_breaker: &CircuitBreaker,
) -> Result<Vec<(u32, Vec<u8>)>> {
    if per_shard.is_empty() {
        return Ok(Vec::new());
    }

    let mut futs = futures::stream::FuturesUnordered::new();

    for (shard_id, payload) in per_shard {
        let admission = circuit_breaker.check(*shard_id as u64)?;

        let env = VShardEnvelope::new(
            msg_type_from_opcode(opcode)?,
            params.source_node,
            0,
            *shard_id,
            payload.clone(),
        );
        let timeout_ms = params.timeout_ms;
        let dispatch = Arc::clone(dispatch);
        let cb_shard = *shard_id;

        futs.push(async move {
            match call_with_wrong_owner_retry(&dispatch, env, timeout_ms).await {
                Ok(resp) => Ok((cb_shard, admission, resp.payload)),
                Err(e) => Err((cb_shard, admission, e)),
            }
        });
    }

    let mut results = Vec::with_capacity(per_shard.len());
    while let Some(outcome) = futs.next().await {
        match outcome {
            Ok((shard_id, admission, payload)) => {
                circuit_breaker.record_success(shard_id as u64, admission);
                results.push((shard_id, payload));
            }
            Err((shard_id, admission, e)) => {
                if counts_against_breaker(&e) {
                    circuit_breaker.record_failure(shard_id as u64, admission);
                }
                return Err(e);
            }
        }
    }

    Ok(results)
}

/// Map an opcode constant to a `VShardMessageType`.
///
/// All array opcodes are in the range 80–89. Any other value is a
/// programming error in the coordinator, not a runtime condition.
fn msg_type_from_opcode(opcode: u32) -> Result<VShardMessageType> {
    match opcode {
        80 => Ok(VShardMessageType::ArrayShardSliceReq),
        81 => Ok(VShardMessageType::ArrayShardSliceResp),
        82 => Ok(VShardMessageType::ArrayShardAggReq),
        83 => Ok(VShardMessageType::ArrayShardAggResp),
        84 => Ok(VShardMessageType::ArrayShardPutReq),
        85 => Ok(VShardMessageType::ArrayShardPutResp),
        86 => Ok(VShardMessageType::ArrayShardDeleteReq),
        87 => Ok(VShardMessageType::ArrayShardDeleteResp),
        88 => Ok(VShardMessageType::ArrayShardSurrogateBitmapReq),
        89 => Ok(VShardMessageType::ArrayShardSurrogateBitmapResp),
        other => Err(ClusterError::Codec {
            detail: format!("msg_type_from_opcode: unknown array opcode {other}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
    use crate::error::{ClusterError, Result};
    use crate::wire::VShardEnvelope;

    use super::super::rpc::ShardRpcDispatch;
    use super::{FanOutParams, FanOutPartitionedParams, fan_out, fan_out_partitioned};

    /// Mock that echoes the request payload back as the response payload,
    /// stamped with the shard's id in the response envelope.
    struct EchoDispatch;

    #[async_trait]
    impl ShardRpcDispatch for EchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            // Echo back a response envelope of the corresponding resp opcode.
            let resp_opcode = req.msg_type as u32 + 1;
            let resp_type = super::msg_type_from_opcode(resp_opcode)?;
            Ok(VShardEnvelope::new(
                resp_type,
                req.target_node,
                req.source_node,
                req.vshard_id,
                req.payload,
            ))
        }
    }

    /// Mock that always returns a transport error.
    struct FailDispatch;

    #[async_trait]
    impl ShardRpcDispatch for FailDispatch {
        async fn call(&self, _req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            Err(ClusterError::Transport {
                detail: "injected failure".into(),
            })
        }
    }

    /// Mock that never resolves, exercising the real `tokio::time::timeout`
    /// expiry path (as opposed to `FailDispatch`, which errors immediately).
    struct HangDispatch;

    #[async_trait]
    impl ShardRpcDispatch for HangDispatch {
        async fn call(&self, _req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            std::future::pending().await
        }
    }

    fn cb() -> CircuitBreaker {
        CircuitBreaker::new(CircuitBreakerConfig::default())
    }

    #[tokio::test]
    async fn fan_out_broadcasts_to_all_shards() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(EchoDispatch);
        let params = FanOutParams {
            shard_ids: vec![0, 1, 2],
            timeout_ms: 1000,
            source_node: 42,
        };
        let req_bytes = b"test-payload";
        let results = fan_out(
            &params,
            super::super::opcodes::ARRAY_SHARD_SLICE_REQ,
            req_bytes,
            &dispatch,
            &cb(),
        )
        .await
        .expect("fan_out should succeed");

        assert_eq!(results.len(), 3);
        // All shards should echo back the same payload.
        for (_, payload) in &results {
            assert_eq!(payload.as_slice(), req_bytes);
        }
        // All three shard IDs should be present.
        let mut ids: Vec<u32> = results.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn fan_out_empty_shards_returns_empty() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(EchoDispatch);
        let params = FanOutParams {
            shard_ids: vec![],
            timeout_ms: 1000,
            source_node: 1,
        };
        let results = fan_out(
            &params,
            super::super::opcodes::ARRAY_SHARD_SLICE_REQ,
            b"",
            &dispatch,
            &cb(),
        )
        .await
        .expect("empty fan_out should succeed");
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn fan_out_propagates_shard_error() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(FailDispatch);
        let params = FanOutParams {
            shard_ids: vec![0, 1],
            timeout_ms: 1000,
            source_node: 1,
        };
        let err = fan_out(
            &params,
            super::super::opcodes::ARRAY_SHARD_SLICE_REQ,
            b"",
            &dispatch,
            &cb(),
        )
        .await
        .expect_err("fan_out should propagate shard failure");
        assert!(
            matches!(err, ClusterError::Transport { .. }),
            "expected Transport error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn fan_out_partitioned_dispatches_distinct_payloads() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(EchoDispatch);
        let params = FanOutPartitionedParams {
            timeout_ms: 1000,
            source_node: 1,
        };
        let per_shard = vec![
            (0u32, b"shard0-data".to_vec()),
            (1u32, b"shard1-data".to_vec()),
        ];
        let results = fan_out_partitioned(
            &params,
            super::super::opcodes::ARRAY_SHARD_PUT_REQ,
            &per_shard,
            &dispatch,
            &cb(),
        )
        .await
        .expect("fan_out_partitioned should succeed");

        assert_eq!(results.len(), 2);
        let mut sorted = results.clone();
        sorted.sort_unstable_by_key(|(id, _)| *id);
        assert_eq!(sorted[0].1, b"shard0-data");
        assert_eq!(sorted[1].1, b"shard1-data");
    }

    #[tokio::test]
    async fn circuit_breaker_open_blocks_fan_out() {
        use crate::circuit_breaker::CircuitBreakerConfig;
        use std::time::Duration;

        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(EchoDispatch);
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            cooldown: Duration::from_secs(60),
        });
        // Trip the breaker for shard 0.
        cb.record_failure(0, crate::circuit_breaker::Admission::Normal);

        let params = FanOutParams {
            shard_ids: vec![0],
            timeout_ms: 1000,
            source_node: 1,
        };
        let err = fan_out(
            &params,
            super::super::opcodes::ARRAY_SHARD_SLICE_REQ,
            b"",
            &dispatch,
            &cb,
        )
        .await
        .expect_err("open circuit should block fan_out");
        assert!(
            matches!(err, ClusterError::CircuitOpen { .. }),
            "expected CircuitOpen, got {err:?}"
        );
    }

    // ── WrongOwner retry tests ────────────────────────────────────────────

    use std::sync::atomic::{AtomicU32, Ordering};

    /// Dispatch that returns `WrongOwner` exactly `fail_count` times, then
    /// echoes the request payload back as a success.
    struct WrongOwnerThenEchoDispatch {
        call_count: Arc<AtomicU32>,
        fail_count: u32,
    }

    #[async_trait]
    impl ShardRpcDispatch for WrongOwnerThenEchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            let n = self.call_count.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_count {
                return Err(ClusterError::WrongOwner {
                    vshard_id: req.vshard_id,
                    expected_owner_node: None,
                });
            }
            let resp_opcode = req.msg_type as u32 + 1;
            let resp_type = super::msg_type_from_opcode(resp_opcode)?;
            Ok(VShardEnvelope::new(
                resp_type,
                req.target_node,
                req.source_node,
                req.vshard_id,
                req.payload,
            ))
        }
    }

    #[tokio::test]
    async fn wrong_owner_triggers_retry_once() {
        // First call returns WrongOwner; retry (second call) succeeds.
        let call_count = Arc::new(AtomicU32::new(0));
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(WrongOwnerThenEchoDispatch {
            call_count: call_count.clone(),
            fail_count: 1,
        });
        let params = FanOutParams {
            shard_ids: vec![0],
            timeout_ms: 1000,
            source_node: 1,
        };
        let result = fan_out(
            &params,
            super::super::opcodes::ARRAY_SHARD_SLICE_REQ,
            b"payload",
            &dispatch,
            &cb(),
        )
        .await
        .expect("fan_out should succeed after one WrongOwner retry");

        assert_eq!(result.len(), 1);
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "must have called dispatch twice"
        );
    }

    #[tokio::test]
    async fn wrong_owner_twice_propagates() {
        // Both attempts return WrongOwner → fan_out surfaces the error.
        let call_count = Arc::new(AtomicU32::new(0));
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(WrongOwnerThenEchoDispatch {
            call_count: call_count.clone(),
            fail_count: 2,
        });
        let params = FanOutParams {
            shard_ids: vec![0],
            timeout_ms: 1000,
            source_node: 1,
        };
        let err = fan_out(
            &params,
            super::super::opcodes::ARRAY_SHARD_SLICE_REQ,
            b"payload",
            &dispatch,
            &cb(),
        )
        .await
        .expect_err("fan_out should propagate WrongOwner when both attempts fail");

        assert!(
            matches!(err, ClusterError::WrongOwner { .. }),
            "expected WrongOwner, got {err:?}"
        );
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "must have called dispatch twice"
        );
    }

    #[tokio::test]
    async fn wrong_owner_does_not_trip_breaker() {
        use crate::circuit_breaker::CircuitState;

        // Both attempts return WrongOwner so the fan-out propagates the error,
        // exercising the failure arm that would otherwise call record_failure.
        let call_count = Arc::new(AtomicU32::new(0));
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(WrongOwnerThenEchoDispatch {
            call_count: call_count.clone(),
            fail_count: 5, // more than the single retry can clear
        });
        let cb = cb();
        let params = FanOutParams {
            shard_ids: vec![0],
            timeout_ms: 1000,
            source_node: 1,
        };
        let err = fan_out(
            &params,
            super::super::opcodes::ARRAY_SHARD_SLICE_REQ,
            b"payload",
            &dispatch,
            &cb,
        )
        .await
        .expect_err("fan_out should propagate WrongOwner");

        assert!(
            matches!(err, ClusterError::WrongOwner { .. }),
            "expected WrongOwner, got {err:?}"
        );
        // WrongOwner is a routing signal, not a health failure: the breaker
        // must not have recorded a failure for the shard.
        assert_eq!(
            cb.failure_count(0),
            0,
            "WrongOwner must not increment the breaker failure count"
        );
        assert_eq!(
            cb.state(0),
            CircuitState::Closed,
            "WrongOwner must leave the breaker Closed"
        );
    }

    #[tokio::test]
    async fn shard_call_expiry_produces_shard_timeout() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(HangDispatch);
        let cb = cb();
        let params = FanOutParams {
            shard_ids: vec![0],
            timeout_ms: 20,
            source_node: 1,
        };
        let err = fan_out(
            &params,
            super::super::opcodes::ARRAY_SHARD_SLICE_REQ,
            b"payload",
            &dispatch,
            &cb,
        )
        .await
        .expect_err("fan_out should surface a timeout once the shard call never resolves");

        match err {
            ClusterError::ShardTimeout {
                vshard_id,
                elapsed_ms,
            } => {
                assert_eq!(vshard_id, 0);
                assert_eq!(elapsed_ms, 20);
            }
            other => panic!("expected ShardTimeout, got {other:?}"),
        }
        // A timeout is a liveness failure and must count against the breaker.
        assert_eq!(
            cb.failure_count(0),
            1,
            "shard timeout must increment the breaker failure count"
        );
    }
}
