// SPDX-License-Identifier: BUSL-1.1

//! Cross-node shuffle (E1/E4a/E4b/E5b), routed-surrogate-exchange (F1b),
//! and routed Calvin-submit (Cv1) RPC bodies.

use crate::error::Result;
use crate::forward::PlanExecutor;
use crate::rpc_codec::{
    AssignSurrogateRequest, AssignSurrogateResponse, ReleaseReservationRequest,
    ReleaseReservationResponse, ReserveReadRequest, ReserveReadResponse,
    ShuffleAggregateConsumeRequest, ShuffleAggregateConsumeResponse, ShuffleConsumeRequest,
    ShuffleConsumeResponse, ShuffleProduceRequest, ShuffleProduceResponse, ShufflePushRequest,
    SubmitCalvinInboxRequest, SubmitCalvinInboxResponse, SubmitCalvinTxnRequest,
    SubmitCalvinTxnResponse, TypedClusterError,
};

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    // Cross-node streaming shuffle (E1) — delegate to the host-crate
    // `ShuffleReceiver` (backed by `nodedb`'s `ShuffleReceiverRegistry`). When
    // no receiver is installed (cluster-only tests / single-node), the request
    // is a no-op and a chunk surfaces a typed "not configured" error.
    pub(super) async fn on_shuffle_request_impl(&self, req: ShufflePushRequest) {
        if let Some(recv) = &self.shuffle_receiver {
            recv.on_shuffle_request(req.shuffle_id, req.part, req.side, req.producer_count)
                .await;
        }
    }

    pub(super) async fn on_shuffle_chunk_impl(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        payload: Vec<u8>,
    ) -> Result<()> {
        match &self.shuffle_receiver {
            Some(recv) => recv.on_shuffle_chunk(shuffle_id, part, side, payload).await,
            None => Err(crate::error::ClusterError::Transport {
                detail: "shuffle receiver not configured (no ShuffleReceiver installed)".into(),
            }),
        }
    }

    pub(super) async fn on_shuffle_end_impl(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<TypedClusterError>,
    ) {
        if let Some(recv) = &self.shuffle_receiver {
            recv.on_shuffle_end(shuffle_id, part, side, error).await;
        }
    }

    // Cross-node shuffle PRODUCER trigger (E4a) — delegate to the host-crate
    // `ShuffleProducer`. When no producer is installed (cluster-only tests /
    // single-node), return a typed "not configured" error so the coordinator
    // learns the trigger could not run rather than silently succeeding.
    pub(super) async fn on_shuffle_produce_impl(
        &self,
        req: ShuffleProduceRequest,
    ) -> ShuffleProduceResponse {
        match &self.shuffle_producer {
            Some(producer) => producer.on_shuffle_produce(req).await,
            None => ShuffleProduceResponse {
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "shuffle producer not configured (no ShuffleProducer installed)"
                        .into(),
                }),
                read_versions: Vec::new(),
            },
        }
    }

    // Cross-node shuffle CONSUMER trigger (E4b) — delegate to the host-crate
    // `ShuffleConsumer`. When no consumer is installed (cluster-only tests /
    // single-node), return a typed "not configured" error (empty rows) so the
    // coordinator learns the trigger could not run rather than silently
    // receiving zero join rows.
    pub(super) async fn on_shuffle_consume_impl(
        &self,
        req: ShuffleConsumeRequest,
    ) -> ShuffleConsumeResponse {
        match &self.shuffle_consumer {
            Some(consumer) => consumer.on_shuffle_consume(req).await,
            None => ShuffleConsumeResponse {
                rows: Vec::new(),
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "shuffle consumer not configured (no ShuffleConsumer installed)"
                        .into(),
                }),
            },
        }
    }

    // Cross-node distributed GROUP BY shuffle CONSUMER trigger (E5b) — delegate
    // to the host-crate `ShuffleAggregator`. The single-sided aggregate sibling
    // of `on_shuffle_consume`. When no aggregator is installed (cluster-only
    // tests / single-node), return a typed "not configured" error (empty rows)
    // so the coordinator learns the trigger could not run rather than silently
    // receiving zero aggregate rows.
    pub(super) async fn on_shuffle_aggregate_impl(
        &self,
        req: ShuffleAggregateConsumeRequest,
    ) -> ShuffleAggregateConsumeResponse {
        match &self.shuffle_aggregator {
            Some(aggregator) => aggregator.on_shuffle_aggregate(req).await,
            None => ShuffleAggregateConsumeResponse {
                rows: Vec::new(),
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "shuffle aggregator not configured (no ShuffleAggregator installed)"
                        .into(),
                }),
            },
        }
    }

    // Routed-surrogate-exchange (F1b) — delegate to the host-crate
    // `AssignRemoteSurrogate`. This node is the home vShard's leader; a LOCAL
    // assign through it yields the authoritative surrogate. When no assigner is
    // installed (cluster-only tests / single-node), return a typed "not
    // configured" error (surrogate 0) so the coordinator learns the request could
    // not run rather than silently receiving a bogus zero surrogate.
    pub(super) async fn on_assign_surrogate_impl(
        &self,
        req: AssignSurrogateRequest,
    ) -> AssignSurrogateResponse {
        match &self.assign_remote_surrogate {
            Some(assigner) => assigner.on_assign_surrogate(req).await,
            None => AssignSurrogateResponse {
                surrogate: 0,
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "assign-remote-surrogate not configured \
                              (no AssignRemoteSurrogate installed)"
                        .into(),
                }),
                found: None,
            },
        }
    }

    // Routed Calvin-submit (Cv1) — delegate to the host-crate `CalvinSubmit`.
    // This node is the sequencer-group leader; submitting + awaiting through it
    // is correct because the leader's sequencer service assigns and the leader's
    // registry receives the replicated completion ack. When no Calvin-submit
    // hook is installed (cluster-only tests / single-node), return a typed "not
    // configured" error so the coordinator learns the request could not run
    // rather than silently believing the cross-shard write committed.
    pub(super) async fn on_submit_calvin_txn_impl(
        &self,
        req: SubmitCalvinTxnRequest,
    ) -> SubmitCalvinTxnResponse {
        match &self.calvin_submit {
            Some(submit) => submit.on_submit_calvin_txn(req).await,
            None => SubmitCalvinTxnResponse {
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "calvin-submit not configured (no CalvinSubmit installed)".into(),
                }),
                payload_bytes: None,
            },
        }
    }

    // Routed Calvin-INBOX submit (Cv1) — delegate to the host-crate
    // `CalvinSubmitInbox`. The OLLP dependent sibling of `on_submit_calvin_txn`.
    // This node is the sequencer-group leader; submitting + awaiting the
    // assignment through it is correct because the leader's sequencer service
    // assigns. When no Calvin-inbox hook is installed (cluster-only tests /
    // single-node), return a typed "not configured" error so the coordinator
    // learns the request could not run rather than silently believing the
    // dependent transaction was assigned.
    pub(super) async fn on_submit_calvin_inbox_impl(
        &self,
        req: SubmitCalvinInboxRequest,
    ) -> SubmitCalvinInboxResponse {
        match &self.calvin_submit_inbox {
            Some(submit) => submit.on_submit_calvin_inbox(req).await,
            None => SubmitCalvinInboxResponse {
                inbox_seq: 0,
                epoch: 0,
                position: 0,
                participants: 0,
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "calvin-inbox not configured (no CalvinSubmitInbox installed)".into(),
                }),
            },
        }
    }

    // Streamed parts of a multi-part Calvin transaction: offered to this
    // node's sequencer leader queue through the host-crate Calvin-inbox
    // hook. Without the hook, the stream learns it cannot run here.
    pub(super) async fn on_calvin_parts_impl(
        &self,
        req: crate::rpc_codec::CalvinPartsRequest,
    ) -> crate::rpc_codec::CalvinPartsResponse {
        match &self.calvin_submit_inbox {
            Some(submit) => submit.on_calvin_parts(req).await,
            None => crate::rpc_codec::CalvinPartsResponse {
                status: crate::calvin::PartsOfferStatus::Unknown as u8,
                next_index: 0,
                detail: Some("calvin-parts not configured (no CalvinSubmitInbox installed)".into()),
            },
        }
    }

    // Routed reserve-read (Calvin OLLP) — delegate to the host-crate
    // `ReserveRead`. This node is the sequencer-group leader; reserving
    // through it is correct because the leader's scheduler holds the
    // authoritative lock table for its local sequencer inbox. When no
    // reserve-read hook is installed (cluster-only tests / single-node),
    // return a typed "not configured" error so the coordinator learns the
    // request could not run rather than silently believing the read was
    // reserved.
    pub(super) async fn on_reserve_read_impl(
        &self,
        req: ReserveReadRequest,
    ) -> ReserveReadResponse {
        match &self.reserve_read {
            Some(hook) => hook.on_reserve_read(req).await,
            None => ReserveReadResponse {
                owner_bytes: None,
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "reserve-read not configured (no ReserveRead installed)".into(),
                }),
            },
        }
    }

    // Routed release-reservation (Calvin OLLP) — delegate to the host-crate
    // `ReleaseReservation`. The ack-only sibling of `on_reserve_read`. When no
    // release-reservation hook is installed (cluster-only tests /
    // single-node), return a typed "not configured" error so the coordinator
    // learns the request could not run rather than silently believing the
    // reservation was released.
    pub(super) async fn on_release_reservation_impl(
        &self,
        req: ReleaseReservationRequest,
    ) -> ReleaseReservationResponse {
        match &self.release_reservation {
            Some(hook) => hook.on_release_reservation(req).await,
            None => ReleaseReservationResponse {
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "release-reservation not configured (no ReleaseReservation installed)"
                        .into(),
                }),
            },
        }
    }
}
