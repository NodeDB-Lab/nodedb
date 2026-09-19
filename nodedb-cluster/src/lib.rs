// SPDX-License-Identifier: BUSL-1.1

//! Distributed coordination for NodeDB: vShards, Multi-Raft, QUIC transport,
//! routing, replication, rebalancing, and the Calvin sequencer for cross-shard
//! atomicity.
//!
//! This crate is consumed by the `nodedb` server binary and by
//! NodeDB-internal tooling. Items marked `#[doc(hidden)]` are operational
//! plumbing whose API shape is not yet stable for v0.1.0 — they remain `pub`
//! so internal tools (`nodedb-cli`, `nodedb-studio`) and server crates can
//! reach them, but external consumers should not depend on those modules
//! directly. They will be promoted, redesigned, or moved behind curated
//! facades in a later release.

pub mod applied_watcher;
pub mod array_routing;
pub mod auth;
pub mod bootstrap;
#[doc(hidden)]
pub mod bootstrap_listener;
#[doc(hidden)]
pub mod calvin;
pub mod catalog;
#[doc(hidden)]
pub mod circuit_breaker;
pub mod cluster_epoch;
pub mod cluster_info;
pub mod conf_change;
pub mod cross_shard_txn;
pub mod decommission;
pub mod diag;
pub mod distributed_array;
pub mod distributed_document;
pub mod distributed_graph;
pub mod distributed_join;
pub mod distributed_spatial;
pub mod distributed_timeseries;
pub mod distributed_vector;
pub mod error;
pub mod forward;
#[doc(hidden)]
pub mod ghost;
#[doc(hidden)]
pub mod ghost_sweeper;
pub mod health;
pub mod install_snapshot;
pub mod lease_liveness;
pub mod lifecycle;
pub mod lifecycle_state;
pub mod loop_metrics;
pub mod metadata_group;
pub mod migration;
#[doc(hidden)]
pub mod migration_executor;
pub mod mirror;
pub mod multi_raft;
pub mod quic_transport;
pub mod raft_loop;
pub mod raft_storage;
pub mod rdma_transport;
pub mod reachability;
pub mod read_index_wait;
pub mod readiness;
pub mod rebalance;
pub mod rebalance_scheduler;
pub mod rebalancer;
pub mod routing;
pub mod routing_liveness;
pub mod rpc_codec;
pub mod shard_split;
pub mod subsystem;
pub mod swim;
pub mod sync_frame_versioned;
pub mod topology;
pub mod transport;
pub mod vshard_handler;
pub mod wire;
pub mod wire_version;

pub use applied_watcher::{AppliedIndexWatcher, GroupAppliedWatchers, WaitOutcome};
pub use bootstrap::{
    ClusterConfig, ClusterState, JoinRetryPolicy, start_cluster, start_cluster_subsystems,
};
#[doc(hidden)]
pub use calvin::{EngineKeySet, EpochBatch, ReadWriteSet, SequencedTxn, SortedVec, TxClass};
pub use catalog::ClusterCatalog;
#[doc(hidden)]
pub use circuit_breaker::BreakerSnapshot;
pub use cluster_epoch::{ClusterEpochState, load_persisted_epoch, persist_applied_epoch};
pub use cluster_info::{
    ClusterInfoSnapshot, ClusterObserver, GroupSnapshot, GroupStatusProvider, PeerSnapshot,
};
pub use conf_change::{ConfChange, ConfChangeType};
pub use decommission::{
    DecommissionCoordinator, DecommissionObserver, DecommissionPlan, DecommissionRunResult,
    DecommissionSafetyError, MetadataProposer, check_can_decommission, plan_full_decommission,
};
pub use error::{
    CalvinError, ClusterError, MigrationCheckpointError, MigrationRecoveryError, Result,
};
pub use forward::{ChunkSink, NoopPlanExecutor, PlanExecutor};
pub use ghost::{GhostStub, GhostTable};
pub use health::{HealthConfig, HealthMonitor};
pub use lease_liveness::{LeaseHolderLiveness, LeaseHolderLivenessHook};
pub use lifecycle_state::{ClusterLifecycleState, ClusterLifecycleTracker};
pub use loop_metrics::{LoopMetrics, LoopMetricsRegistry};
pub use migration::{MigrationPhase, MigrationState};
pub use migration_executor::{
    MigrationExecutor, MigrationRequest, MigrationResult, MigrationSnapshot, MigrationTracker,
};
pub use multi_raft::{GroupStatus, MultiRaft};
pub use raft_loop::{
    AssignRemoteSurrogate, CalvinSubmit, CalvinSubmitInbox, CommitApplier, RaftLoop,
    ReleaseReservation, ReserveRead, ShuffleAggregator, ShuffleConsumer, ShuffleProducer,
    ShuffleReceiver, SnapshotApplier, SnapshotBuilder, SnapshotQuarantineHook,
    VShardEnvelopeHandler,
};
pub use reachability::{
    NoopProber, ReachabilityDriver, ReachabilityDriverConfig, ReachabilityProber, TransportProber,
};
pub use read_index_wait::confirm_read_index;
pub use rebalance::{RebalancePlan, compute_plan, plan_to_requests};
pub use rebalancer::{
    AlwaysReadyGate, ElectionGate, LoadMetrics, LoadMetricsProvider, LoadWeights,
    MigrationDispatcher, RebalancerKickHook, RebalancerLoop, RebalancerLoopConfig,
    RebalancerPlanConfig, compute_load_based_plan, normalized_score,
};
pub use routing::RoutingTable;
pub use routing_liveness::{NodeIdResolver, RoutingLivenessHook};
pub use rpc_codec::{
    AssignSurrogateRequest, AssignSurrogateResponse, DataPlaneErrorCode, JoinKeyPair, MacKey,
    PartNodeEntry, RaftRpc, ReleaseReservationRequest, ReleaseReservationResponse,
    ReserveReadRequest, ReserveReadResponse, ShuffleAggregateConsumeRequest,
    ShuffleAggregateConsumeResponse, ShuffleConsumeRequest, ShuffleConsumeResponse,
    ShuffleProduceRequest, ShuffleProduceResponse, ShufflePushChunk, ShufflePushEnd,
    ShufflePushRequest, SortKey, SubmitCalvinInboxRequest, SubmitCalvinInboxResponse,
    SubmitCalvinTxnRequest, SubmitCalvinTxnResponse, TypedClusterError,
};
pub use topology::{ClusterTopology, NodeInfo, NodeState};
pub use transport::{
    IDENTITY_MISMATCH_QUIC_ERROR, NexarTransport, NoopIdentityStore, PeerIdentityStore,
    PinnedClientVerifier, PinnedServerVerifier, RaftRpcHandler, ShufflePushStream, TlsCredentials,
    TransportCredentials, TransportPeerSnapshot, VerifyMethod, VerifyOutcome, ca_fingerprint,
    ca_fingerprint_hex, enrollment_identity_from_cert_der, generate_node_credentials,
    generate_node_credentials_multi_san, insecure_transport_bind_allowed, insecure_transport_count,
    issue_leaf_for_sans, load_crls_from_pem, make_raft_client_config_mtls,
    make_raft_server_config_mtls, spki_pin_from_cert_der,
};
pub use wire::VShardEnvelope;

pub use cross_shard_txn::{
    EdgeValidationRequest, EdgeValidationResult, GsiAction, GsiForwardEntry,
};
pub use metadata_group::entry::JoinTokenTransitionKind;
pub use metadata_group::{
    CacheApplier, Compensation, DescriptorHeader, DescriptorId, DescriptorKind, DescriptorLease,
    DescriptorState, METADATA_GROUP_ID, MetadataApplier, MetadataCache, MetadataEntry,
    MigrationCheckpointPayload, MigrationId, MigrationPhaseTag, NoopMetadataApplier,
    PendingDdlObject, PersistedMigrationCheckpoint, RoutingChange, SharedMigrationStateTable,
    TopologyChange, apply_migration_abort, apply_migration_checkpoint, decode_entry, encode_entry,
    new_shared,
};
pub use migration_executor::recover_in_flight_migrations;
pub use quic_transport::{QuicTransport, QuicTransportConfig};

pub use array_routing::{tile_id_of_coord, vshard_for_array_coord, vshard_for_array_tile};
pub use distributed_join::{BroadcastJoinRequest, JoinStrategy, ShufflePartition, select_strategy};
pub use lifecycle::{
    DecommissionResult, handle_learner_promotion, handle_node_join, plan_decommission,
};
pub use rdma_transport::{RdmaConfig, RdmaTransport};
pub use rebalance_scheduler::{NodeMetrics, RebalanceScheduler, RebalanceTrigger, SchedulerConfig};
pub use shard_split::{SplitPlan, SplitStrategy, plan_graph_split, plan_vector_split};
pub use subsystem::{
    BootstrapCtx, BootstrapError, ClusterHealth, ClusterSubsystem, RunningCluster, ShutdownError,
    SubsystemHandle, SubsystemHealth, SubsystemRegistry, TopoError, topo_sort,
};
pub use swim::bootstrap::spawn_with_subscribers as spawn_swim_with_subscribers;
pub use swim::{
    Incarnation, Member, MemberState, MembershipList, MembershipSubscriber, SwimConfig, SwimError,
    SwimHandle, UdpTransport, spawn as spawn_swim,
};

pub use auth::{
    AuditEvent, AuditWriter, AuthenticatedJoinBundle, BundleError, InMemoryTokenStore, JoinOutcome,
    JoinTokenLifecycle, JoinTokenState, NoopAuditWriter, RaftBackedTokenStore,
    SharedTokenStateMirror, TokenError, TokenStateBackend, TokenStateError, VecAuditWriter,
    apply_token_transition_to_mirror, derive_mac_key, issue_token, open_bundle, seal_bundle,
    spawn_inflight_timeout, token_hash, verify_token,
};

pub use wire_version::handshake_io::{
    perform_version_handshake_client, perform_version_handshake_server,
};
pub use wire_version::{
    VersionHandshake, VersionHandshakeAck, VersionRange, Versioned, WireVersion, WireVersionError,
    WireVersionMetrics, decode_versioned, encode_versioned, local_version_range, negotiate,
    unwrap_bytes_versioned, wrap_bytes_versioned,
};
