// SPDX-License-Identifier: Apache-2.0

//! Network, bridge, WAL, and cluster transport tuning.

use serde::{Deserialize, Serialize};

/// SPSC bridge and slab allocator tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeTuning {
    #[serde(default = "default_slab_page_size")]
    pub slab_page_size: usize,
    #[serde(default = "default_slab_pool_size")]
    pub slab_pool_size: usize,
}

impl Default for BridgeTuning {
    fn default() -> Self {
        Self {
            slab_page_size: default_slab_page_size(),
            slab_pool_size: default_slab_pool_size(),
        }
    }
}

fn default_slab_page_size() -> usize {
    64 * 1024
}
fn default_slab_pool_size() -> usize {
    256
}

/// Network, deadline, and session tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkTuning {
    #[serde(default = "default_deadline_secs")]
    pub default_deadline_secs: u64,
    #[serde(default = "default_copy_deadline_secs")]
    pub copy_deadline_secs: u64,
    #[serde(default = "default_max_ws_sessions")]
    pub max_ws_sessions: usize,
    #[serde(default = "default_subscription_buffer_size")]
    pub subscription_buffer_size: usize,
    #[serde(default = "default_subscription_idle_timeout_secs")]
    pub subscription_idle_timeout_secs: u64,
    #[serde(default = "default_token_refresh_window_secs")]
    pub token_refresh_window_secs: u64,
    #[serde(default = "default_drain_timeout_secs")]
    pub drain_timeout_secs: u64,
    #[serde(default = "default_max_frame_size")]
    pub max_frame_size: u32,
    /// Hard cap on the total payload bytes a single dispatched query may
    /// accumulate across streamed partials in the Control Plane. Any
    /// query whose combined response exceeds this is cancelled and the
    /// session sees a typed `ExecutionLimitExceeded` — prevents a
    /// runaway `SELECT *` from a scheduled job (or any caller) from
    /// filling Control-Plane RAM.
    #[serde(default = "default_max_query_result_bytes")]
    pub max_query_result_bytes: u64,
    /// The oldest snapshot, in seconds, a transaction block reads at. A
    /// statement of a block whose snapshot is older fails with SQLSTATE
    /// `40001`, and the client retries the block. The cleanup horizon also
    /// stops waiting for a node it cannot reach once its snapshots are this
    /// old. Set via `[tuning.network] txn_snapshot_max_age_secs`.
    #[serde(default = "default_txn_snapshot_max_age_secs")]
    pub txn_snapshot_max_age_secs: u64,
}

impl NetworkTuning {
    /// The longest any statement reads at one snapshot: the snapshot age
    /// limit, plus the longest statement deadline, plus the clock skew
    /// bound. A snapshot older than this serves no statement on any node.
    ///
    /// Every node of a cluster must run one value. The cleanup horizon and
    /// the commit check of every replica rely on it: a commit whose snapshot
    /// is older than this below its own ordinal is refused on every replica.
    pub fn snapshot_max_hold(&self) -> std::time::Duration {
        let longest_deadline = self.default_deadline_secs.max(self.copy_deadline_secs);
        std::time::Duration::from_secs(self.txn_snapshot_max_age_secs)
            .saturating_add(std::time::Duration::from_secs(longest_deadline))
            .saturating_add(std::time::Duration::from_nanos(crate::MAX_CLOCK_SKEW_NS))
    }
}

impl Default for NetworkTuning {
    fn default() -> Self {
        Self {
            default_deadline_secs: default_deadline_secs(),
            copy_deadline_secs: default_copy_deadline_secs(),
            max_ws_sessions: default_max_ws_sessions(),
            subscription_buffer_size: default_subscription_buffer_size(),
            subscription_idle_timeout_secs: default_subscription_idle_timeout_secs(),
            token_refresh_window_secs: default_token_refresh_window_secs(),
            drain_timeout_secs: default_drain_timeout_secs(),
            max_frame_size: default_max_frame_size(),
            max_query_result_bytes: default_max_query_result_bytes(),
            txn_snapshot_max_age_secs: default_txn_snapshot_max_age_secs(),
        }
    }
}

fn default_deadline_secs() -> u64 {
    30
}
fn default_copy_deadline_secs() -> u64 {
    120
}
fn default_max_ws_sessions() -> usize {
    10_000
}
fn default_subscription_buffer_size() -> usize {
    1024
}
fn default_subscription_idle_timeout_secs() -> u64 {
    300
}
fn default_token_refresh_window_secs() -> u64 {
    300
}
fn default_drain_timeout_secs() -> u64 {
    30
}
fn default_max_frame_size() -> u32 {
    16 * 1024 * 1024
}
fn default_max_query_result_bytes() -> u64 {
    // 1 GiB — enough headroom for legitimate analytic scans while
    // preventing an unbounded result-set from pinning Control-Plane
    // RAM. Configurable via `[tuning.network] max_query_result_bytes`.
    1024 * 1024 * 1024
}

fn default_txn_snapshot_max_age_secs() -> u64 {
    300
}

/// WAL writer and double-write buffer tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalTuning {
    #[serde(default = "default_wal_write_buffer_size")]
    pub write_buffer_size: usize,
    #[serde(default = "default_wal_alignment")]
    pub alignment: usize,
    #[serde(default = "default_dwb_capacity")]
    pub dwb_capacity: usize,
    /// Open WAL segments with `O_DIRECT`, keeping WAL traffic out of the page
    /// cache so it cannot evict mmapped indexes and so durability does not
    /// depend on kernel writeback.
    ///
    /// Turning this off is a deliberate weakening of the durability path and
    /// is only correct where the filesystem cannot do direct I/O at all
    /// (tmpfs, most overlayfs setups, most network filesystems). Startup fails
    /// with an actionable error rather than downgrading on its own.
    #[serde(default = "default_wal_direct_io")]
    pub direct_io: bool,
}

impl Default for WalTuning {
    fn default() -> Self {
        Self {
            write_buffer_size: default_wal_write_buffer_size(),
            alignment: default_wal_alignment(),
            dwb_capacity: default_dwb_capacity(),
            direct_io: default_wal_direct_io(),
        }
    }
}

fn default_wal_write_buffer_size() -> usize {
    2 * 1024 * 1024
}
fn default_wal_alignment() -> usize {
    4096
}
fn default_dwb_capacity() -> usize {
    64
}
fn default_wal_direct_io() -> bool {
    true
}

/// Cluster transport tuning for QUIC connections and Raft consensus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterTransportTuning {
    #[serde(default = "default_raft_tick_interval_ms")]
    pub raft_tick_interval_ms: u64,
    #[serde(default = "default_election_timeout_min_secs")]
    pub election_timeout_min_secs: u64,
    #[serde(default = "default_election_timeout_max_secs")]
    pub election_timeout_max_secs: u64,
    /// Sub-second override for `election_timeout_min_secs`. When non-zero,
    /// this value (in milliseconds) is used instead of the seconds field.
    /// Production deployments leave this 0 and use the seconds field;
    /// integration tests set this to ~150ms so 3-node convergence completes
    /// in under a second instead of 1–2 seconds per cluster spawn.
    #[serde(default)]
    pub election_timeout_min_ms: u64,
    /// Sub-second override for `election_timeout_max_secs`. See
    /// [`Self::election_timeout_min_ms`].
    #[serde(default)]
    pub election_timeout_max_ms: u64,
    /// Timeout of one RPC round trip, and the QUIC idle timeout: a peer
    /// silent this long is dead. Keep-alives go out at a fifth of it.
    /// Startup refuses a value below 500 ms.
    #[serde(default = "default_rpc_timeout_secs")]
    pub rpc_timeout_secs: u64,
    #[serde(default = "default_quic_max_bi_streams")]
    pub quic_max_bi_streams: u32,
    #[serde(default = "default_quic_max_uni_streams")]
    pub quic_max_uni_streams: u32,
    #[serde(default = "default_quic_receive_window")]
    pub quic_receive_window: u32,
    #[serde(default = "default_quic_send_window")]
    pub quic_send_window: u32,
    #[serde(default = "default_quic_stream_receive_window")]
    pub quic_stream_receive_window: u32,
    /// Maximum payload size for broadcast join strategy selection.
    /// Above this threshold, shuffle join is preferred over broadcast.
    /// See `nodedb_cluster::distributed_join::select_strategy`.
    #[serde(default = "default_broadcast_threshold_bytes")]
    pub broadcast_threshold_bytes: usize,
    /// How often (in seconds) to sweep for dangling ghost nodes.
    /// See `nodedb_cluster::ghost_sweeper::DEFAULT_SWEEP_INTERVAL`.
    #[serde(default = "default_ghost_sweep_interval_secs")]
    pub ghost_sweep_interval_secs: u64,
    /// Cluster health check ping interval in seconds.
    /// See `nodedb_cluster::health::DEFAULT_PING_INTERVAL`.
    #[serde(default = "default_health_ping_interval_secs")]
    pub health_ping_interval_secs: u64,
    /// Consecutive ping failures before marking a node as down.
    /// See `nodedb_cluster::health::DEFAULT_FAILURE_THRESHOLD`.
    #[serde(default = "default_health_failure_threshold")]
    pub health_failure_threshold: u32,
    /// Default duration of a descriptor lease, in seconds.
    /// Used by `SharedState::acquire_descriptor_lease` when the
    /// caller does not specify an explicit duration. The renewal
    /// loop re-acquires leases at 80% of this value.
    #[serde(default = "default_descriptor_lease_duration_secs")]
    pub descriptor_lease_duration_secs: u64,
    /// How often the descriptor-lease renewal loop wakes up to
    /// look for near-expiry leases. Default: 60s. Tests override
    /// this with a much smaller value (e.g. 1s) to drive renewal
    /// within a multi-second test budget.
    #[serde(default = "default_descriptor_lease_renewal_check_interval_secs")]
    pub descriptor_lease_renewal_check_interval_secs: u64,
    /// Re-acquire leases when the remaining time (until expiry)
    /// falls below this percentage of the original duration.
    /// Default: 20 (re-acquire when 80% of the duration has
    /// elapsed). Tests can set this to 50 to make renewal trigger
    /// earlier in the lease lifecycle.
    #[serde(default = "default_descriptor_lease_renewal_threshold_pct")]
    pub descriptor_lease_renewal_threshold_pct: u8,
    /// Longest interval, in milliseconds, a data-group leader lets a node
    /// that does not replicate the group go without the group's change-feed
    /// position. A settled run with events carries the position at once; an
    /// idle feed carries it on a heartbeat at this interval.
    #[serde(default = "default_change_feed_heartbeat_ms")]
    pub change_feed_heartbeat_ms: u64,
}

impl ClusterTransportTuning {
    /// Effective minimum election timeout. Returns `_ms` when non-zero,
    /// otherwise `_secs * 1000`. Tests use `_ms` for sub-second values;
    /// production leaves it 0 and the seconds field wins.
    pub fn effective_election_timeout_min_ms(&self) -> u64 {
        if self.election_timeout_min_ms > 0 {
            self.election_timeout_min_ms
        } else {
            self.election_timeout_min_secs.saturating_mul(1000)
        }
    }

    /// Effective maximum election timeout. See
    /// [`Self::effective_election_timeout_min_ms`].
    pub fn effective_election_timeout_max_ms(&self) -> u64 {
        if self.election_timeout_max_ms > 0 {
            self.election_timeout_max_ms
        } else {
            self.election_timeout_max_secs.saturating_mul(1000)
        }
    }
}

impl Default for ClusterTransportTuning {
    fn default() -> Self {
        Self {
            raft_tick_interval_ms: default_raft_tick_interval_ms(),
            election_timeout_min_secs: default_election_timeout_min_secs(),
            election_timeout_max_secs: default_election_timeout_max_secs(),
            election_timeout_min_ms: 0,
            election_timeout_max_ms: 0,
            rpc_timeout_secs: default_rpc_timeout_secs(),
            quic_max_bi_streams: default_quic_max_bi_streams(),
            quic_max_uni_streams: default_quic_max_uni_streams(),
            quic_receive_window: default_quic_receive_window(),
            quic_send_window: default_quic_send_window(),
            quic_stream_receive_window: default_quic_stream_receive_window(),
            broadcast_threshold_bytes: default_broadcast_threshold_bytes(),
            ghost_sweep_interval_secs: default_ghost_sweep_interval_secs(),
            health_ping_interval_secs: default_health_ping_interval_secs(),
            health_failure_threshold: default_health_failure_threshold(),
            descriptor_lease_duration_secs: default_descriptor_lease_duration_secs(),
            descriptor_lease_renewal_check_interval_secs:
                default_descriptor_lease_renewal_check_interval_secs(),
            descriptor_lease_renewal_threshold_pct: default_descriptor_lease_renewal_threshold_pct(
            ),
            change_feed_heartbeat_ms: default_change_feed_heartbeat_ms(),
        }
    }
}

fn default_change_feed_heartbeat_ms() -> u64 {
    1_000
}

fn default_descriptor_lease_duration_secs() -> u64 {
    300
}
fn default_descriptor_lease_renewal_check_interval_secs() -> u64 {
    60
}
fn default_descriptor_lease_renewal_threshold_pct() -> u8 {
    20
}

fn default_raft_tick_interval_ms() -> u64 {
    10
}
fn default_election_timeout_min_secs() -> u64 {
    2
}
fn default_election_timeout_max_secs() -> u64 {
    5
}
fn default_rpc_timeout_secs() -> u64 {
    5
}
fn default_quic_max_bi_streams() -> u32 {
    256
}
fn default_quic_max_uni_streams() -> u32 {
    256
}
fn default_quic_receive_window() -> u32 {
    16 * 1024 * 1024
}
fn default_quic_send_window() -> u32 {
    16 * 1024 * 1024
}
fn default_quic_stream_receive_window() -> u32 {
    4 * 1024 * 1024
}
fn default_broadcast_threshold_bytes() -> usize {
    8 * 1024 * 1024
}
fn default_ghost_sweep_interval_secs() -> u64 {
    1800
}
fn default_health_ping_interval_secs() -> u64 {
    5
}
fn default_health_failure_threshold() -> u32 {
    3
}
