//! `SyncSession` struct + lifecycle helpers.

use std::collections::HashMap;
use std::time::Instant;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::TenantId;

use super::super::dlq::DeviceMetadata;
use super::super::rate_limit::{RateLimitConfig, SyncRateLimiter};

/// State of a single sync session (one WebSocket connection).
pub struct SyncSession {
    /// Unique session ID.
    pub session_id: String,
    /// Authenticated tenant.
    pub tenant_id: Option<TenantId>,
    /// Authenticated username.
    pub username: Option<String>,
    /// Full authenticated identity (set after handshake).
    pub identity: Option<AuthenticatedIdentity>,
    /// Whether the handshake completed successfully.
    pub authenticated: bool,
    /// Client's vector clock per collection.
    pub client_clock: HashMap<String, HashMap<String, u64>>,
    /// Server's vector clock per collection (latest LSN).
    pub server_clock: HashMap<String, u64>,
    /// Subscribed shape IDs.
    pub subscribed_shapes: Vec<String>,
    /// Mutations processed in this session.
    pub mutations_processed: u64,
    /// Mutations rejected in this session.
    pub mutations_rejected: u64,
    /// Mutations silently dropped (security rejections).
    pub mutations_silent_dropped: u64,
    /// Last activity timestamp.
    pub last_activity: Instant,
    /// Session creation time.
    pub created_at: Instant,
    /// Per-session rate limiter.
    pub rate_limiter: SyncRateLimiter,
    /// Device metadata from handshake (for DLQ entries).
    pub device_metadata: DeviceMetadata,
    /// Per-peer replay deduplication: highest mutation_id successfully processed.
    /// Key = peer_id, Value = highest mutation_id seen from that peer.
    /// Deltas with mutation_id <= this value are idempotently skipped.
    pub last_seen_mutation: HashMap<u64, u64>,
    /// Set of `(tenant_id, collection_name)` pairs the client has
    /// ever sent a delta or shape subscription for — used by the
    /// Origin `CollectionPurged` broadcast to decide which sessions
    /// need to be notified when a collection is hard-deleted.
    pub tracked_collections: std::collections::HashSet<(u32, String)>,
    /// Last WAL LSN the client advertised in its vector clock at
    /// handshake. Used by offline-client replay to identify
    /// `CollectionPurged` events that committed while the client
    /// was disconnected.
    pub last_seen_lsn: u64,
}

impl SyncSession {
    pub fn new(session_id: String) -> Self {
        Self::with_rate_limit(session_id, &RateLimitConfig::default())
    }

    pub fn with_rate_limit(session_id: String, rate_config: &RateLimitConfig) -> Self {
        let now = Instant::now();
        Self {
            session_id,
            tenant_id: None,
            username: None,
            identity: None,
            authenticated: false,
            client_clock: HashMap::new(),
            server_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            mutations_processed: 0,
            mutations_rejected: 0,
            mutations_silent_dropped: 0,
            last_activity: now,
            created_at: now,
            rate_limiter: SyncRateLimiter::new(rate_config),
            device_metadata: DeviceMetadata::default(),
            last_seen_mutation: HashMap::new(),
            tracked_collections: std::collections::HashSet::new(),
            last_seen_lsn: 0,
        }
    }

    /// Session uptime in seconds.
    pub fn uptime_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Seconds since last activity.
    pub fn idle_secs(&self) -> u64 {
        self.last_activity.elapsed().as_secs()
    }

    /// Record that the client has interacted with this
    /// `(tenant, collection)` pair. Called from the delta-push and
    /// shape-subscribe paths. Membership here is the subscription
    /// state the Origin `CollectionPurged` broadcast filters on.
    pub fn track_collection(&mut self, tenant_id: u32, collection: &str) {
        self.tracked_collections
            .insert((tenant_id, collection.to_string()));
    }
}
