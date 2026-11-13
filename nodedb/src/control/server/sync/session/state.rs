// SPDX-License-Identifier: BUSL-1.1

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
    /// The registry row and kill signal of the handshake's identity. A handshake
    /// clears it and binds its own identity ahead of the `HandshakeAck`.
    pub binding: Option<crate::control::security::sessions::SessionBinding>,
    /// Client's vector clock per collection.
    pub client_clock: HashMap<String, HashMap<String, u64>>,
    /// Server's vector clock per collection (latest LSN).
    pub server_clock: HashMap<String, u64>,
    /// Shape IDs the client named in its handshake. Client-supplied and not
    /// authorized: the session's delivery registration in
    /// `CrdtSyncDelivery` holds the authorized shapes and the collections a
    /// purge notice reaches.
    pub subscribed_shapes: Vec<String>,
    /// Mutations admitted by envelope validation in this session.
    ///
    /// Admission is provisional — it says the frame was well-formed and
    /// authorized, NOT that it was applied. Compare against
    /// [`Self::mutations_applied`] to see how many actually landed.
    pub mutations_processed: u64,
    /// Mutations the durable apply confirmed as applied.
    pub mutations_applied: u64,
    /// Mutations refused after admission — by the constraint validator, the
    /// Data Plane, or authorization. Counted wherever the refusal is decided,
    /// including refusals raised downstream of this session's provisional ack.
    pub mutations_rejected: u64,
    /// Mutations neither applied nor permanently refused: retryable refusals,
    /// sequence gaps, and fenced epochs. The sender is expected to re-push
    /// these, so they are neither successes nor dead letters.
    pub mutations_not_applied: u64,
    /// Mutations whose operations were already present, so the apply moved
    /// nothing. Counted apart from [`Self::mutations_applied`] because they are
    /// opposite facts about the database: one says this session's write landed,
    /// the other says it did not have to. Folding them together is what let a
    /// session that applied nothing close looking identical to one that applied
    /// everything.
    pub mutations_deduplicated: u64,
    /// Mutations silently dropped (security rejections).
    pub mutations_silent_dropped: u64,
    /// Operations this session's deltas carried that the target document
    /// already knew, and the CRDT merge therefore discarded.
    ///
    /// A resync re-sends a prefix it has already delivered and this rises while
    /// `mutations_applied` also rises. A session whose every delta trims — the
    /// peer-id collision shape — shows this rising with nothing applied, which
    /// before this counter existed was invisible at every log level.
    pub ops_trimmed: u64,
    /// Last activity timestamp.
    pub last_activity: Instant,
    /// Session creation time.
    pub created_at: Instant,
    /// Per-session rate limiter.
    pub rate_limiter: SyncRateLimiter,
    /// Rate limiter for snapshot reads on this session's behalf: a
    /// `ShapeSubscribe`, a client `ResyncRequest`, a server collection
    /// resync. Each costs a full collection scan, so they share one bucket
    /// apart from the mutation limiter.
    pub snapshot_limiter: SyncRateLimiter,
    /// Collections whose `shape:{collection}` presence channel this session
    /// joined through an authorized subscribe. Each push drain re-checks the
    /// read grant and leaves the channel when it is gone.
    pub presence_shapes: std::collections::HashSet<String>,
    /// Device metadata from handshake (for DLQ entries).
    pub device_metadata: DeviceMetadata,
    /// Collections whose descriptor has already been announced to the peer
    /// this session via a `CollectionSchema` frame. Enforces the
    /// announce-precedes-data guard: a collection's schema is emitted at most
    /// once per session, strictly before its first shape snapshot or delta.
    pub announced_collections: std::collections::HashSet<String>,
    /// The `last_server_watermark` of the client's handshake: the metadata
    /// log position through which it applied every purge it holds. The
    /// handshake replays each purge-log row past it.
    pub client_watermark: u64,
    /// The `CollectionPurged` or `DatabaseResync` frames the accepted
    /// handshake owes the client. The session sends them ahead of the
    /// `HandshakeAck`, so a client that applied the ack applied them first.
    pub purge_catch_up: Vec<super::super::wire::SyncFrame>,
    /// Durable producer id assigned by `SyncProducerRegistry` at handshake.
    /// `0` means the session is not a Lite client or the registry was
    /// unavailable.
    pub producer_id: u64,
    /// The fencing epoch accepted by `SyncProducerRegistry` at handshake.
    /// `0` for non-Lite sessions.
    pub accepted_epoch: u64,
    /// Catalog-backed per-user HMAC key issued after JWT authentication.
    /// Zero/absent in trust mode and test-only sessions without a catalog.
    pub delta_signing_key: Option<[u8; 32]>,
    /// The token expiry the client last got a `TokenRefreshRequired` hint for.
    pub refresh_hinted_expiry_ms: Option<u64>,
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
            binding: None,
            client_clock: HashMap::new(),
            server_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            mutations_processed: 0,
            mutations_applied: 0,
            mutations_not_applied: 0,
            mutations_deduplicated: 0,
            mutations_rejected: 0,
            mutations_silent_dropped: 0,
            ops_trimmed: 0,
            last_activity: now,
            created_at: now,
            rate_limiter: SyncRateLimiter::new(rate_config),
            snapshot_limiter: SyncRateLimiter::new(&super::super::async_dispatch::snapshot_rate()),
            presence_shapes: std::collections::HashSet::new(),
            device_metadata: DeviceMetadata::default(),
            announced_collections: std::collections::HashSet::new(),
            client_watermark: 0,
            purge_catch_up: Vec::new(),
            producer_id: 0,
            accepted_epoch: 0,
            delta_signing_key: None,
            refresh_hinted_expiry_ms: None,
        }
    }

    /// Database this session is bound to.
    ///
    /// Derived from the handshake-established identity's default database, so
    /// it cannot drift from the principal the session authenticated as. Every
    /// session-bound sync operation — catalog lookup, surrogate assignment,
    /// vShard routing, snapshot dispatch — routes through this rather than
    /// assuming the built-in default, which would let a user whose default
    /// database is not `DEFAULT` read and write across the database boundary.
    ///
    /// An unauthenticated session has no identity and therefore no database;
    /// callers must establish identity before they have anything to route.
    pub fn database_id(&self) -> crate::types::DatabaseId {
        self.identity
            .as_ref()
            .and_then(|identity| identity.default_database)
            .unwrap_or(crate::types::DatabaseId::DEFAULT)
    }

    /// Session uptime in seconds.
    pub fn uptime_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Seconds since last activity.
    pub fn idle_secs(&self) -> u64 {
        self.last_activity.elapsed().as_secs()
    }
}
