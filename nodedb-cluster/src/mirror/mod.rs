// SPDX-License-Identifier: BUSL-1.1

//! Cross-cluster mirror transport: an mTLS QUIC listener on the source, a
//! seeded client on the mirror, and snapshot bootstrap.
//!
//! # Module layout
//!
//! - [`error`] — [`MirrorError`] enum; wires into [`crate::error::ClusterError`].
//! - [`allowlist`] — [`MirrorAllowlist`]: who can open a link, and the TLS
//!   configs that enforce it.
//! - [`handshake`] — [`MirrorHello`] / [`MirrorHelloAck`].
//! - [`tail_wire`] — the bounded frames, requests and refusals of a link.
//! - [`listener`] — [`serve_mirror_source`]: the source's accept loop.
//! - [`link`] — [`CrossClusterLink`]: one mirror's connection, seed rotation and
//!   reconnect backoff.
//! - [`client`] — [`MirrorSourceClient`]: hello, fetch and ack across replicas.
//! - [`throttle`] — [`SendThrottle`]: a bootstrap transfer's bytes-in-flight cap.
//! - [`bootstrap`] — [`MirrorBootstrapReceiver`]: cross-cluster snapshot transfer,
//!   `Bootstrapping → Following` status transitions.

pub mod allowlist;
pub mod bootstrap;
pub mod client;
pub mod error;
pub mod handshake;
pub mod link;
pub mod listener;
pub mod tail_wire;
pub mod throttle;

pub use allowlist::{
    ALPN_NODEDB_MIRROR, MirrorAllowlist, MirrorClient, make_mirror_client_config,
    make_mirror_server_config,
};

pub use bootstrap::{
    BootstrapChunkOutcome, CrossClusterSnapshotEnvelope, MirrorBootstrapReceiver,
    PROGRESS_REPORT_CHUNK_BYTES, ProgressCallback,
};
pub use client::MirrorSourceClient;
pub use error::MirrorError;
pub use handshake::{MIRROR_PROTOCOL_VERSION, MirrorHello, MirrorHelloAck};
pub use link::{CrossClusterLink, MirrorLinkConfig, derive_link_id};
pub use listener::{MirrorSourceService, bind_mirror_endpoint, serve_mirror_source};
pub use tail_wire::{
    MAX_BOOTSTRAP_BYTES, MAX_BOOTSTRAP_CHUNK, MAX_TAIL_BYTES, MAX_TAIL_WAIT_MS, MirrorAck,
    MirrorBootstrapRequest, MirrorRefusal, MirrorRequest, MirrorResponse, MirrorTailBatch,
    MirrorTailEntry, MirrorTailRequest,
};
pub use throttle::{DEFAULT_CAP_BYTES, SendThrottle};
