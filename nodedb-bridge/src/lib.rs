// SPDX-License-Identifier: BUSL-1.1

// Library code propagates errors; tests may unwrap.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! # nodedb-bridge
//!
//! Lock-free, capacity-bounded SPSC ring buffer bridging a **Tokio Control Plane**
//! (`Send + Sync`) and a **Thread-per-Core Data Plane** (`!Send`).
//!
//! This crate is the single most critical infrastructure component in NodeDB.
//! If this bridge fails under load, the entire database fails.
//!
//! ## Design constraints
//!
//! - **No locks, no atomics in the hot path** beyond the two cache-line-padded
//!   head/tail counters (one written by producer, one by consumer).
//! - **Bounded capacity**: the ring buffer has a fixed power-of-two slot count.
//!   When full, the producer must yield — this is the backpressure mechanism.
//! - **Cross-runtime waker integration**: when the queue transitions from
//!   full→not-full or empty→not-empty, the sleeping side must be woken
//!   *without* requiring `Send` on the waker itself.
//! - **Zero-copy where possible**: payloads are `Arc<[u8]>` or slab handles;
//!   the ring buffer moves lightweight envelopes, not bulk data.
//!
//! ## Validation target
//!
//! Pump 50 GB of dummy data Tokio→TPC→Tokio. Memory must stay flat.
//! Throughput must exceed 5 GB/s on a modern x86_64 machine.

pub mod async_bridge;
pub mod backpressure;
pub mod buffer;
pub mod envelope;
pub mod error;
pub mod eventfd;
pub mod metrics;
pub mod telemetry;
#[cfg(feature = "tokio")]
pub mod tokio_fd;
pub mod waker;
pub mod wfq;
pub mod wfq_metrics;

pub use async_bridge::{BridgeChannel, ControlHandle, DataHandle, PinnedDataHandle};
pub use backpressure::{BackpressureConfig, BackpressureController, PressureState};
pub use buffer::{Consumer, Producer, RingBuffer};
pub use envelope::{Request, Response};
pub use error::{BridgeError, Result};
pub use eventfd::{EventFd, WakePair};
pub use metrics::BridgeMetrics;
#[cfg(feature = "tokio")]
pub use tokio_fd::AsyncControlHandle;
pub use wfq::{WeightedFairQueue, priority_weight};
pub use wfq_metrics::VirtualQueueMetrics;
