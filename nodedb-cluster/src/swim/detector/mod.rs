//! SWIM failure detector — the runtime that drives the probe loop.
//!
//! This module is the `!I/O` portion of the SWIM subsystem: it owns the
//! probe scheduler, the suspicion timer, and the main `tokio::select!`
//! loop. All actual networking is pushed behind the [`Transport`] trait
//! so unit tests can run fully in-process against [`InMemoryTransport`]
//! and the real UDP transport in E-ε can slot in without touching the
//! detector logic.

pub mod probe_round;
pub mod runner;
pub mod scheduler;
pub mod suspicion;
pub mod transport;

pub use probe_round::{ProbeOutcome, ProbeRound};
pub use runner::FailureDetector;
pub use scheduler::ProbeScheduler;
pub use suspicion::SuspicionTimer;
pub use transport::{InMemoryTransport, Transport, TransportFabric, UdpTransport};
