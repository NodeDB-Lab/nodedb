//! SWIM — Scalable Weakly-consistent Infection-style Membership.
//!
//! This module implements the foundation of NodeDB's cluster membership and
//! failure-detection subsystem, modelled after Das, Gupta & Motivala's SWIM
//! paper (DSN 2002) with the Lifeguard refinements (suspicion multiplier,
//! incarnation refutation, dedicated acks) used by modern systems such as
//! Hashicorp memberlist and Cassandra's gossiper.
//!
//! ## Layer map (Phase E)
//!
//! | Sub-batch | Contents                                                   |
//! |-----------|------------------------------------------------------------|
//! | **E-α**   | Core types — `config`, `error`, `incarnation`, `member`, `membership` (this file's children) |
//! | E-β       | Wire messages (`Ping`/`PingReq`/`Ack`/`Nack`) + zerompk codec |
//! | E-γ       | Failure detector loop over an injected transport trait     |
//! | E-δ       | Piggyback dissemination queue + convergence tests          |
//! | E-ε       | Real UDP transport, bootstrap seeding, cluster integration |
//!
//! E-α is deliberately side-effect-free: no tasks, no I/O, no wire formats.
//! It exposes the pure data model — member states, incarnation numbers, and
//! the state-merge rule — that every later sub-batch builds on.

pub mod bootstrap;
pub mod config;
pub mod detector;
pub mod dissemination;
pub mod error;
pub mod incarnation;
pub mod member;
pub mod membership;
pub mod wire;

pub use bootstrap::{SwimHandle, spawn};
pub use config::SwimConfig;
pub use detector::{
    FailureDetector, InMemoryTransport, ProbeScheduler, Transport, TransportFabric, UdpTransport,
};
pub use dissemination::{DisseminationQueue, PendingUpdate, apply_and_disseminate};
pub use error::SwimError;
pub use incarnation::Incarnation;
pub use member::{Member, MemberState};
pub use membership::{MembershipList, MembershipSnapshot, merge_update};
pub use wire::{Ack, Nack, NackReason, Ping, PingReq, ProbeId, SwimMessage};
