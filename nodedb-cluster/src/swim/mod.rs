//! SWIM — Scalable Weakly-consistent Infection-style Membership.
//!
//! This module implements the foundation of NodeDB's cluster membership and
//! failure-detection subsystem, modelled after Das, Gupta & Motivala's SWIM
//! paper (DSN 2002) with the Lifeguard refinements (suspicion multiplier,
//! incarnation refutation, dedicated acks) used by modern systems such as
//! Hashicorp memberlist and Cassandra's gossiper.
//!
//! ## Layer map
//!
//! - `config`, `error`, `incarnation`, `member`, `membership` — pure
//!   data model: states, incarnation numbers, and the merge rule.
//! - `wire` — `Ping` / `PingReq` / `Ack` / `Nack` datagrams + codec.
//! - `detector` — failure detector loop over a pluggable transport
//!   trait, scheduler, suspicion timer, probe round machinery.
//! - `dissemination` — piggyback queue with `lambda * log(n)` fanout.
//! - `bootstrap` — one-stop `spawn` entry point.
//! - `subscriber` — hook trait fired on every membership transition.

pub mod bootstrap;
pub mod config;
pub mod detector;
pub mod dissemination;
pub mod error;
pub mod incarnation;
pub mod member;
pub mod membership;
pub mod subscriber;
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
pub use subscriber::MembershipSubscriber;
pub use wire::{Ack, Nack, NackReason, Ping, PingReq, ProbeId, SwimMessage};
