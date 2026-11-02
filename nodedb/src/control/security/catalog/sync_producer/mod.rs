// SPDX-License-Identifier: BUSL-1.1

//! Catalog tables that back sync-client identity: the producer-id allocator
//! watermark, per-`lite_id` producer registrations, the CRDT peer-id bindings
//! that keep two replicas from claiming one Loro identity, per-user delta
//! signing material, and the replicated join-token / enrollment mirrors.

pub mod enrollment;
pub mod hwm;
pub mod join_token;
pub mod peer_binding;
pub mod registration;
pub mod signing;

pub use enrollment::ENROLLMENT_PREAUTHORIZATIONS;
pub use hwm::SYNC_PRODUCER_HWM;
pub use join_token::JOIN_TOKEN_STATES;
pub use peer_binding::{PeerBindingKey, SYNC_PEER_BINDINGS, StoredPeerBinding};
pub use registration::{SYNC_PRODUCERS, StoredProducerRegistration};
pub use signing::CRDT_SIGNING_ROOT_METADATA;
