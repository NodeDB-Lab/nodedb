// SPDX-License-Identifier: BUSL-1.1

//! Which bridge-dispatcher capacity limit refused a request.
//!
//! Carried by [`Error::DispatchCapacity`](super::types::Error::DispatchCapacity).
//! Each scope is transient: the dispatcher enqueued nothing, and the same
//! request succeeds once in-flight responses free capacity.

use std::fmt;

use crate::types::{DatabaseId, TenantId};

/// The capacity limit that refused a dispatch, with the facts of the refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchCapacityScope {
    /// The tenant already holds its cap of in-flight requests.
    TenantInflight {
        /// The refused tenant.
        tenant_id: TenantId,
        /// The tenant's in-flight requests at the refusal.
        inflight: u32,
        /// The per-tenant in-flight cap.
        cap: u32,
    },
    /// The database's virtual queue on the target core is suspended at its
    /// fair share.
    DatabaseSuspended {
        /// The refused database.
        database_id: DatabaseId,
        /// The target core.
        core_id: usize,
    },
    /// The target core's weighted-fair queue is full.
    QueueFull {
        /// The target core.
        core_id: usize,
        /// The queue's total capacity in requests.
        capacity: u32,
    },
}

impl fmt::Display for DispatchCapacityScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TenantInflight {
                tenant_id,
                inflight,
                cap,
            } => write!(
                f,
                "tenant {tenant_id} holds {inflight}/{cap} in-flight requests"
            ),
            Self::DatabaseSuspended {
                database_id,
                core_id,
            } => write!(
                f,
                "database {database_id} virtual queue is suspended at its fair share on core \
                 {core_id}"
            ),
            Self::QueueFull { core_id, capacity } => {
                write!(f, "core {core_id} queue is full at {capacity} requests")
            }
        }
    }
}
