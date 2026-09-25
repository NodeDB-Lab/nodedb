// SPDX-License-Identifier: BUSL-1.1

//! The durable ledgers of the Event Plane's non-idempotent sinks.
//!
//! - **DML audit:** the durable audit log, read back at startup into
//!   [`AuditedKeys`].
//! - **CRDT sync packaging:** [`CrdtLedger`].
//! - **CDC change streams:** [`CdcLedger`], which holds the stream buffers
//!   with the keys of the events routed into them.
//! - **Streaming materialized views:** the applied keys persisted with the
//!   view state itself (see `streaming_mv::persist`).
//!
//! Each sink stores an event's key with its effect, so an event delivered
//! again after a restart is applied once.

use crate::event::watermark::WatermarkStore;
use crate::wal::WalManager;

use super::audit::AuditedKeys;
use super::cdc::CdcLedger;
use super::crdt::CrdtLedger;

/// The sink ledgers of this node.
pub struct SinkLedgers {
    pub audited: AuditedKeys,
    pub crdt: CrdtLedger,
    pub cdc: CdcLedger,
}

impl SinkLedgers {
    /// Open the ledgers beside the watermark store, reading audited keys
    /// above the persisted watermark of each of `num_cores` cores.
    pub fn open(
        wal: &WalManager,
        watermarks: &WatermarkStore,
        num_cores: usize,
    ) -> crate::Result<Self> {
        let mut floors = Vec::with_capacity(num_cores);
        for core in 0..num_cores {
            floors.push(watermarks.load(core)?.as_u64());
        }
        let audited = AuditedKeys::from_recovered(&wal.recover_audit_entries()?, |core| {
            floors.get(core as usize).copied().unwrap_or(0)
        })?;
        Ok(Self {
            audited,
            crdt: CrdtLedger::open(watermarks.dir())?,
            cdc: CdcLedger::open(watermarks.dir())?,
        })
    }

    /// Drop the keys of `core` at or below `through`: the consumer persisted
    /// its watermark there and never delivers them again.
    pub fn prune(&self, core: u32, through: u64) -> crate::Result<()> {
        self.audited.prune(core, through);
        self.crdt.prune(core, through)?;
        self.cdc.prune(core, through)
    }
}
