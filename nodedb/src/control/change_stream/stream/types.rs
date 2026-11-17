// SPDX-License-Identifier: BUSL-1.1

use std::ops::Deref;

use nodedb_types::RowIdentity;

use crate::types::{DatabaseId, Lsn, TenantId};

use crate::event::cdc::CdcOffset;

use super::ChangePartition;

/// A single mutation event broadcast by the change stream.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub lsn: Lsn,
    pub tenant_id: TenantId,
    pub collection: String,
    /// The identity a subscriber addresses the changed row by. A batch or
    /// predicate write carries `"*"`: every row in the collection.
    pub document_id: RowIdentity,
    pub operation: ChangeOperation,
    pub timestamp_ms: u64,
    pub after: Option<serde_json::Value>,
}

/// Type of mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperation {
    Insert,
    Update,
    Delete,
}

impl ChangeOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

/// A change event at its position in its partition's feed.
#[derive(Debug, Clone)]
pub struct SequencedChangeEvent {
    partition: ChangePartition,
    position: CdcOffset,
    /// The node's feed of `partition` holds every event above this position.
    /// A consumer whose cursor sits below it can have missed events.
    floor: CdcOffset,
    database_id: DatabaseId,
    event: ChangeEvent,
}

impl SequencedChangeEvent {
    pub(crate) fn new(
        partition: ChangePartition,
        position: CdcOffset,
        floor: CdcOffset,
        database_id: DatabaseId,
        event: ChangeEvent,
    ) -> Self {
        Self {
            partition,
            position,
            floor,
            database_id,
            event,
        }
    }

    pub fn partition(&self) -> ChangePartition {
        self.partition
    }

    pub fn position(&self) -> CdcOffset {
        self.position
    }

    pub fn floor(&self) -> CdcOffset {
        self.floor
    }

    pub fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub fn event(&self) -> &ChangeEvent {
        &self.event
    }

    pub fn into_event(self) -> ChangeEvent {
        self.event
    }
}

impl Deref for SequencedChangeEvent {
    type Target = ChangeEvent;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

/// What a node did with a run a peer forwarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOutcome {
    /// The journal holds every event of the run: this call wrote it, or an
    /// earlier call took it.
    Durable,
    /// The node took nothing of the run: its journal did not write it. The
    /// sender must send it again. Carries the journal's error.
    Refused(String),
}
