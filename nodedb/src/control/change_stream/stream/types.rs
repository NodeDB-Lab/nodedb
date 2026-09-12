// SPDX-License-Identifier: BUSL-1.1

use std::ops::Deref;

use nodedb_types::RowIdentity;

use crate::types::{DatabaseId, Lsn, TenantId};

use super::ChangeCursor;

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

/// A publication-ordered change event. The cursor is allocated atomically with
/// ring insertion and broadcast, rather than derived from the WAL LSN.
#[derive(Debug, Clone)]
pub struct SequencedChangeEvent {
    cursor: ChangeCursor,
    database_id: DatabaseId,
    event: ChangeEvent,
}

impl SequencedChangeEvent {
    pub(crate) fn new(cursor: ChangeCursor, database_id: DatabaseId, event: ChangeEvent) -> Self {
        Self {
            cursor,
            database_id,
            event,
        }
    }

    pub fn cursor(&self) -> ChangeCursor {
        self.cursor
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
