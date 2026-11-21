// SPDX-License-Identifier: BUSL-1.1

//! WAL payloads for the document secondary-index records
//! (`RecordType::DocumentIndexBuild` / `DocumentIndexRetire`).
//!
//! A document collection's index entries derive from its rows. The records
//! carry the build or purge itself, so restart replay runs it again at the
//! record's place in LSN order: after every row stored below it, and before
//! every row stored above it. Each record type has its own payload, so no
//! discriminator is needed.

use serde::{Deserialize, Serialize};

/// Payload of a `DocumentIndexBuild` record: every field of
/// `DocumentOp::BackfillIndex`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct DocumentIndexBuildRecord {
    pub collection: String,
    pub path: String,
    pub is_array: bool,
    pub unique: bool,
    pub case_insensitive: bool,
    /// Raw SQL text of a partial index's `WHERE` body. `None` for a full
    /// index.
    pub predicate: Option<String>,
    /// The index or declared field the build adds to the path.
    pub owner: nodedb_physical::physical_plan::DocIndexOwner,
    /// The descriptor the build derives from.
    pub fence: nodedb_physical::physical_plan::DocIndexFence,
}

/// Payload of a `DocumentIndexRetire` record: every field of
/// `DocumentOp::DropIndex`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct DocumentIndexRetireRecord {
    pub collection: String,
    pub field: String,
    /// The index or declared field the retire removes from the path.
    pub owner: nodedb_physical::physical_plan::DocIndexOwner,
    /// The descriptor the retire derives from.
    pub fence: nodedb_physical::physical_plan::DocIndexFence,
}

impl DocumentIndexBuildRecord {
    pub(crate) fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal document index build: {e}"),
        })
    }

    pub(crate) fn from_bytes(payload: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal document index build: {e}"),
        })
    }
}

impl DocumentIndexRetireRecord {
    pub(crate) fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal document index retire: {e}"),
        })
    }

    pub(crate) fn from_bytes(payload: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal document index retire: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_record_round_trips() {
        let record = DocumentIndexBuildRecord {
            collection: "users".into(),
            path: "$.email".into(),
            is_array: false,
            unique: true,
            case_insensitive: true,
            predicate: Some("active = true".into()),
            owner: nodedb_physical::physical_plan::DocIndexOwner::Named("by_email".into()),
            fence: nodedb_physical::physical_plan::DocIndexFence {
                incarnation: nodedb_types::Hlc::new(5, 0),
                descriptor_version: 3,
            },
        };
        let bytes = record.to_bytes().expect("encode");
        assert_eq!(
            DocumentIndexBuildRecord::from_bytes(&bytes).expect("decode"),
            record
        );
    }

    #[test]
    fn retire_record_round_trips() {
        let record = DocumentIndexRetireRecord {
            collection: "users".into(),
            field: "$.email".into(),
            owner: nodedb_physical::physical_plan::DocIndexOwner::Declared,
            fence: nodedb_physical::physical_plan::DocIndexFence {
                incarnation: nodedb_types::Hlc::new(5, 0),
                descriptor_version: 4,
            },
        };
        let bytes = record.to_bytes().expect("encode");
        assert_eq!(
            DocumentIndexRetireRecord::from_bytes(&bytes).expect("decode"),
            record
        );
    }
}
