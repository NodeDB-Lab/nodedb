// SPDX-License-Identifier: BUSL-1.1

//! Installed document images and public event pairs stored beside a journalled write.
//!
//! A Data Plane core stores it beside the write's effects, in the transaction
//! that persists them (see `crate::engine::durability_gate`). Boot journals a
//! stored write set whose group the WAL lacks as that group's parts, exactly
//! as the Control Plane journals a response's write set. When a crash cut the
//! write's origin from the WAL, boot first journals the origin again from
//! the stored [`OriginAppend`].

use crate::types::installed_document::{CapturedInstalledDocument, InstalledDocument};
use nodedb_physical::physical_plan::StorageMode;
use serde::{Deserialize, Serialize};

use super::record::{EdgeDeleteRedo, EdgePutRedo};
use crate::bridge::envelope::{EdgeImage, RowEffect, WriteSetEntry};

/// One write's stored write set and the group it belongs to.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct WriteSetCapture {
    pub tenant_id: u64,
    pub vshard_id: u32,
    pub database_id: u64,
    /// The collection of every entry without its own.
    pub collection: String,
    /// The group origin LSN.
    pub origin: u64,
    pub entries: Vec<CapturedEntry>,
    pub schemas: Vec<StorageMode>,
    pub origin_append: OriginAppend,
}

/// What the write appended before dispatch, in the shape the funnel appends
/// it from.
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
#[msgpack(map)]
pub struct OriginAppend {
    /// The dispatched plan, encoded as MessagePack.
    pub plan: Vec<u8>,
    /// The idempotency key every record of the write carries.
    pub apply_key: u64,
    /// The WAL code of the write's event source.
    pub event_source: u8,
    /// The commit instant every record carries, when it was known before the
    /// append.
    pub commit_hlc: Option<u64>,
    /// The instant the forward record resolved.
    pub resolved_now_ms: Option<u64>,
    /// `(epoch, group_id, log_index)` of the replicated position whose marker
    /// precedes the origin.
    pub change_position: Option<(u64, u64, u64)>,
}

/// One stored [`WriteSetEntry`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct CapturedEntry {
    pub surrogate: u32,
    pub identity: String,
    pub collection: Option<String>,
    pub effect: CapturedEffect,
}

/// One stored [`RowEffect`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CapturedEffect {
    InstalledPut {
        image: CapturedInstalledDocument,
        event: crate::types::document_event::DocumentPutEvent,
    },
    Delete {
        /// The tombstone's system-time ordinal.
        system_from: i64,
    },
    CancelForward,
    EdgePut(EdgePutRedo),
    EdgeDelete(EdgeDeleteRedo),
}

pub(crate) fn intern_schema(
    schemas: &mut Vec<StorageMode>,
    schema: &StorageMode,
) -> crate::Result<u32> {
    if let Some(index) = schemas.iter().position(|existing| existing == schema) {
        return u32::try_from(index)
            .map_err(|_| schema_error("schema table exceeds u32 references"));
    }
    let index = u32::try_from(schemas.len())
        .map_err(|_| schema_error("schema table exceeds u32 references"))?;
    schemas.push(schema.clone());
    Ok(index)
}

pub(crate) fn schema_at(schemas: &[StorageMode], index: u32) -> crate::Result<&StorageMode> {
    schemas.get(index as usize).ok_or_else(|| {
        schema_error(&format!(
            "installed document schema reference {index} exceeds table length {}",
            schemas.len()
        ))
    })
}

fn schema_error(detail: &str) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: detail.into(),
    }
}

impl CapturedEntry {
    pub fn from_entry(
        entry: &WriteSetEntry,
        schemas: &mut Vec<StorageMode>,
    ) -> crate::Result<Self> {
        let effect = match &entry.effect {
            RowEffect::InstalledPut { image, event } => CapturedEffect::InstalledPut {
                image: InstalledDocument {
                    canonical: image.canonical.clone(),
                    stored: image.stored.clone(),
                    version: image.version,
                    schema: intern_schema(schemas, &image.schema)?,
                    chain_head: image.chain_head.clone(),
                },
                event: event.clone(),
            },
            RowEffect::Delete { system_from } => CapturedEffect::Delete {
                system_from: *system_from,
            },
            RowEffect::CancelForward => CapturedEffect::CancelForward,
            RowEffect::Edge(EdgeImage::Put(put)) => CapturedEffect::EdgePut(put.clone()),
            RowEffect::Edge(EdgeImage::Delete(delete)) => {
                CapturedEffect::EdgeDelete(delete.clone())
            }
        };
        Ok(Self {
            surrogate: entry.surrogate,
            identity: entry.identity.as_str().to_string(),
            collection: entry.collection.clone(),
            effect,
        })
    }

    pub fn into_entry(
        self,
        schemas: &[std::sync::Arc<StorageMode>],
    ) -> crate::Result<WriteSetEntry> {
        let effect = match self.effect {
            CapturedEffect::InstalledPut { image, event } => RowEffect::InstalledPut {
                image: InstalledDocument {
                    canonical: image.canonical,
                    stored: image.stored,
                    version: image.version,
                    schema: schemas.get(image.schema as usize).cloned().ok_or_else(|| {
                        schema_error(&format!(
                            "installed document schema reference {} exceeds table length {}",
                            image.schema,
                            schemas.len()
                        ))
                    })?,
                    chain_head: image.chain_head,
                },
                event,
            },
            CapturedEffect::Delete { system_from } => RowEffect::Delete { system_from },
            CapturedEffect::CancelForward => RowEffect::CancelForward,
            CapturedEffect::EdgePut(put) => RowEffect::Edge(EdgeImage::Put(put)),
            CapturedEffect::EdgeDelete(delete) => RowEffect::Edge(EdgeImage::Delete(delete)),
        };
        Ok(WriteSetEntry {
            surrogate: self.surrogate,
            identity: nodedb_types::RowIdentity::from_user_key(self.identity),
            effect,
            collection: self.collection,
        })
    }
}

impl WriteSetCapture {
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("write set capture encode: {e}"),
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        let capture: Self =
            zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("write set capture decode: {e}"),
            })?;
        for entry in &capture.entries {
            if let CapturedEffect::InstalledPut { image, .. } = &entry.effect {
                schema_at(&capture.schemas, image.schema)?;
            }
        }
        Ok(capture)
    }

    /// The write set as the response carried it.
    pub fn write_set(&self) -> crate::Result<Vec<WriteSetEntry>> {
        let schemas: Vec<_> = self
            .schemas
            .iter()
            .cloned()
            .map(std::sync::Arc::new)
            .collect();
        self.entries
            .iter()
            .cloned()
            .map(|entry| entry.into_entry(&schemas))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::RowVersion;

    fn installed_entry(
        surrogate: u32,
        identity: nodedb_types::RowIdentity,
        payload: Vec<u8>,
        version: RowVersion,
    ) -> WriteSetEntry {
        let value =
            nodedb_types::json_to_msgpack(&serde_json::json!({"payload": payload})).expect("body");
        WriteSetEntry::installed_put(
            surrogate,
            identity,
            InstalledDocument {
                canonical: value.clone(),
                stored: value.clone(),
                version,
                schema: std::sync::Arc::new(StorageMode::Schemaless),
                chain_head: None,
            },
            crate::types::document_event::DocumentPutEvent {
                new_value: value,
                old_value: None,
            },
        )
    }

    #[test]
    fn a_capture_round_trips_every_effect() {
        let identity = nodedb_types::RowIdentity::from_user_key("r1");
        let entries = vec![
            installed_entry(1, identity.clone(), b"v".to_vec(), RowVersion::open(40)),
            installed_entry(2, identity.clone(), b"w".to_vec(), RowVersion::open(41))
                .in_collection("other".to_string()),
            WriteSetEntry::delete(3, identity.clone(), 42),
            WriteSetEntry::cancel_forward(1, identity),
            WriteSetEntry::edge(EdgeImage::Delete(EdgeDeleteRedo {
                collection: "g".into(),
                src_id: "n".into(),
                label: "L".into(),
                dst_id: "m".into(),
                src_surrogate: 3,
                dst_surrogate: 4,
                system_from: Some(9),
                applied: None,
            })),
        ];
        let mut schemas = Vec::new();
        let captured_entries = entries
            .iter()
            .map(|entry| CapturedEntry::from_entry(entry, &mut schemas).expect("capture"))
            .collect();
        let capture = WriteSetCapture {
            tenant_id: 1,
            vshard_id: 2,
            database_id: 0,
            collection: "orders".into(),
            origin: 77,
            entries: captured_entries,
            schemas,
            origin_append: OriginAppend {
                plan: vec![0x90],
                apply_key: 5,
                event_source: crate::event::EventSource::User.wal_code(),
                commit_hlc: Some(11),
                resolved_now_ms: None,
                change_position: Some((1, 2, 3)),
            },
        };
        let decoded = WriteSetCapture::from_bytes(&capture.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, capture);
        let restored = decoded.write_set().unwrap();
        assert_eq!(restored.len(), entries.len());
        for (restored, original) in restored.iter().zip(&entries) {
            assert_eq!(restored.effect, original.effect);
            assert_eq!(restored.identity, original.identity);
            assert_eq!(restored.collection, original.collection);
            assert_eq!(restored.surrogate, original.surrogate);
        }
    }
    #[test]
    fn installed_capture_shares_schema_witnesses_and_checks_references() {
        let image = InstalledDocument {
            canonical: vec![0x80],
            stored: vec![0x80],
            version: RowVersion::open(9),
            schema: std::sync::Arc::new(StorageMode::Schemaless),
            chain_head: None,
        };
        let entries = [
            WriteSetEntry::installed_put(
                1,
                nodedb_types::RowIdentity::from_user_key("a"),
                image.clone(),
                crate::types::document_event::DocumentPutEvent {
                    new_value: vec![0x81, 0xa1, b'x', 1],
                    old_value: Some(vec![0x80]),
                },
            ),
            WriteSetEntry::installed_put(
                2,
                nodedb_types::RowIdentity::from_user_key("b"),
                image,
                crate::types::document_event::DocumentPutEvent {
                    new_value: vec![0x81, 0xa1, b'x', 1],
                    old_value: Some(vec![0x80]),
                },
            ),
        ];
        let mut schemas = Vec::new();
        let entries: Vec<_> = entries
            .iter()
            .map(|entry| CapturedEntry::from_entry(entry, &mut schemas).expect("capture"))
            .collect();
        assert_eq!(schemas.len(), 1);
        let shared: Vec<_> = schemas.into_iter().map(std::sync::Arc::new).collect();
        let first = entries[0].clone().into_entry(&shared).expect("first");
        let second = entries[1].clone().into_entry(&shared).expect("second");
        match (&first.effect, &second.effect) {
            (
                RowEffect::InstalledPut {
                    image: first,
                    event: first_event,
                },
                RowEffect::InstalledPut {
                    image: second,
                    event: second_event,
                },
            ) => {
                assert!(std::sync::Arc::ptr_eq(&first.schema, &second.schema));
                assert_eq!(first_event, second_event);
                assert_ne!(&first_event.new_value, &first.canonical);
                assert_eq!(
                    first_event,
                    &crate::types::document_event::DocumentPutEvent {
                        new_value: vec![0x81, 0xa1, b'x', 1],
                        old_value: Some(vec![0x80])
                    }
                );
            }
            _ => panic!("installed effects"),
        }
        assert!(entries[0].clone().into_entry(&[]).is_err());
    }
}
