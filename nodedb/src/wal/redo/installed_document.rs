// SPDX-License-Identifier: BUSL-1.1

//! Exact installed-document payloads. Grouped rows reference their part's schema table.

use crate::types::installed_document::InstalledDocument;
use nodedb_physical::physical_plan::StorageMode;
use serde::{Deserialize, Serialize};

const INDEXED: &[u8] = b"nodedb.installed_document.indexed";
const RESOLVED: &[u8] = b"nodedb.installed_document";

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
pub struct InstalledDocumentRedo<S> {
    pub collection: String,
    pub document_id: String,
    pub surrogate: u32,
    pub image: InstalledDocument<S>,
    pub event: crate::types::document_event::DocumentPutEvent,
}

impl InstalledDocumentRedo<u32> {
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&(marker(INDEXED), self)).map_err(codec_error)
    }

    pub fn decode(payload: &[u8]) -> crate::Result<Option<Self>> {
        decode_tagged(payload, INDEXED)
    }

    pub fn resolve(
        self,
        schemas: &[StorageMode],
    ) -> crate::Result<InstalledDocumentRedo<StorageMode>> {
        Ok(InstalledDocumentRedo {
            collection: self.collection,
            document_id: self.document_id,
            surrogate: self.surrogate,
            event: self.event,
            image: InstalledDocument {
                schema: super::capture::schema_at(schemas, self.image.schema)?.clone(),
                canonical: self.image.canonical,
                stored: self.image.stored,
                version: self.image.version,
                chain_head: self.image.chain_head,
            },
        })
    }
}

impl InstalledDocumentRedo<StorageMode> {
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&(marker(RESOLVED), self)).map_err(codec_error)
    }

    pub fn decode(payload: &[u8]) -> crate::Result<Option<Self>> {
        decode_tagged(payload, RESOLVED)
    }
}

fn marker(bytes: &[u8]) -> zerompk::Value<'_> {
    zerompk::Value::Binary(std::borrow::Cow::Borrowed(bytes))
}

fn codec_error(error: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("installed document payload: {error}"),
    }
}

fn decode_tagged<T: zerompk::FromMessagePackOwned>(
    payload: &[u8],
    expected: &[u8],
) -> crate::Result<Option<T>> {
    use zerompk::Read;
    let mut reader = zerompk::SliceReader::new(payload);
    let Ok(length) = reader.read_array_len() else {
        return Ok(None);
    };
    let Ok(kind) = reader.read_binary() else {
        return Ok(None);
    };
    if kind.as_ref() != expected {
        return Ok(None);
    }
    if length != 2 {
        return Err(codec_error(format!(
            "installed document expects two elements, got {length}"
        )));
    }
    let row = T::read(&mut reader).map_err(codec_error)?;
    if reader.peek_marker().is_ok() {
        return Err(codec_error("installed document contains trailing bytes"));
    }
    Ok(Some(row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::RowVersion;
    use crate::types::hash_chain::ChainHead;

    fn indexed() -> InstalledDocumentRedo<u32> {
        InstalledDocumentRedo {
            collection: "ledger".into(),
            document_id: "a".into(),
            surrogate: 7,
            event: crate::types::document_event::DocumentPutEvent {
                new_value: vec![0x81, 0xa1, b'x', 2],
                old_value: Some(vec![0x80]),
            },
            image: InstalledDocument {
                canonical: vec![0x81, 0xa1, b'x', 1],
                stored: vec![0x91, 1],
                version: RowVersion {
                    sys_from: 37,
                    valid_from_ms: 2,
                    valid_until_ms: 8,
                },
                schema: 0,
                chain_head: Some(ChainHead {
                    seq: 9,
                    hash: "head".into(),
                }),
            },
        }
    }

    #[test]
    fn installed_payload_preserves_exact_bytes_intervals_and_head() {
        let row = indexed();
        let decoded = InstalledDocumentRedo::<u32>::decode(&row.encode().expect("encode"))
            .expect("decode")
            .expect("installed");
        assert_eq!(decoded, row);
        let resolved = decoded.resolve(&[StorageMode::Schemaless]).expect("schema");
        let decoded = InstalledDocumentRedo::<StorageMode>::decode(
            &resolved.encode().expect("encode resolved"),
        )
        .expect("decode resolved")
        .expect("installed");
        assert_eq!(decoded, resolved);
        assert_eq!(decoded.image.stored, vec![0x91, 1]);
        assert_ne!(decoded.image.canonical, decoded.image.stored);
        assert_ne!(decoded.event.new_value, decoded.image.canonical);
        assert_eq!(decoded.event.new_value, vec![0x81, 0xa1, b'x', 2]);
        assert_eq!(decoded.event.old_value, Some(vec![0x80]));
    }

    #[test]
    fn installed_schema_reference_requires_a_witness() {
        assert!(indexed().resolve(&[]).is_err());
        let mut row = indexed();
        row.image.schema = 1;
        assert!(row.resolve(&[StorageMode::Schemaless]).is_err());
    }

    #[test]
    fn installed_tagged_malformed_payloads_return_errors() {
        for payload in [
            zerompk::to_msgpack_vec(&(marker(INDEXED), ())).expect("nil"),
            zerompk::to_msgpack_vec(&vec![marker(INDEXED)]).expect("arity"),
        ] {
            assert!(InstalledDocumentRedo::<u32>::decode(&payload).is_err());
        }
        let mut trailing = indexed().encode().expect("encode");
        trailing.push(0);
        assert!(InstalledDocumentRedo::<u32>::decode(&trailing).is_err());
        assert!(
            InstalledDocumentRedo::<u32>::decode(
                &zerompk::to_msgpack_vec(&("other", ())).expect("other")
            )
            .expect("unrelated")
            .is_none()
        );
    }
    #[test]
    fn submitted_collection_names_never_select_installed_payloads() {
        for collection in ["installed_document", "installed_document_indexed"] {
            let payload = zerompk::to_msgpack_vec(&(
                collection,
                "row",
                vec![0x80u8],
                Option::<nodedb_types::sync::wire::SyncProvenance>::None,
                7u32,
            ))
            .expect("submitted");
            assert!(
                InstalledDocumentRedo::<u32>::decode(&payload)
                    .expect("unrelated indexed")
                    .is_none()
            );
            assert!(
                InstalledDocumentRedo::<StorageMode>::decode(&payload)
                    .expect("unrelated resolved")
                    .is_none()
            );
            assert!(crate::wal::DocumentPutRedo::decode(&payload).is_some());
        }
    }
}
