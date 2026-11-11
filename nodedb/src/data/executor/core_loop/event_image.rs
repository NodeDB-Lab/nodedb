// SPDX-License-Identifier: BUSL-1.1

//! Stored rows as MessagePack images, for the Event Plane and the write-set
//! journal.
//!
//! The Event Plane has no schema, so it reads every row image as
//! MessagePack. A strict collection stores Binary Tuples, which are decoded
//! here against the collection's schema. A strict row that does not decode
//! is corrupt on disk: it is reported once at detection, and its image is
//! withheld, never handed on as raw bytes no reader can decode.

use super::CoreLoop;
use crate::data::executor::strict_format::strict_row_to_msgpack;

/// How a stored row reads as MessagePack.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::data::executor) enum StoredImage {
    /// The stored bytes are MessagePack already: a schemaless body, a row of
    /// a collection this core holds no strict schema for, or a MessagePack
    /// map stored before its collection became strict.
    AsStored,
    /// A strict Binary Tuple, decoded to MessagePack.
    Converted(Vec<u8>),
    /// A strict row that decodes neither as a Binary Tuple nor as a
    /// MessagePack map. The recorder report is already filed.
    Undecodable,
}

/// A stored strict row with no MessagePack image. The recorder report is
/// already filed where the decode failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) struct UndecodableImage;

impl CoreLoop {
    /// Render the accepted post-image once, using its captured storage schema.
    pub(in crate::data::executor) fn prepare_installed_event_image(
        &self,
        row: &super::redo_image::WriteSetRow<'_>,
        image: &crate::types::SharedInstalledDocument,
    ) -> crate::Result<Vec<u8>> {
        use nodedb_physical::physical_plan::StorageMode;
        match &*image.schema {
            StorageMode::Schemaless => self.native_body_event_image(
                row.database_id,
                row.tid,
                row.collection,
                row.identity.as_str(),
                &image.stored,
                true,
            ),
            StorageMode::Strict { schema } => {
                let version = nodedb_strict::TupleDecoder::new(schema)
                    .schema_version(&image.stored)
                    .map_err(|error| crate::Error::Serialization {
                        format: "binary_tuple".into(),
                        detail: format!(
                            "installed event tuple version for '{}': {error}",
                            row.collection
                        ),
                    })?;
                if version == 0 || version > schema.version {
                    return Err(crate::Error::Serialization {
                        format: "binary_tuple".into(),
                        detail: format!(
                            "installed event tuple version {version} for '{}' is outside captured schema versions 1..={}",
                            row.collection, schema.version,
                        ),
                    });
                }
                match strict_row_to_msgpack(&image.stored, schema) {
                    Ok(Some(bytes)) => Ok(bytes),
                    Ok(None) => Err(crate::Error::Serialization {
                        format: "binary_tuple".into(),
                        detail: format!(
                            "installed event row '{}' of '{}' is not a Binary Tuple under captured schema version {version}",
                            row.identity.as_str(),
                            row.collection
                        ),
                    }),
                    Err(fault) => Err(crate::Error::Serialization {
                        format: "binary_tuple".into(),
                        detail: format!(
                            "installed event row '{}' of '{}' cannot decode under captured schema version {version}: {}",
                            row.identity.as_str(),
                            row.collection,
                            fault.as_str()
                        ),
                    }),
                }
            }
        }
    }

    /// Capture both public row images before the source transaction commits.
    pub(in crate::data::executor) fn prepare_installed_put_event(
        &self,
        row: &super::redo_image::WriteSetRow<'_>,
        image: &crate::types::SharedInstalledDocument,
        prior_stored: Option<&[u8]>,
    ) -> crate::Result<crate::types::document_event::DocumentPutEvent> {
        let new_value = self.prepare_installed_event_image(row, image)?;
        let old_value = prior_stored.map(|prior| {
            match &*image.schema {
                nodedb_physical::physical_plan::StorageMode::Schemaless => {
                    self.native_body_event_image(
                        row.database_id, row.tid, row.collection,
                        row.identity.as_str(), prior, true,
                    )
                }
                nodedb_physical::physical_plan::StorageMode::Strict { schema } => {
                    match strict_row_to_msgpack(prior, schema) {
                        Ok(Some(bytes)) => Ok(bytes),
                        Ok(None) => self.native_body_event_image(
                            row.database_id, row.tid, row.collection,
                            row.identity.as_str(), prior, false,
                        ),
                        Err(fault) => Err(crate::Error::Serialization {
                            format: "binary_tuple".into(),
                            detail: format!(
                                "prior event row '{}' of '{}' cannot decode under captured schema: {}",
                                row.identity.as_str(), row.collection, fault.as_str(),
                            ),
                        }),
                    }
                }
            }
        }).transpose()?;
        let event = crate::types::document_event::DocumentPutEvent {
            new_value,
            old_value,
        };
        // Every image the row's write-set entry journals is held to the row
        // limit before the caller commits, so a refusal leaves no row, no
        // write set and no event.
        crate::data::executor::row_image_limit::check_installed_row(
            row.collection,
            row.identity.as_str(),
            &image.canonical,
            &image.stored,
            &event,
        )?;
        Ok(event)
    }

    /// How `stored`, a row of `collection`, reads as MessagePack.
    pub(in crate::data::executor) fn resolve_event_payload(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        stored: &[u8],
    ) -> StoredImage {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let Some(config) = self.doc_configs.get(&config_key) else {
            return StoredImage::AsStored;
        };
        let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
            config.storage_mode
        else {
            return StoredImage::AsStored;
        };
        match strict_row_to_msgpack(stored, schema) {
            Ok(Some(converted)) => StoredImage::Converted(converted),
            Ok(None) => StoredImage::AsStored,
            Err(fault) => {
                tracing::error!(
                    core = self.core_id,
                    collection,
                    fault = fault.as_str(),
                    "stored strict row does not decode; its image is withheld"
                );
                crate::diag::strict_row_image_unrendered(collection, fault.as_str());
                StoredImage::Undecodable
            }
        }
    }

    /// A stored document row as the Event Plane reads it. A strict Binary
    /// Tuple becomes MessagePack. A schemaless body gains its identity under
    /// the collection's identity column when it lacks that column.
    pub(in crate::data::executor) fn stored_event_image(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        identity: &str,
        stored: &[u8],
    ) -> Result<Vec<u8>, UndecodableImage> {
        match self.resolve_event_payload(database_id, tid, collection, stored) {
            StoredImage::Converted(converted) => Ok(converted),
            StoredImage::AsStored => {
                self.body_event_image(database_id, tid, collection, identity, stored)
                    .map_err(|error| {
                        tracing::error!(collection, %error, "stored document event image cannot render");
                        UndecodableImage
                    })
            }
            StoredImage::Undecodable => Err(UndecodableImage),
        }
    }

    /// A MessagePack document body as the Event Plane reads it. A schemaless
    /// body that lacks its identity column gains the identity there, as every
    /// read injects it. A declared-key body holds its key and gains no `id`.
    pub(in crate::data::executor) fn body_event_image(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        identity: &str,
        body: &[u8],
    ) -> crate::Result<Vec<u8>> {
        self.native_body_event_image(
            database_id,
            tid,
            collection,
            identity,
            body,
            self.is_schemaless_document_collection(database_id, tid, collection),
        )
    }

    /// Convert native storage to the public event shape at the final boundary.
    fn native_body_event_image(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        identity: &str,
        body: &[u8],
        inject_identity: bool,
    ) -> crate::Result<Vec<u8>> {
        let mut value = crate::data::executor::doc_format::decode_document_value(body)?;
        let nodedb_types::Value::Object(fields) = &mut value else {
            return Err(crate::Error::Serialization {
                format: "document".into(),
                detail: format!(
                    "public event row '{identity}' of '{collection}' is not a document object"
                ),
            });
        };
        if inject_identity {
            fields
                .entry(self.identity_column(database_id, tid, collection))
                .or_insert_with(|| nodedb_types::Value::String(identity.to_owned()));
        }
        nodedb_types::value_to_msgpack(&value).map_err(|error| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("render public event row '{identity}' of '{collection}': {error}"),
        })
    }

    /// Whether `collection` is a schemaless document collection.
    ///
    /// A schemaless body carries no storage key of its own, so its `id` field
    /// is absent whenever the caller declared no `id` column. A strict row's
    /// `id` is a real tuple column, already present after Binary Tuple
    /// conversion, so it needs no identity injection.
    fn is_schemaless_document_collection(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> bool {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        matches!(
            self.doc_configs.get(&config_key).map(|c| &c.storage_mode),
            Some(nodedb_physical::physical_plan::StorageMode::Schemaless)
        )
    }
}

#[cfg(test)]
#[path = "event_image_tests.rs"]
mod tests;
