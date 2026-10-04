// SPDX-License-Identifier: BUSL-1.1

//! Stored rows as MessagePack images, for the Event Plane and the write-set
//! journal.
//!
//! The Event Plane has no schema, so it reads every row image as
//! MessagePack. A strict collection stores Binary Tuples, which are decoded
//! here against the collection's schema. A strict row that does not decode
//! is corrupt on disk: it is reported once at detection, and its image is
//! withheld, never handed on as raw bytes no reader can decode.

use nodedb_query::msgpack_scan;

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
    /// Tuple becomes MessagePack. A schemaless body gains its `id`.
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
                Ok(self.body_event_image(database_id, tid, collection, identity, stored))
            }
            StoredImage::Undecodable => Err(UndecodableImage),
        }
    }

    /// A MessagePack document body as the Event Plane reads it. A schemaless
    /// body gains its `id`, the identity every read injects.
    pub(in crate::data::executor) fn body_event_image(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        identity: &str,
        body: &[u8],
    ) -> Vec<u8> {
        if self.is_schemaless_document_collection(database_id, tid, collection) {
            msgpack_scan::inject_str_field(body, "id", identity)
        } else {
            body.to_vec()
        }
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
mod tests {
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_types::value::Value;

    use super::*;
    use crate::data::executor::core_loop::redo_image::StoredRow;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::engine::document::store::{CollectionConfig, RowIdentity};
    use crate::event::bus::{EventConsumerRx, create_event_bus_with_capacity};
    use crate::event::image_fault::ImageFault;
    use crate::event::{WriteEvent, WriteOp};
    use crate::types::{DatabaseId, TenantId};

    const TID: u64 = 1;
    const COLL: &str = "strict_images";
    /// Bytes no strict row is stored as: neither a Binary Tuple nor a map.
    const CORRUPT: &[u8] = b"corrupt row bytes";

    fn schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::String),
            ColumnDef::nullable("name", ColumnType::String),
        ])
        .expect("valid schema")
    }

    fn strict_core(dir: &std::path::Path) -> (CoreLoop, EventConsumerRx) {
        let (mut core, _tx, _rx) = make_core_with_dir(dir);
        core.seed_doc_configs(&[(
            (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
            CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema: schema() }),
        )]);
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
        core.set_event_producer(producers.pop().expect("producer"));
        (core, consumers.pop().expect("consumer"))
    }

    fn tuple(name: &str) -> Vec<u8> {
        let row = Value::Object(
            [
                ("id".to_string(), Value::String("r1".into())),
                ("name".to_string(), Value::String(name.into())),
            ]
            .into_iter()
            .collect(),
        );
        crate::data::executor::strict_format::value_to_binary_tuple(&row, &schema(), COLL)
            .expect("row encodes")
    }

    fn drain(events: &mut EventConsumerRx) -> Vec<WriteEvent> {
        std::iter::from_fn(|| events.try_recv()).collect()
    }

    fn name_of(image: &[u8]) -> String {
        nodedb_types::value_from_msgpack(image)
            .expect("image is MessagePack")
            .as_object()
            .and_then(|o| o.get("name"))
            .and_then(|v| v.as_str())
            .expect("name field")
            .to_owned()
    }

    #[test]
    fn a_tuple_converts_and_corrupt_bytes_are_undecodable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _events) = strict_core(dir.path());
        match core.resolve_event_payload(0, TID, COLL, &tuple("alice")) {
            StoredImage::Converted(image) => assert_eq!(name_of(&image), "alice"),
            other => panic!("a tuple converts, got {other:?}"),
        }
        assert_eq!(
            core.resolve_event_payload(0, TID, COLL, CORRUPT),
            StoredImage::Undecodable
        );
        // A collection with no strict schema reads its bytes as stored.
        assert_eq!(
            core.resolve_event_payload(0, TID, "schemaless", CORRUPT),
            StoredImage::AsStored
        );
    }

    #[test]
    fn a_corrupt_prior_row_withholds_the_old_image_and_names_the_fault() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, mut events) = strict_core(dir.path());
        let task = make_default_task();
        core.emit_put_event(
            &task,
            TID,
            COLL,
            RowIdentity::from_user_key("r1"),
            &tuple("bob"),
            Some(CORRUPT),
        );
        let emitted = drain(&mut events);
        assert_eq!(emitted.len(), 1);
        let event = &emitted[0];
        assert_eq!(event.op, WriteOp::Update);
        assert_eq!(event.image_fault, Some(ImageFault::Old));
        assert!(
            event.old_value.is_none(),
            "raw bytes never reach the Event Plane"
        );
        assert_eq!(
            name_of(event.new_value.as_deref().expect("new image")),
            "bob"
        );
    }

    #[test]
    fn a_corrupt_deleted_row_emits_no_raw_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, mut events) = strict_core(dir.path());
        let task = make_default_task();
        core.emit_document_delete_event(
            &task,
            TID,
            COLL,
            RowIdentity::from_user_key("r1"),
            Some(CORRUPT),
        );
        let emitted = drain(&mut events);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].op, WriteOp::Delete);
        assert_eq!(emitted[0].image_fault, Some(ImageFault::Old));
        assert!(emitted[0].old_value.is_none());

        // A decodable prior converts and names no fault.
        core.emit_document_delete_event(
            &task,
            TID,
            COLL,
            RowIdentity::from_user_key("r1"),
            Some(&tuple("carol")),
        );
        let emitted = drain(&mut events);
        assert_eq!(emitted[0].image_fault, None);
        assert_eq!(
            name_of(emitted[0].old_value.as_deref().expect("old image")),
            "carol"
        );
    }

    #[test]
    fn a_corrupt_post_image_refuses_its_write_set_and_stops_the_core() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _events) = strict_core(dir.path());
        let row = || StoredRow {
            database_id: 0,
            tid: TID,
            collection: COLL,
            surrogate: 7,
            identity: RowIdentity::from_user_key("r1"),
        };
        assert!(core.stored_row_image(row(), &tuple("dave"), None).is_ok());
        assert!(!core.is_fail_stopped());
        let error = core
            .stored_row_image(row(), CORRUPT, None)
            .expect_err("a corrupt row journals no redo body");
        assert!(
            matches!(error, crate::Error::Serialization { .. }),
            "{error:?}"
        );
        assert!(core.is_fail_stopped());
    }
}
