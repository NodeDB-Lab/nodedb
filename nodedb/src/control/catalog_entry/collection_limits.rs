// SPDX-License-Identifier: BUSL-1.1

//! The size limits a collection descriptor passes before it is proposed.
//!
//! A write-set part carries a row's collection name and its schema witness.
//! A row stored under an earlier schema version renders the columns added
//! since. The bound that fits one row in one WAL record relies on all three,
//! so CREATE and ALTER refuse a descriptor past any of them with SQLSTATE
//! 54000, before the entry reaches the metadata log.

use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::columnar::StrictSchema;
use nodedb_types::{
    CollectionType, DocumentMode, MAX_ADDED_COLUMN_IMAGE_BYTES, MAX_COLLECTION_NAME_BYTES,
    MAX_SCHEMA_WITNESS_BYTES, added_column_image_bytes,
};

use crate::bridge::envelope::ErrorCode;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::StoredCollection;

/// Refuse `entry` when it puts a collection descriptor past a size limit.
pub(crate) fn check_collection_limits(entry: &CatalogEntry) -> crate::Result<()> {
    match entry {
        CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
            check_stored_collection(stored)
        }
        _ => Ok(()),
    }
}

/// Refuse `stored` when its name, its schema witness, or what its added
/// columns add to an older row is past its limit.
pub(crate) fn check_stored_collection(stored: &StoredCollection) -> crate::Result<()> {
    if stored.name.len() > MAX_COLLECTION_NAME_BYTES {
        return Err(refusal(format!(
            "collection name '{}' is {} bytes, over the limit of {MAX_COLLECTION_NAME_BYTES} \
             bytes; use a shorter name",
            crate::data::executor::row_image_limit::shown_id(&stored.name),
            stored.name.len()
        )));
    }
    let Some(schema) = strict_schema(&stored.collection_type) else {
        return Ok(());
    };
    let witness = zerompk::to_msgpack_vec(&vec![StorageMode::Strict {
        schema: schema.clone(),
    }])
    .map_err(|error| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("schema witness of '{}': {error}", stored.name),
    })?
    .len();
    if witness > MAX_SCHEMA_WITNESS_BYTES {
        return Err(refusal(format!(
            "the schema of '{}' encodes to {witness} bytes, over the limit of \
             {MAX_SCHEMA_WITNESS_BYTES} bytes; declare fewer columns or shorter names and \
             defaults",
            stored.name
        )));
    }
    let added = added_column_image_bytes(schema);
    if added > MAX_ADDED_COLUMN_IMAGE_BYTES {
        return Err(refusal(format!(
            "the columns added to '{}' add up to {added} bytes to a row stored before them, \
             over the limit of {MAX_ADDED_COLUMN_IMAGE_BYTES} bytes; use shorter names and \
             defaults for added columns",
            stored.name
        )));
    }
    Ok(())
}

/// The strict schema rows of `collection_type` are stored under.
fn strict_schema(collection_type: &CollectionType) -> Option<&StrictSchema> {
    match collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => Some(schema),
        CollectionType::KeyValue(config) => Some(&config.schema),
        CollectionType::Document(DocumentMode::Schemaless) | CollectionType::Columnar(_) => None,
    }
}

fn refusal(detail: String) -> crate::Error {
    crate::Error::DataPlane(ErrorCode::ProgramLimitExceeded { detail })
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    use super::*;
    use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;

    fn assert_refused(stored: StoredCollection) {
        let error = check_collection_limits(&CatalogEntry::PutCollection(Box::new(stored)))
            .expect_err("refused");
        let crate::Error::DataPlane(code) = &error else {
            panic!("expected a program limit refusal, got {error:?}");
        };
        assert!(
            matches!(code, ErrorCode::ProgramLimitExceeded { .. }),
            "{code:?}"
        );
        assert_eq!(error_code_to_sqlstate(code).1, "54000");
    }

    fn strict(columns: Vec<ColumnDef>) -> StrictSchema {
        StrictSchema::new(columns).expect("strict schema")
    }

    #[test]
    fn a_name_at_the_limit_passes_and_one_past_it_refuses() {
        let at = StoredCollection::new(1, &"n".repeat(MAX_COLLECTION_NAME_BYTES), "admin");
        check_stored_collection(&at).expect("a name at the limit");
        assert_refused(StoredCollection::new(
            1,
            &"n".repeat(MAX_COLLECTION_NAME_BYTES + 1),
            "admin",
        ));
    }

    #[test]
    fn a_strict_schema_whose_witness_is_over_the_limit_refuses() {
        let name_bytes = 64 * 1024;
        let columns = (0..MAX_SCHEMA_WITNESS_BYTES / name_bytes + 1)
            .map(|index| {
                ColumnDef::nullable(
                    format!("{index:08}{}", "c".repeat(name_bytes)),
                    ColumnType::String,
                )
            })
            .collect();
        let mut stored = StoredCollection::new(1, "wide", "admin");
        stored.collection_type = CollectionType::strict(strict(columns));
        assert_refused(stored);
    }

    #[test]
    fn added_columns_past_their_image_limit_refuse() {
        let mut schema = strict(vec![ColumnDef::required("id", ColumnType::Int64)]);
        let mut added = ColumnDef::nullable("note", ColumnType::String);
        added.default = Some(format!("'{}'", "d".repeat(MAX_ADDED_COLUMN_IMAGE_BYTES)));
        added.added_at_version = 2;
        schema.columns.push(added);
        schema.version = 2;
        let mut stored = StoredCollection::new(1, "grown", "admin");
        stored.collection_type = CollectionType::strict(schema.clone());
        assert_refused(stored);

        // The same default on an original column renders in every stored row
        // already, so it adds nothing to an older row.
        schema.columns[1].added_at_version = 1;
        schema.version = 1;
        let mut original = StoredCollection::new(1, "original", "admin");
        original.collection_type = CollectionType::strict(schema);
        check_stored_collection(&original).expect("an original column");
    }
}
