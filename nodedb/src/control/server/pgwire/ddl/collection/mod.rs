//! Collection DDL: CREATE COLLECTION, DROP COLLECTION, SHOW COLLECTIONS,
//! DESCRIBE, indexes, and ALTER commands.

pub mod alter;
pub mod check_constraint;
pub mod create;
pub mod describe;
pub mod drop;
pub mod helpers;
pub mod index;
pub mod insert;
pub(super) mod insert_parse;
pub mod upsert;
pub mod vector_metadata;

// Re-export all public functions so existing callers via `super::collection::*` continue to work.
pub use alter::{
    alter_collection_alter_column_type, alter_collection_drop_column, alter_collection_enforcement,
    alter_collection_rename_column, alter_table_add_column,
};
pub use create::{create_collection, dispatch_register_if_needed};
pub use describe::{describe_collection, show_collections};
pub use drop::drop_collection;
pub use index::{create_index, drop_index, show_indexes};
pub use insert::insert_document;
pub use upsert::upsert_document;
pub use vector_metadata::{
    handle_set_vector_metadata, handle_show_vector_models, handle_vector_metadata_query,
};

// Re-export validate_document_schema from schema_validation (was re-exported from old collection.rs).
pub use super::schema_validation::validate_document_schema;

// Re-export parse_typed_schema for kv.rs which imports it as `super::collection::parse_typed_schema`.
pub(in crate::control::server::pgwire::ddl) use helpers::parse_typed_schema;
