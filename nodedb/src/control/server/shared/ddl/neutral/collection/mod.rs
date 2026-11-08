// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral collection DDL family: DESCRIBE COLLECTION / SHOW
//! COLLECTIONS / SHOW INDEXES / UNDROP COLLECTION / CREATE COLLECTION /
//! CREATE TABLE / DROP COLLECTION / CREATE INDEX / DROP INDEX, plus the
//! collection purge helpers.

pub mod alter;
pub mod copy_from;
pub mod copy_to;
pub mod create;
pub mod describe;
mod describe_array;
pub mod dml;
pub mod drop;
pub mod enforcement;
mod generated_join;
pub(crate) mod helpers;
pub mod index;
pub(super) mod index_fanout;
pub mod purge;
pub mod register;
pub(crate) mod server_path;
pub mod show_indexes;
pub mod undrop;
pub mod vector_metadata;

pub use alter::dispatch_alter_collection;
pub use copy_from::{CopyFromOptions, copy_from_file};
pub use copy_to::{CopyToOptions, copy_to_file};
pub use create::{CreateCollectionRequest, create_collection, create_table};
pub use describe::{describe_collection, show_collections};
pub use dml::{insert_document, upsert_document};
pub use drop::{DropCollectionRequest, drop_collection};
pub use index::{CreateIndexRequest, DropIndexRequest, create_index, drop_index};
pub use register::{
    dispatch_register_for_sum_sources, dispatch_register_from_stored, register_proposed_collection,
};
pub use show_indexes::show_indexes;
pub use undrop::undrop_collection;
pub use vector_metadata::{
    handle_set_vector_metadata, handle_show_vector_models, handle_vector_metadata_query,
};
