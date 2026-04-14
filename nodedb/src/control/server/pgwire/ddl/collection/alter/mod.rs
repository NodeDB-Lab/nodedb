//! ALTER TABLE / ALTER COLLECTION DDL handlers.
//!
//! Each column operation (`ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`,
//! `ALTER COLUMN ... TYPE ...`) lives in its own file and shares the
//! catalog mutation pattern: fetch `StoredCollection`, mutate the
//! `StrictSchema`, bump version, propose through Raft, refresh the
//! Data Plane cache.

pub mod add_column;
pub mod alter_type;
pub mod drop_column;
pub mod enforcement;
pub mod materialized_sum;
pub mod rename_column;

pub use add_column::alter_table_add_column;
pub use alter_type::alter_collection_alter_column_type;
pub use drop_column::alter_collection_drop_column;
pub use enforcement::alter_collection_enforcement;
pub use materialized_sum::add_materialized_sum;
pub use rename_column::alter_collection_rename_column;
