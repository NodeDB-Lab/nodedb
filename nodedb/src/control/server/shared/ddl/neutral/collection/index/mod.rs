// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral index DDL: CREATE INDEX, DROP INDEX.

pub mod build;
pub mod commit;
pub mod create;
pub mod drop;
pub mod kv_index;
pub mod teardown;

pub use create::{CreateIndexRequest, create_index};
pub use drop::{DropIndexRequest, drop_index};
