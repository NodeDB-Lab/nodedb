// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL dispatch shared by native + http entrypoints.
pub mod catalog;
pub mod dispatch;
pub mod engine_apply;
pub mod index_registry;
pub mod neutral;
pub mod owner;
pub mod result;
pub mod schema_validation;
pub mod sql_parse;
pub mod sql_split;
pub mod sqlstate;
pub mod static_sqlstate;
pub mod sync_dispatch;
pub mod user_dispatch;

pub use self::dispatch::{abort_block_on_error, dispatch};
pub use self::result::{DdlError, DdlResult};
