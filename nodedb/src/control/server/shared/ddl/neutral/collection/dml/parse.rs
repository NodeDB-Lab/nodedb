// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral INSERT/UPSERT parsing, SQL generation, response shaping,
//! and dispatch helpers.

mod dispatch;
mod encoding;
mod response;
mod statement;
mod types;

pub(in crate::control::server::shared::ddl::neutral::collection) use dispatch::{
    authorize_write_target, dispatch_plan, plan_and_dispatch,
};
pub(in crate::control::server::shared::ddl::neutral::collection) use encoding::fields_to_insert_sql;
pub(super) use encoding::fields_to_upsert_sql;
pub(super) use response::returning_response;
pub(super) use statement::parse_write_statement;
pub(super) use types::extract_vector_fields;
