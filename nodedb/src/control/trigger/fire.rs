// SPDX-License-Identifier: BUSL-1.1

//! Re-exports of the trigger fire functions from their split modules, so a
//! call site names them as `control::trigger::fire::fire_after_insert`.

pub use super::fire_after::{
    FireAfterDeleteParams, FireAfterInsertParams, FireAfterUpdateParams, ShippedBlock,
    fire_after_delete, fire_after_insert, fire_after_update, fire_sql,
};
pub use super::fire_before::{fire_before_delete, fire_before_insert, fire_before_update};
pub use super::fire_instead::{
    InsteadOfResult, fire_instead_of_delete, fire_instead_of_insert, fire_instead_of_update,
};
