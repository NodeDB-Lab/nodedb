// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral TYPEGUARD DDL family handlers.

pub mod handlers;
mod injected_expr;
pub mod parse;
pub mod validate;

pub use handlers::{
    alter_typeguard, create_typeguard, drop_typeguard, show_typeguard, show_typeguards,
};
pub use validate::validate_typeguard;
