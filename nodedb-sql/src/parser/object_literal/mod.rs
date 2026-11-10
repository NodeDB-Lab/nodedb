// SPDX-License-Identifier: Apache-2.0

//! Parser for `{ key: value }` object literal syntax.
//!
//! Two contracts live here, and the difference between them is the point:
//! [`parse_object_literal`] reads one literal off the FRONT of its input and
//! leaves the rest alone, while [`parse_object_literal_complete`] requires the
//! literal to be the whole input and reports anything left over. A caller whose
//! input is supposed to be the literal must use the strict form, or a clause the
//! author wrote is discarded without anyone being told.

pub mod api;
pub mod scan;
pub mod template;
mod typed;

pub use api::{
    ObjectTemplate, object_literal_end, parse_object_literal, parse_object_literal_array,
    parse_object_literal_array_complete, parse_object_literal_complete,
    parse_object_template_array_complete, parse_object_template_complete,
};
pub use template::Literal;
