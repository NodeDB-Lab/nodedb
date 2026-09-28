// SPDX-License-Identifier: BUSL-1.1

//! Output-schema derivation: `schema` builds it, `tests` pins it.

mod schema;
#[cfg(test)]
mod tests;

pub use schema::build_output_schema;
