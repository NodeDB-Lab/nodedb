// SPDX-License-Identifier: BUSL-1.1

//! Output schemas for read, aggregate, constant, and write plans.

mod aggregate;
mod constant;
#[cfg(test)]
mod fixtures;
mod schema;

pub use schema::build_output_schema;
