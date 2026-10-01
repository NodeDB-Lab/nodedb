// SPDX-License-Identifier: Apache-2.0

//! Plan-time catalog expression folding.

mod filter;
mod leaf;
mod walk;

pub use walk::fold_catalog_exprs_in_plan;
