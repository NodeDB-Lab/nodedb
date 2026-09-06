// SPDX-License-Identifier: BUSL-1.1

//! Environment variable overrides for `ServerConfig`: a table-driven
//! startup gate. See `table::apply_env_overrides`.

mod memory_size;
mod parse;
mod rows;
mod seed_nodes;
mod table;

pub use memory_size::parse_memory_size;
pub use seed_nodes::parse_seed_nodes;
pub use table::apply_env_overrides;
