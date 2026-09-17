// SPDX-License-Identifier: Apache-2.0

//! Low-level MessagePack binary reader: tag parsing, value skipping, and typed reads.
//!
//! All functions operate on `&[u8]` with explicit offsets. Zero allocation,
//! zero copy. Returns `None` on truncated/invalid data — never panics.

pub mod scalar;
pub mod skip;
pub mod tags;
pub mod value;

pub(crate) use scalar::str_bounds;
pub use scalar::{
    array_header, map_header, read_bin_advance, read_bool, read_f64, read_i64, read_null, read_str,
    read_str_advance, read_u32_advance,
};
pub use skip::skip_value;
pub use value::read_value;
