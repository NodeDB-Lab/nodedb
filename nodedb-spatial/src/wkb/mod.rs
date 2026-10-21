// SPDX-License-Identifier: Apache-2.0

//! Well-Known Binary (ISO 13249) and PostGIS EWKB, with Z / M / ZM and
//! EMPTY.
//!
//! Format: `[byte_order: u8] [type: u32] [srid: u32 if EWKB] [body]`. Byte
//! order 1 is little-endian (NDR), 0 is big-endian (XDR). Writers emit
//! little-endian. The reader accepts both.

pub mod error;
pub mod ewkb;
pub mod format;
pub mod read;
pub mod write;

pub use error::WkbError;
pub use ewkb::{geometry_to_ewkb, geometry_to_ewkb_ordered};
pub use format::{EWKB_M_FLAG, EWKB_SRID_FLAG, EWKB_Z_FLAG};
pub use read::{Wkb, geometry_from_wkb, wkb_bbox};
pub use write::{ByteOrder, geometry_to_wkb, geometry_to_wkb_ordered};
