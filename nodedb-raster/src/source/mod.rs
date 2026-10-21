// SPDX-License-Identifier: Apache-2.0

//! Byte ranges of source objects and the bounded cache an open reads from.

pub mod cache;
pub mod range;

pub use cache::{Lookup, RangeCache};
pub use range::{ByteRange, COALESCE_GAP, ObjectId, coalesce};
