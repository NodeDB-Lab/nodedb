// SPDX-License-Identifier: Apache-2.0

//! The sans-IO reader contract and a synchronous driver over it.

pub mod driver;
pub mod traits;

pub use driver::{RangeRead, SliceSource, open_all, open_with, read_block_all, read_block_with};
pub use traits::{Opened, RasterOpener, RasterReader};
