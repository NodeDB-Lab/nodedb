// SPDX-License-Identifier: BUSL-1.1

//! Columnar base scan: parameters, the entry point, and its stages.

mod block_skip;
mod execute;
mod order;
mod params;

pub(in crate::data::executor) use params::ColumnarScanParams;
