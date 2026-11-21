// SPDX-License-Identifier: BUSL-1.1

mod array;
mod class;
mod document;
mod engines;
mod graph;
mod keys;
mod kv;
mod meta;
mod plan;

pub(in crate::data::executor) use class::Footprint;
pub(in crate::data::executor) use plan::footprint;
