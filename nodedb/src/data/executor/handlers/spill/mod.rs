// SPDX-License-Identifier: BUSL-1.1

pub(in crate::data::executor) mod cleanup;
pub(super) mod core;
pub mod error;
pub(super) mod groupby;
pub(in crate::data::executor) mod run;
pub(in crate::data::executor) mod tiered;

pub use error::SpillError;
