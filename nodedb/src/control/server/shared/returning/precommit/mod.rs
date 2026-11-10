// SPDX-License-Identifier: BUSL-1.1

//! Finalize a write response after source preparation and before source commit.

mod columns;
mod finalize;
#[cfg(test)]
mod fixtures;
mod limits;
mod types;

pub(crate) use finalize::finalize_returning;
pub(crate) use types::{
    FrozenReturningReply, PrecommitReturning, PreparedReturningRow, PreparedRowIdentity,
    ReturningLimits,
};
