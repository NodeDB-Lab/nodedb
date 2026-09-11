// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane resolution of period-lock reference rows. See
//! [`resolve::resolve_period_lock_targets`] for the full contract.

mod gate;
mod lookup;
mod point_update;
mod predicate;
mod resolve;
mod singular;

pub use resolve::{
    resolve_period_lock_targets, resolve_period_lock_targets_for_bodies,
    target_declares_period_lock,
};
