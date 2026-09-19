// SPDX-License-Identifier: BUSL-1.1

//! Resolve-before-propose handlers for governed vector-primary writes.

mod apply;
mod dispatch;
mod resolve_delete;
mod resolve_update;
mod resolve_upsert;

pub(in crate::data::executor) use apply::{VectorResolvedApplyParams, VectorResolvedIndexSpec};
