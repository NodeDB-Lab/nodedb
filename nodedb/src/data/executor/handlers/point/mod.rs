//! Point operation handlers: PointGet, PointPut, PointDelete, PointUpdate,
//! plus the shared `apply_point_put` transaction helper.
//!
//! Each handler is a method on `CoreLoop`; files here contribute `impl CoreLoop`
//! blocks that share the same type. Dispatch sees them via the normal method
//! lookup — no re-export needed.

pub mod apply_put;
pub mod delete;
pub mod get;
pub mod put;
pub mod update;
