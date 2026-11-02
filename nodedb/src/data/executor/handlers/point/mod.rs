// SPDX-License-Identifier: BUSL-1.1

//! Point operation handlers: PointGet, PointPut, PointDelete, PointUpdate,
//! plus the shared `apply_point_put` transaction helper.
//!
//! Each handler is a method on `CoreLoop`; files here contribute `impl CoreLoop`
//! blocks that share the same type. Dispatch sees them via the normal method
//! lookup — no re-export needed.

pub mod apply_delete;
pub mod apply_put;
pub mod delete;
pub mod delete_version;
pub mod get;
pub mod history_install;
pub mod insert;
pub mod overlay_lookup;
pub mod put;
pub mod update;
pub mod update_reindex;
pub mod update_reindex_secondary;
pub mod update_reindex_sparse;
pub mod update_reindex_text;
pub mod update_reindex_vector;
