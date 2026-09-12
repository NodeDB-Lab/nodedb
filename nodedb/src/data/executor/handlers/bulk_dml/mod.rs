// SPDX-License-Identifier: BUSL-1.1

//! Bulk DML handlers: BulkUpdate, BulkDelete.
//!
//! These operate on document sets matching ScanFilter predicates,
//! unlike PointUpdate/PointDelete which require `WHERE id = 'x'`.

pub mod admission;
pub mod delete;
pub mod delete_cascade;
pub mod scan;
pub mod update;
pub mod update_persist;
pub mod update_project;

pub(in crate::data::executor) use admission::BulkAdmission;
pub(in crate::data::executor) use delete::{BulkDeleteParams, OllpPrediction};
pub(in crate::data::executor) use update::BulkUpdateParams;
