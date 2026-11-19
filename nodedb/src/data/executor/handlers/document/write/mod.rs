// SPDX-License-Identifier: BUSL-1.1

//! Document write handlers: PointPut, BatchInsert, Upsert, Register.
//! Secondary-index lookup / fetch handlers live in `index_fetch`; index
//! backfill / drop handlers live in `index_maintenance`.

pub mod batch_insert;
pub mod batch_insert_judge;
pub mod config_wire;
pub mod register;

pub(in crate::data::executor) use batch_insert::DocumentBatchInsertParams;
pub(in crate::data::executor) use config_wire::WireConfigFields;
pub(in crate::data::executor) use register::RegisterDocumentCollectionParams;
