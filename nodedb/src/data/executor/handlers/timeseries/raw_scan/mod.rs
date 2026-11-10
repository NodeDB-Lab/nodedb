// SPDX-License-Identifier: BUSL-1.1

//! Raw scan mode: emit rows from memtable + disk partitions.
//!
//! No aggregation — returns individual rows as MessagePack.

pub mod gather;
pub mod partition_scan;
pub mod row_emit;
pub mod row_form;
pub mod scan;

pub(in crate::data::executor) use gather::TsGatherParams;
pub(in crate::data::executor) use row_emit::emit_memtable_rows_at;
pub(in crate::data::executor) use scan::RawScanParams;
