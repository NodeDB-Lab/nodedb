// SPDX-License-Identifier: BUSL-1.1

pub mod delta;
pub mod scope;
pub mod table;

pub use delta::StorageDelta;
pub use scope::{DroppedScope, retire_storage_bytes, storage_bytes_snapshot};
pub use table::{STORAGE_BYTES, StorageBytesRow, ensure_storage_bytes_table, row_bytes};
