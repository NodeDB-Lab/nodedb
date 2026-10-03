// SPDX-License-Identifier: BUSL-1.1

//! Wire forms of the payload enums a Data-Plane [`ErrorCode`] carries.
//!
//! Each local type and its wire mirror live in different crates, so every
//! mapping is a function, not a `From` impl. Every match is exhaustive, so a
//! new variant fails to compile here until it is mirrored on the wire.
//!
//! [`ErrorCode`]: crate::bridge::envelope::ErrorCode

use nodedb_cluster::rpc_codec::{
    DataPlaneCounterFault, DataPlaneSyncHold, DataPlaneTextColumnFault,
};
use nodedb_types::text_search::TextColumnFault;

use crate::bridge::envelope::{CounterFault, SyncHold};

/// The wire form of a sync hold.
pub(super) fn sync_hold_to_wire(hold: SyncHold) -> DataPlaneSyncHold {
    match hold {
        SyncHold::Duplicate => DataPlaneSyncHold::Duplicate,
        SyncHold::Fenced => DataPlaneSyncHold::Fenced,
        SyncHold::Gap { expected } => DataPlaneSyncHold::Gap { expected },
    }
}

/// The sync hold a wire form names.
pub(super) fn sync_hold_from_wire(hold: DataPlaneSyncHold) -> SyncHold {
    match hold {
        DataPlaneSyncHold::Duplicate => SyncHold::Duplicate,
        DataPlaneSyncHold::Fenced => SyncHold::Fenced,
        DataPlaneSyncHold::Gap { expected } => SyncHold::Gap { expected },
    }
}

/// The wire form of a counter fault.
pub(super) fn counter_fault_to_wire(fault: CounterFault) -> DataPlaneCounterFault {
    match fault {
        CounterFault::NotAnInteger => DataPlaneCounterFault::NotAnInteger,
        CounterFault::NotAFloat => DataPlaneCounterFault::NotAFloat,
        CounterFault::IntegerOverflow => DataPlaneCounterFault::IntegerOverflow,
        CounterFault::NonFinite => DataPlaneCounterFault::NonFinite,
    }
}

/// The counter fault a wire form carries.
pub(super) fn counter_fault_from_wire(fault: DataPlaneCounterFault) -> CounterFault {
    match fault {
        DataPlaneCounterFault::NotAnInteger => CounterFault::NotAnInteger,
        DataPlaneCounterFault::NotAFloat => CounterFault::NotAFloat,
        DataPlaneCounterFault::IntegerOverflow => CounterFault::IntegerOverflow,
        DataPlaneCounterFault::NonFinite => CounterFault::NonFinite,
    }
}

/// The wire form of a text-column fault.
pub(super) fn text_column_fault_to_wire(fault: TextColumnFault) -> DataPlaneTextColumnFault {
    match fault {
        TextColumnFault::Undeclared => DataPlaneTextColumnFault::Undeclared,
        TextColumnFault::NotText { data_type } => DataPlaneTextColumnFault::NotText { data_type },
        TextColumnFault::NotAColumn => DataPlaneTextColumnFault::NotAColumn,
        TextColumnFault::NotIndexed => DataPlaneTextColumnFault::NotIndexed,
    }
}

/// The text-column fault a wire form carries.
pub(super) fn text_column_fault_from_wire(fault: DataPlaneTextColumnFault) -> TextColumnFault {
    match fault {
        DataPlaneTextColumnFault::Undeclared => TextColumnFault::Undeclared,
        DataPlaneTextColumnFault::NotText { data_type } => TextColumnFault::NotText { data_type },
        DataPlaneTextColumnFault::NotAColumn => TextColumnFault::NotAColumn,
        DataPlaneTextColumnFault::NotIndexed => TextColumnFault::NotIndexed,
    }
}
