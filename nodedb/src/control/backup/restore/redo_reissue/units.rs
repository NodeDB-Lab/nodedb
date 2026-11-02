// SPDX-License-Identifier: BUSL-1.1

//! The units a restored row, KV key, columnar key, vector row, timeseries
//! chunk or edge re-issues as.

use nodedb_physical::physical_plan::{
    RestoredColumnarKey, RestoredEdgeVersion, RestoredKvKey, RestoredRow, RestoredVectorRow,
};

use crate::control::surrogate::CarriedIdentity;
use crate::types::{DatabaseId, HomedRecord, RecordHomes, VShardId};
use crate::wal::{RedoRowChange, RedoSubRecord};

/// One restored row or KV key: its sub-records in apply order, the identities every
/// replica binds before it installs them, the row change its install
/// publishes, and the row its writers lock. A unit never splits across two
/// transactions.
#[derive(Clone)]
pub(super) struct RowUnit {
    pub ops: Vec<RedoSubRecord>,
    pub identities: Vec<CarriedIdentity>,
    /// The change events the unit's install publishes: a restored row that
    /// ends live is an insert.
    pub changes: Vec<RedoRowChange>,
    /// The document row the unit writes, as its writers lock it.
    pub rows: Vec<RestoredRow>,
    /// The KV key the unit writes, as its writers lock it.
    pub kv_keys: Vec<RestoredKvKey>,
    /// The columnar key the unit writes, as its writers lock it.
    pub columnar_keys: Vec<RestoredColumnarKey>,
    /// The vector row the unit writes, as its writers lock it.
    pub vector_rows: Vec<RestoredVectorRow>,
    /// The timeseries collection the unit appends to, as its writers lock
    /// it: whole.
    pub timeseries: Vec<String>,
}

impl RowUnit {
    /// The unit's encoded size, for sizing the batches it goes into.
    pub(super) fn byte_len(&self) -> usize {
        self.ops.iter().map(|op| op.payload.len()).sum::<usize>() + identities_len(&self.identities)
    }
}

/// Units of one collection that all write one vShard.
pub(super) struct CollectionUnits {
    /// The destination database.
    pub database_id: DatabaseId,
    /// Bare catalog name of the collection.
    pub collection: String,
    /// The vShard every unit writes.
    pub vshard_id: VShardId,
    pub units: Vec<RowUnit>,
}

impl CollectionUnits {
    /// Rows of `collection`, written to its home vShard.
    pub(super) fn rows(database_id: DatabaseId, collection: String, units: Vec<RowUnit>) -> Self {
        let vshard_id = RecordHomes::of(HomedRecord::Row(nodedb_types::CollectionKey::from_bare(
            database_id,
            &collection,
        )))
        .owner();
        Self {
            database_id,
            collection,
            vshard_id,
            units,
        }
    }
}

/// Every restored version of one edge, in system-time order, the identities
/// of its endpoints, and the homes it re-issues to. A unit never splits
/// across two transactions, so one resolve sees every version of the edge
/// and no earlier batch of the RESTORE wrote it.
#[derive(Clone)]
pub(super) struct EdgeUnit {
    pub versions: Vec<RestoredEdgeVersion>,
    pub identities: Vec<CarriedIdentity>,
    pub homes: RecordHomes,
}

impl EdgeUnit {
    /// The unit's encoded size, for sizing the batches it goes into.
    pub(super) fn byte_len(&self) -> usize {
        self.versions
            .iter()
            .map(|version| {
                version.collection.len()
                    + version.src_id.len()
                    + version.label.len()
                    + version.dst_id.len()
                    + version.properties.as_ref().map_or(0, Vec::len)
            })
            .sum::<usize>()
            + identities_len(&self.identities)
    }

    /// Whether `version` is a version of this unit's edge.
    pub(super) fn holds_edge_of(&self, version: &RestoredEdgeVersion) -> bool {
        self.versions.first().is_some_and(|first| {
            first.collection == version.collection
                && first.src_id == version.src_id
                && first.label == version.label
                && first.dst_id == version.dst_id
        })
    }
}

/// The restored edges of one collection, one unit per edge, in key order.
pub(super) struct CollectionEdges {
    /// The destination database.
    pub database_id: DatabaseId,
    /// Bare catalog name of the collection.
    pub collection: String,
    pub units: Vec<EdgeUnit>,
}

fn identities_len(identities: &[CarriedIdentity]) -> usize {
    identities
        .iter()
        .map(|identity| identity.collection.len() + identity.pk_bytes.len())
        .sum()
}
