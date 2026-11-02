// SPDX-License-Identifier: Apache-2.0

//! The payload of `MetaOp::RestoreRedo`: one batch of a RESTORE's rows, KV
//! keys and edge versions, installed as a Calvin transaction.
//!
//! A RESTORE re-issues what a backup captured in the Calvin sequence, so its
//! writes order against every other transaction, a TRUNCATE included. Each
//! participant installs the batch at its transaction's turn. A restored row
//! installs as the backup holds it. A restored edge version keeps its
//! historical `system_from` and is applied at the transaction's ordinal. A
//! restored node label version keeps its system time too. A restored KV key
//! installs every version at its backed-up ordinal. A restored array cell
//! installs every version at its backed-up system time. A restored columnar
//! key and a restored vector row install every version at its backed-up
//! interval. Restored timeseries rows append at the batch's turn.

use nodedb_array::types::ArrayId;

use super::meta_restore_keys::{RestoredColumnarKey, RestoredVectorRow};

/// One restored edge version.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RestoredEdgeVersion {
    /// The collection, as the Data Plane stores it.
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub src_surrogate: u32,
    pub dst_surrogate: u32,
    /// The system time the backup holds the version at.
    pub system_from: i64,
    /// The version's properties, `None` for a tombstone.
    pub properties: Option<Vec<u8>>,
    /// The version's valid time `[from, until)` in ms since epoch, as the
    /// backup holds it. `None` for a tombstone.
    pub valid_time: Option<(i64, i64)>,
}

/// One restored document row: the keys its writers lock.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RestoredRow {
    /// The collection, as the Data Plane stores it.
    pub collection: String,
    /// The row's key, as its surrogate binds it.
    pub document_id: String,
    pub surrogate: u32,
}

/// One restored KV key: the key its writers lock.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RestoredKvKey {
    /// The collection, as the Data Plane stores it.
    pub collection: String,
    pub key: Vec<u8>,
}

/// One restored node's label history and current label set.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RestoredNodeLabels {
    pub node_id: String,
    /// Every label version the backup holds, as `(system_from, labels)`, in
    /// ascending system time. Each keeps its system time.
    pub versions: Vec<(i64, Vec<String>)>,
    /// The label set the node carries in the backup.
    pub current: Vec<String>,
}

/// The restored cell versions of one array on the batch's vShard.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RestoredArrayCells {
    /// The destination array.
    pub array_id: ArrayId,
    /// The encoded array restore sub-record of the cells: every backed-up
    /// version of each coordinate, each at its own system time. The resolve
    /// adds the versions that set each coordinate's backed-up state again.
    pub redo: Vec<u8>,
}

/// One `(collection, key) → surrogate` identity every participant binds
/// before the batch installs.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RestoredIdentity {
    /// The bare catalog name.
    pub collection: String,
    pub pk_bytes: Vec<u8>,
    pub surrogate: u32,
}

/// One batch of a RESTORE on one vShard.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RestoredRedo {
    /// The vShard every row and edge version of the batch writes.
    pub vshard: u32,
    /// The zerompk-encoded redo record of the restored rows: their
    /// sub-records and the change events their install publishes. Empty
    /// when the batch restores edges only.
    pub rows_redo: Vec<u8>,
    /// Every document row `rows_redo` writes.
    pub rows: Vec<RestoredRow>,
    /// Every KV key `rows_redo` writes. Each carries every version of the
    /// key at its backed-up ordinal.
    pub kv_keys: Vec<RestoredKvKey>,
    /// The restored edge versions, in apply order.
    pub edges: Vec<RestoredEdgeVersion>,
    /// The restored node labels of nodes homed on `vshard`.
    pub node_labels: Vec<RestoredNodeLabels>,
    /// The restored cells of arrays whose cells route to `vshard`.
    pub arrays: Vec<RestoredArrayCells>,
    /// Every columnar key `rows_redo` writes. Each carries every version of
    /// the key at its backed-up interval.
    pub columnar_keys: Vec<RestoredColumnarKey>,
    /// Every vector row `rows_redo` writes. Each carries every node of the
    /// row at its backed-up span.
    pub vector_rows: Vec<RestoredVectorRow>,
    /// Every timeseries collection `rows_redo` writes: its series catalog,
    /// its ended partitions and its restored rows. A timeseries row has no
    /// key, so its writers lock the collection.
    pub timeseries: Vec<String>,
    /// Every user collection the batch writes, as the Data Plane stores it.
    /// A restored array is no collection: `arrays` names it by its
    /// `ArrayId`, so a same-name collection never shares its identity.
    pub collections: Vec<String>,
    /// Every identity the rows and edge endpoints are stored under.
    pub identities: Vec<RestoredIdentity>,
    /// The ordinal the RESTORE takes effect at: a closed cut taken before
    /// its first batch. A destination row, KV key or array cell with a
    /// version above it was written after the RESTORE began, and the
    /// resolve leaves that version current. Every write at or below it
    /// applied before the first batch resolved, and the resolve sets the
    /// backup's state again above it. `i64::MAX` for a re-issue that sets
    /// the backup's state over every destination version.
    pub restore_cut: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_restored_batch_round_trips_through_msgpack() {
        let batch = RestoredRedo {
            vshard: 7,
            rows_redo: vec![1, 2, 3],
            rows: vec![RestoredRow {
                collection: "people".into(),
                document_id: "alice".into(),
                surrogate: 4,
            }],
            kv_keys: vec![RestoredKvKey {
                collection: "sessions".into(),
                key: b"s1".to_vec(),
            }],
            edges: vec![RestoredEdgeVersion {
                collection: "knows".into(),
                src_id: "alice".into(),
                label: "KNOWS".into(),
                dst_id: "bob".into(),
                src_surrogate: 4,
                dst_surrogate: 5,
                system_from: 100,
                properties: None,
                valid_time: None,
            }],
            node_labels: vec![RestoredNodeLabels {
                node_id: "alice".into(),
                versions: vec![(90, vec!["Person".into()])],
                current: vec!["Person".into()],
            }],
            arrays: vec![RestoredArrayCells {
                array_id: ArrayId::new(nodedb_types::TenantId::new(1), "grid"),
                redo: vec![4, 5],
            }],
            columnar_keys: vec![RestoredColumnarKey {
                collection: "metrics".into(),
                surrogate: 6,
            }],
            vector_rows: vec![RestoredVectorRow {
                collection: "docs".into(),
                surrogate: 8,
            }],
            timeseries: vec!["cpu".into()],
            collections: vec!["knows".into(), "people".into()],
            identities: vec![RestoredIdentity {
                collection: "people".into(),
                pk_bytes: b"alice".to_vec(),
                surrogate: 4,
            }],
            restore_cut: 9,
        };
        let bytes = zerompk::to_msgpack_vec(&batch).expect("encode");
        let decoded: RestoredRedo = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, batch);
    }
}
