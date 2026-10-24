// SPDX-License-Identifier: Apache-2.0

//! The payload of `MetaOp::RestoreRedo`: one batch of a RESTORE's rows and
//! edge versions, installed as a Calvin transaction.
//!
//! A RESTORE re-issues what a backup captured in the Calvin sequence, so its
//! writes order against every other transaction, a TRUNCATE included. Each
//! participant installs the batch at its transaction's turn. A restored row
//! installs as the backup holds it. A restored edge version keeps its
//! historical `system_from` and is applied at the transaction's ordinal. A
//! restored node label version keeps its system time too.

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
    /// Every row `rows_redo` writes.
    pub rows: Vec<RestoredRow>,
    /// The restored edge versions, in apply order.
    pub edges: Vec<RestoredEdgeVersion>,
    /// The restored node labels of nodes homed on `vshard`.
    #[serde(default)]
    #[msgpack(default)]
    pub node_labels: Vec<RestoredNodeLabels>,
    /// Every collection the batch writes, as the Data Plane stores it.
    pub collections: Vec<String>,
    /// Every identity the rows and edge endpoints are stored under.
    pub identities: Vec<RestoredIdentity>,
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
            collections: vec!["knows".into(), "people".into()],
            identities: vec![RestoredIdentity {
                collection: "people".into(),
                pk_bytes: b"alice".to_vec(),
                surrogate: 4,
            }],
        };
        let bytes = zerompk::to_msgpack_vec(&batch).expect("encode");
        let decoded: RestoredRedo = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, batch);
    }
}
