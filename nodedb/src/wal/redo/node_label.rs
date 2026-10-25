// SPDX-License-Identifier: BUSL-1.1

//! Payload of a graph node-label record: the bytes of an autocommit
//! [`RecordType::GraphNodeLabelSet`](nodedb_wal::record::RecordType::GraphNodeLabelSet)
//! or `GraphNodeLabelRemove` record, and of the same sub-records inside a
//! `RedoRecord`.
//!
//! Set and remove differ by record type. A record that carries its
//! `system_from` writes its label version at that ordinal on every replica.
//! A record without one takes the applying core's clock.

use serde::{Deserialize, Serialize};

/// One node-label op.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct NodeLabelRedo {
    pub node_id: String,
    /// The labels the op sets or removes. An exact set names the node's
    /// whole label set.
    pub labels: Vec<String>,
    /// The system-time ordinal of the op's label version. `None` on an
    /// autocommit record, which takes its live ordinal.
    #[serde(default)]
    #[msgpack(default)]
    pub system_from: Option<i64>,
    /// On a set: `labels` replaces the node's label set, and the version
    /// holds exactly `labels`. A restored label version is exact.
    #[serde(default)]
    #[msgpack(default)]
    pub exact: bool,
}

impl NodeLabelRedo {
    /// An op on `node_id` over `labels` that takes its live ordinal.
    pub fn live(node_id: &str, labels: &[String]) -> Self {
        Self {
            node_id: node_id.to_string(),
            labels: labels.to_vec(),
            system_from: None,
            exact: false,
        }
    }

    /// Encode the record payload.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal graph node label: {e}"),
        })
    }

    /// Decode a record payload.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal graph node label: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_round_trips_with_its_ordinal() {
        let record = NodeLabelRedo {
            node_id: "alice".into(),
            labels: vec!["Person".into()],
            system_from: Some(42),
            exact: true,
        };
        let decoded = NodeLabelRedo::decode(&record.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, record);
        let live = NodeLabelRedo::live("bob", &["City".to_string()]);
        let decoded = NodeLabelRedo::decode(&live.encode().expect("encode")).expect("decode");
        assert_eq!((decoded.system_from, decoded.exact), (None, false));
    }
}
