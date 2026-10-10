// SPDX-License-Identifier: Apache-2.0

//! The request and answer rows of `MetaOp::HomeVersions`.

/// One vShard home whose current version a core reports.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct HomeVersionProbe {
    /// The vShard the home read observed.
    pub vshard: u32,
    /// The database-qualified collection the read scoped, or `None` when the
    /// read walked every collection.
    pub collection: Option<String>,
}

/// A node's answer for one [`HomeVersionProbe`].
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
pub struct HomeVersion {
    pub probe: HomeVersionProbe,
    pub answer: HomeAnswer,
}

/// What a node answers for one home.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum HomeAnswer {
    /// The collection's write floor on the probe's vShard, or that vShard's
    /// latest version for a probe with no collection.
    Version(nodedb_types::WriteVersion),
    /// The node does not hold the leader lease of the probe's group, so it
    /// cannot answer. `leader_node` is the leader its routing table names at
    /// `leader_term`, `0` when it names none.
    NotLeader { leader_node: u64, leader_term: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn answers_round_trip_through_msgpack() {
        let answers = vec![
            HomeVersion {
                probe: HomeVersionProbe {
                    vshard: 7,
                    collection: Some("db1.edges".into()),
                },
                answer: HomeAnswer::Version(nodedb_types::WriteVersion::logged(3, 42)),
            },
            HomeVersion {
                probe: HomeVersionProbe {
                    vshard: 9,
                    collection: None,
                },
                answer: HomeAnswer::NotLeader {
                    leader_node: 2,
                    leader_term: 7,
                },
            },
        ];
        let bytes = zerompk::to_msgpack_vec(&answers).expect("encode");
        let decoded: Vec<HomeVersion> = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, answers);
    }
}
