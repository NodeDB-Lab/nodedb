// SPDX-License-Identifier: Apache-2.0

//! The reply a committed Calvin slice answers with, in the form a redo
//! entry carries to every replica.
//!
//! The leader's stage decides the reply. Every replica renders it after its
//! own install of the slice's redo, so every replica answers the same bytes.

use nodedb_types::Surrogate;

use super::ReturningSpec;

/// What a Calvin slice answers once its redo installs.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CalvinReplySpec {
    /// An affected count, decided when the plan staged.
    Count(Vec<u8>),
    /// `RETURNING` rows, decided when the plan staged.
    Rows(Vec<u8>),
    /// `RETURNING` rows read from base after the install.
    PostImages(CalvinPostImagesSpec),
    /// `RETURNING` rows of a resolved timeseries ingest, rendered from what
    /// its install stored.
    InstalledTimeseries(CalvinInstalledTimeseriesSpec),
}

/// The engine that stores a row a `RETURNING` reply reads after the install.
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
pub enum CalvinRowEngine {
    Document,
    Crdt,
    Kv,
    Vector,
    Columnar,
    Timeseries,
}

/// One row a `RETURNING` plan wrote: its client identity and its surrogate.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CalvinReplyRow {
    pub identity: String,
    pub surrogate: Surrogate,
}

/// The rows of a `RETURNING` plan read from base after the install.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CalvinPostImagesSpec {
    pub spec: ReturningSpec,
    pub rls_filters: Vec<u8>,
    pub collection: String,
    pub engine: CalvinRowEngine,
    /// Each row the plan wrote, in the order the plan wrote them.
    pub rows: Vec<CalvinReplyRow>,
}

/// The `RETURNING` projection of a resolved timeseries ingest.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CalvinInstalledTimeseriesSpec {
    pub spec: ReturningSpec,
    pub rls_filters: Vec<u8>,
    pub collection: String,
    /// The ingest's position among the slice's timeseries ingests. Its
    /// install is the install at this position.
    pub ordinal: u64,
}

/// What a stamped redo install of a Calvin slice needs beyond the redo
/// record.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CalvinInstall {
    pub epoch: u64,
    pub position: u32,
    /// The epoch's deterministic instant. The install stamps graph versions
    /// at the transaction's ordinal and renders the reply at this instant.
    pub epoch_system_ms: i64,
    /// The reply the install answers with.
    pub reply: CalvinReplySpec,
}

/// The answer of `MetaOp::CalvinResolve`: the slice's resolved redo record
/// and the reply its stage decided.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CalvinResolved {
    /// The zerompk-encoded redo record.
    pub redo: Vec<u8>,
    pub reply: CalvinReplySpec,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::ReturningColumns;

    #[test]
    fn a_resolved_answer_round_trips() {
        let resolved = CalvinResolved {
            redo: vec![1, 2, 3],
            reply: CalvinReplySpec::PostImages(CalvinPostImagesSpec {
                spec: ReturningSpec {
                    columns: ReturningColumns::Star,
                },
                rls_filters: vec![9],
                collection: "orders".into(),
                engine: CalvinRowEngine::Kv,
                rows: vec![CalvinReplyRow {
                    identity: "k1".into(),
                    surrogate: Surrogate::new(4),
                }],
            }),
        };
        let bytes = zerompk::to_msgpack_vec(&resolved).expect("encode");
        let decoded: CalvinResolved = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, resolved);
    }
}
