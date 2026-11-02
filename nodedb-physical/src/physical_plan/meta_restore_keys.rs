// SPDX-License-Identifier: Apache-2.0

//! The keys a RESTORE batch's columnar keys and vector rows lock.

/// One restored columnar primary key: the row its writers lock.
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
pub struct RestoredColumnarKey {
    /// The collection, as the Data Plane stores it.
    pub collection: String,
    /// The key's identity in the destination database.
    pub surrogate: u32,
}

/// One restored row of a vector index: the row its writers lock.
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
pub struct RestoredVectorRow {
    /// The collection, as the Data Plane stores it.
    pub collection: String,
    /// The row's identity in the destination database.
    pub surrogate: u32,
}
