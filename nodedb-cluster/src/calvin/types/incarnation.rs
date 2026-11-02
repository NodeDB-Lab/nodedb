// SPDX-License-Identifier: BUSL-1.1

//! The incarnation a Calvin transaction names for each user collection and
//! each array it writes.
//!
//! An array and a collection keep separate catalogs, separate incarnations
//! and separate write gates. One name can be both a collection and an array
//! in one database. The kind keeps the two identities apart.

use serde::{Deserialize, Serialize};

/// Which catalog an incarnation belongs to.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum IncarnationKind {
    /// A user collection of the collection catalog.
    Collection = 0,
    /// An array of the array catalog.
    Array = 1,
}

/// A user collection or array a Calvin transaction names, and the
/// incarnation its coordinator planned against.
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
pub struct CalvinIncarnation {
    /// The collection or array as the plans name it: database-qualified
    /// outside the default database.
    pub collection: String,
    pub incarnation: nodedb_types::Hlc,
    /// The catalog `collection` names an entry of.
    pub kind: IncarnationKind,
}
