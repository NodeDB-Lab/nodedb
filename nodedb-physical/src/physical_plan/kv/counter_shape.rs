// SPDX-License-Identifier: Apache-2.0

//! The row a KV counter atomic creates when its key is absent.

/// What `INCR` / `INCRBYFLOAT` stores for a key that does not exist yet.
///
/// The Data Plane does not know a collection's declared columns. The Control
/// Plane decides the shape from the catalog when it builds the op, and the
/// shape travels with the op to every path that computes the value: the live
/// handler, transaction staging, the resolve path, and WAL replay.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum KvCounterShape {
    /// A raw collection (a single `value` column, or none declared), or a
    /// RESP command, where every value is a byte string. The new value is
    /// stored as its decimal text.
    #[default]
    Raw,
    /// A typed collection. The new row is `template` with `column` set to the
    /// new value: the row `INSERT (key, column) VALUES (key, delta)` stores,
    /// with DEFAULTs materialized.
    Typed {
        /// The declared numeric column the counter moves. `None` when the
        /// collection declares no column of the counter's type.
        column: Option<String>,
        /// A msgpack map body: the columns the insert stores other than
        /// `column`.
        template: Vec<u8>,
    },
}
