// SPDX-License-Identifier: Apache-2.0

//! Storage of the text history, for the shared logic in [`crate::history`].
//!
//! The history holds two row kinds per index. A live start records the
//! ordinal of the write that indexed a document's live version. An ended
//! version holds a version a later write replaced or removed, with its
//! interval and its analyzed tokens. A backend keys both by
//! `(database_id, tid, collection, field_key, …)`, like every other entry.
//!
//! The read and write traits are split so a read-only snapshot of a store
//! can serve a read at a cut. Write methods take `&mut self`: a backend
//! that joins a caller's transaction holds its handle exclusively.

use nodedb_types::Surrogate;

use crate::history::{EndedKey, EndedRow, EndedVersion, LiveKey, LiveRow, TextCut};
use crate::scope::IndexScope;

/// Read access to the text history.
pub trait FtsHistoryRead {
    /// Error type for history operations.
    type Error: std::fmt::Display;

    /// The recorded start of `surrogate`'s live version in `index`. `None`
    /// when every retained cut reads the version.
    fn live_from(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
    ) -> Result<Option<i64>, Self::Error>;

    /// The documents of `index` whose live version starts after `cut`.
    /// A backend ranges over the starts from [`TextCut::first_after`].
    fn live_started_after(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        cut: TextCut,
    ) -> Result<Vec<Surrogate>, Self::Error>;

    /// The ended versions of `index` that `cut` sees, by [`TextCut::sees`].
    /// A backend ranges over the versions ended from
    /// [`TextCut::first_after`], and decodes only the tokens it returns.
    fn ended_seen_at(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        cut: TextCut,
    ) -> Result<Vec<EndedVersion>, Self::Error>;

    /// Visit every ended version of every index, without its tokens.
    fn scan_ended(&self, visit: &mut dyn FnMut(EndedRow<'_>)) -> Result<(), Self::Error>;

    /// Visit every live start of every index.
    fn scan_live(&self, visit: &mut dyn FnMut(LiveRow<'_>)) -> Result<(), Self::Error>;
}

/// Write access to the text history.
pub trait FtsHistoryWrite: FtsHistoryRead {
    /// Record `surrogate`'s live version in `index` as starting at
    /// `sys_from`, replacing any recorded start.
    fn set_live_from(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
        sys_from: i64,
    ) -> Result<(), Self::Error>;

    /// Drop the recorded start of `surrogate`'s live version in `index`.
    /// Returns the start it held.
    fn clear_live_from(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
    ) -> Result<Option<i64>, Self::Error>;

    /// Store `version` in `index`, replacing a version with the same
    /// surrogate, start, and end.
    fn put_ended(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        version: &EndedVersion,
    ) -> Result<(), Self::Error>;

    /// Remove and return the version of `surrogate` in `index` that ended
    /// at `sys_to`.
    fn take_ended_at(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
        sys_to: i64,
    ) -> Result<Option<EndedVersion>, Self::Error>;

    /// Remove the ended versions `keys` name. An absent key is a no-op.
    fn remove_ended(&mut self, keys: &[EndedKey]) -> Result<(), Self::Error>;

    /// Remove the live starts `keys` name. An absent key is a no-op.
    fn remove_live(&mut self, keys: &[LiveKey]) -> Result<(), Self::Error>;
}
