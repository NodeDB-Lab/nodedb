// SPDX-License-Identifier: BUSL-1.1

//! The text history stores of the redb backend.
//!
//! The history logic in `nodedb_fts::history` reads and writes through
//! these. A text write ends and starts versions inside the document write's
//! own redb transaction, so the write store borrows that transaction and the
//! write's stored-bytes delta. A read at a cut reads through a read
//! transaction.

use nodedb_fts::IndexScope;
use nodedb_fts::backend::{FtsHistoryRead, FtsHistoryWrite};
use nodedb_fts::history::{EndedKey, EndedRow, EndedVersion, LiveKey, LiveRow, TextCut};
use nodedb_types::Surrogate;
use redb::{ReadTransaction, WriteTransaction};

use super::history_rows;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::tables::{TEXT_ENDED, TEXT_LIVE_BY_FROM, TEXT_LIVE_FROM};
use crate::engine::storage_bytes::StorageDelta;

/// The text history as one read transaction sees it.
pub(crate) struct RedbHistoryRead<'a> {
    txn: &'a ReadTransaction,
}

impl<'a> RedbHistoryRead<'a> {
    pub(crate) fn new(txn: &'a ReadTransaction) -> Self {
        Self { txn }
    }
}

/// The text history inside one write transaction. `delta` records the
/// stored bytes every write changes.
pub(crate) struct RedbHistoryTxn<'a> {
    txn: &'a WriteTransaction,
    delta: &'a mut StorageDelta,
}

impl<'a> RedbHistoryTxn<'a> {
    pub(crate) fn new(txn: &'a WriteTransaction, delta: &'a mut StorageDelta) -> Self {
        Self { txn, delta }
    }
}

impl FtsHistoryRead for RedbHistoryRead<'_> {
    type Error = crate::Error;

    fn live_from(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
    ) -> crate::Result<Option<i64>> {
        let table = self
            .txn
            .open_table(TEXT_LIVE_FROM)
            .map_err(|e| redb_err("open text live_from", e))?;
        history_rows::live_from(&table, (database_id, tid), index, surrogate)
    }

    fn live_started_after(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        cut: TextCut,
    ) -> crate::Result<Vec<Surrogate>> {
        let table = self
            .txn
            .open_table(TEXT_LIVE_BY_FROM)
            .map_err(|e| redb_err("open text live_by_from", e))?;
        history_rows::live_started_after(&table, (database_id, tid), index, cut)
    }

    fn ended_seen_at(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        cut: TextCut,
    ) -> crate::Result<Vec<EndedVersion>> {
        let table = self
            .txn
            .open_table(TEXT_ENDED)
            .map_err(|e| redb_err("open text ended", e))?;
        history_rows::ended_seen_at(&table, (database_id, tid), index, cut)
    }

    fn scan_ended(&self, visit: &mut dyn FnMut(EndedRow<'_>)) -> crate::Result<()> {
        let table = self
            .txn
            .open_table(TEXT_ENDED)
            .map_err(|e| redb_err("open text ended", e))?;
        history_rows::scan_ended(&table, visit)
    }

    fn scan_live(&self, visit: &mut dyn FnMut(LiveRow<'_>)) -> crate::Result<()> {
        let table = self
            .txn
            .open_table(TEXT_LIVE_BY_FROM)
            .map_err(|e| redb_err("open text live_by_from", e))?;
        history_rows::scan_live(&table, visit)
    }
}

impl FtsHistoryRead for RedbHistoryTxn<'_> {
    type Error = crate::Error;

    fn live_from(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
    ) -> crate::Result<Option<i64>> {
        let table = self
            .txn
            .open_table(TEXT_LIVE_FROM)
            .map_err(|e| redb_err("open text live_from", e))?;
        history_rows::live_from(&table, (database_id, tid), index, surrogate)
    }

    fn live_started_after(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        cut: TextCut,
    ) -> crate::Result<Vec<Surrogate>> {
        let table = self
            .txn
            .open_table(TEXT_LIVE_BY_FROM)
            .map_err(|e| redb_err("open text live_by_from", e))?;
        history_rows::live_started_after(&table, (database_id, tid), index, cut)
    }

    fn ended_seen_at(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        cut: TextCut,
    ) -> crate::Result<Vec<EndedVersion>> {
        let table = self
            .txn
            .open_table(TEXT_ENDED)
            .map_err(|e| redb_err("open text ended", e))?;
        history_rows::ended_seen_at(&table, (database_id, tid), index, cut)
    }

    fn scan_ended(&self, visit: &mut dyn FnMut(EndedRow<'_>)) -> crate::Result<()> {
        let table = self
            .txn
            .open_table(TEXT_ENDED)
            .map_err(|e| redb_err("open text ended", e))?;
        history_rows::scan_ended(&table, visit)
    }

    fn scan_live(&self, visit: &mut dyn FnMut(LiveRow<'_>)) -> crate::Result<()> {
        let table = self
            .txn
            .open_table(TEXT_LIVE_BY_FROM)
            .map_err(|e| redb_err("open text live_by_from", e))?;
        history_rows::scan_live(&table, visit)
    }
}

impl FtsHistoryWrite for RedbHistoryTxn<'_> {
    fn set_live_from(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
        sys_from: i64,
    ) -> crate::Result<()> {
        history_rows::set_live(
            self.txn,
            (database_id, tid),
            index,
            surrogate,
            sys_from,
            self.delta,
        )
    }

    fn clear_live_from(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
    ) -> crate::Result<Option<i64>> {
        history_rows::clear_live(self.txn, (database_id, tid), index, surrogate, self.delta)
    }

    fn put_ended(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        version: &EndedVersion,
    ) -> crate::Result<()> {
        history_rows::put_ended(self.txn, (database_id, tid), index, version, self.delta)
    }

    fn take_ended_at(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
        sys_to: i64,
    ) -> crate::Result<Option<EndedVersion>> {
        history_rows::take_ended_at(
            self.txn,
            (database_id, tid),
            index,
            surrogate,
            sys_to,
            self.delta,
        )
    }

    fn remove_ended(&mut self, keys: &[EndedKey]) -> crate::Result<()> {
        history_rows::remove_ended(self.txn, keys, self.delta)
    }

    fn remove_live(&mut self, keys: &[LiveKey]) -> crate::Result<()> {
        history_rows::remove_live(self.txn, keys, self.delta)
    }
}
