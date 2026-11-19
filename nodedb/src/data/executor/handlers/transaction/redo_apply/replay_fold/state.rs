// SPDX-License-Identifier: BUSL-1.1

//! What restart replay keeps while it folds the never-durable records.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{RedoSumTargets, ResolvedSumTarget};

use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::wal::{NeverDurableRedos, WriteSetCapture};

/// The replay folds of one core.
#[derive(Default)]
pub(in crate::data::executor) struct ReplayFolds {
    /// The committed records boot settle judged never durable.
    never_durable: NeverDurableRedos,
    /// The sum targets of each never-durable record, by its LSN, then by
    /// source collection.
    targets: HashMap<u64, HashMap<String, RedoSumTargets>>,
    /// The ops of each record whose folds are durable, by the record's LSN:
    /// a batched install folded ops `0..ledger` before the crash.
    ledger: HashMap<u64, u64>,
    /// The record whose document writes replay folds now.
    pub(super) open: Option<OpenFold>,
    /// The write sets stored for the records replay folded.
    pub(super) captures: Vec<WriteSetCapture>,
}

/// The never-durable record the document arm folds now. Its store commits
/// are deferred until its write set is stored.
pub(super) struct OpenFold {
    pub lsn: u64,
    /// The `TransactionRedo` WAL record of the record.
    pub tenant_id: u64,
    pub vshard_id: u32,
    pub database_id: u64,
    pub apply_key: u64,
    pub event_source: u8,
    pub commit_hlc: u64,
    pub payload: Vec<u8>,
    /// The fold target rows the record's document writes wrote, in order.
    pub target_writes: Vec<TargetWrite>,
    /// The apply scope's ordinal before the fold opened.
    pub outer_ordinal: Option<i64>,
}

impl ReplayFolds {
    pub(super) fn set_never_durable(
        &mut self,
        never_durable: NeverDurableRedos,
        ledger: HashMap<u64, u64>,
    ) {
        self.never_durable = never_durable;
        self.ledger = ledger;
    }

    /// Whether boot judged any record never durable.
    pub(super) fn is_idle(&self) -> bool {
        self.never_durable.is_empty()
    }

    pub(super) fn is_never_durable(&self, lsn: u64) -> bool {
        self.never_durable.contains(lsn)
    }

    /// Keep the targets of each never-durable record among `targets`.
    pub(super) fn keep_targets(&mut self, targets: Vec<(u64, Vec<RedoSumTargets>)>) {
        for (lsn, by_source) in targets {
            if !self.never_durable.contains(lsn) {
                continue;
            }
            self.targets.insert(
                lsn,
                by_source
                    .into_iter()
                    .map(|targets| (targets.collection.clone(), targets))
                    .collect(),
            );
        }
    }

    /// The LSN of the record replay folds now.
    pub(super) fn open_lsn(&self) -> Option<u64> {
        self.open.as_ref().map(|open| open.lsn)
    }

    /// Whether op `op` of the record at `lsn` folds: replay folds that record
    /// now, and the op sits at or above the record's fold ledger. A record
    /// with no ledger entry folds from op 0. A record carrying no op index
    /// is no op of the record, so it folds nothing.
    pub(super) fn folds_op(&self, lsn: u64, op: Option<u64>) -> bool {
        if self.open_lsn() != Some(lsn) {
            return false;
        }
        let durable = self.ledger.get(&lsn).copied().unwrap_or(0);
        op.is_some_and(|op| op >= durable)
    }

    /// The targets a write of the record at `lsn` to `collection` folds
    /// into. Empty unless replay folds that record now.
    pub(super) fn targets_for(&self, lsn: u64, collection: &str) -> Vec<ResolvedSumTarget> {
        if self.open_lsn() != Some(lsn) {
            return Vec::new();
        }
        self.targets
            .get(&lsn)
            .and_then(|by_source| by_source.get(collection))
            .map(|targets| targets.resolved.clone())
            .unwrap_or_default()
    }

    /// The sum targets of the record at `lsn`, in source collection order.
    pub(super) fn record_targets(&self, lsn: u64) -> Vec<RedoSumTargets> {
        let mut targets: Vec<RedoSumTargets> = self
            .targets
            .get(&lsn)
            .map(|by_source| by_source.values().cloned().collect())
            .unwrap_or_default();
        targets.sort_by(|a, b| a.collection.cmp(&b.collection));
        targets
    }

    /// The committed records boot settle judged never durable, and the
    /// records replay judges.
    pub(in crate::data::executor) fn never_durable(&self) -> &NeverDurableRedos {
        &self.never_durable
    }

    /// Hand back the stored write sets and forget every fold: replay ended.
    pub(super) fn finish(&mut self) -> Vec<WriteSetCapture> {
        let captures = std::mem::take(&mut self.captures);
        *self = Self::default();
        captures
    }
}
