// SPDX-License-Identifier: BUSL-1.1

//! The non-recording implementation of the Calvin sequencer's report sites.
//!
//! Compiled when the `diagnostics` feature is off, and unconditionally on
//! wasm32 where there is no filesystem to write a report to. Every entry
//! point keeps the signature of its recording counterpart so call sites are
//! free of `cfg`, and every one is empty so the sequencer behaves
//! byte-for-byte as it did before the recorder existed.

#[inline]
pub fn sequencer_epoch_gap(
    _epoch_expected: u64,
    _epoch_found: u64,
    _direction: &'static str,
    _txns_in_batch: usize,
    _raft_index: u64,
) {
}

#[inline]
pub fn sequencer_backpressure_drop(
    _epoch: u64,
    _dropped_count: u64,
    _drops: &[(u32, &'static str)],
) {
}

#[inline]
pub fn sequencer_participants_underivable(_epoch: u64, _raft_index: u64, _detail: &str) {}

#[inline]
pub fn raft_disk_write_retried(_group_id: u64, _attempt: u32, _ops: &[&'static str], _error: &str) {
}

#[inline]
pub fn raft_disk_fail_stopped(_group_id: u64, _ops: &[&'static str], _error: &str) {}

#[inline]
pub fn raft_disk_write_abandoned(_group_id: u64, _ops: &[&'static str], _error: &str) {}

#[inline]
pub fn calvin_verdict_conflict(
    _epoch: u64,
    _position: u32,
    _difference: &'static str,
    _kept: &str,
    _refused: &str,
) {
}
