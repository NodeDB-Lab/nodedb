// SPDX-License-Identifier: BUSL-1.1

//! Verdict-driven commit resolution for staged static Calvin transactions.
//!
//! A static Calvin dispatch STAGES its transaction on the Data Plane (validate
//! the read-set + buffer the plans, no base mutation). Its executor response
//! carries the local commit vote on `stage_vote`. This module drives the
//! final step: dispatch a flush (commit, after `commit_redo` has WAL-appended
//! the resolved `TransactionRedo`) or drop (abort) of the staged buffer, wait
//! for its response, then run the commit tail (deposit applied result, record
//! write versions — plus a `CalvinApplied` WAL fallback when no redo record
//! was appended — propose `CompletionAck`) for a flush, or ack-only for a
//! drop.

mod apply_tail;
mod flush_parts;
mod verdict;
mod vote;
