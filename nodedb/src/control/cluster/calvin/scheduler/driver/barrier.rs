// SPDX-License-Identifier: BUSL-1.1

//! Dependent-read barrier of the Calvin scheduler.
//!
//! A dependent-read txn names the rows its passive vShards read. On an
//! active vShard (one the write set names) the data-group leader opens a
//! [`PendingDependentBarrier`] once the txn holds its locks. The barrier
//! waits for the read result of every passive vShard through the vShard's
//! data-group log (`ReplicatedWrite::CalvinReadResult`).
//!
//! The log alone decides the barrier, so every replica decides it alike.
//! The scheduler folds the log's barrier entries into a
//! [`crate::control::cluster::calvin::scheduler::BarrierLog`] per txn:
//!
//! - Every result came first: the barrier completes. The leader compares the
//!   values with the ones the coordinator read, and dispatches
//!   `MetaOp::CalvinExecuteActive` or votes `PredictionDrift`.
//! - A `ReplicatedWrite::CalvinReadTimeout` came first: the barrier times
//!   out, and the leader votes abort.
//!
//! The leader proposes the timeout entry once its barrier waited past
//! `tuning.calvin.dependent_read_passive_timeout_ms` on its own clock. The
//! clock only decides when the entry is proposed. Its place in the log
//! decides what it does.
//!
//! # Determinism
//!
//! All maps here use `BTreeMap`/`BTreeSet` — never `HashMap`/`HashSet`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use nodedb_cluster::calvin::types::{DependentReadSpec, SequencedTxn};
use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
use nodedb_types::Value;

use super::super::lock_manager::TxnId;

/// A dependent-read barrier the data-group leader holds open.
pub struct PendingDependentBarrier {
    /// Original sequenced transaction.
    pub txn: SequencedTxn,
    /// The lock-table owner id for this txn (equals the apply-slot id unless a
    /// reservation owns the lock). Carried through the barrier so the eventual
    /// `dispatch_active_txn` call can park the correct id in `PendingTxn`.
    pub lock_owner: TxnId,
    /// The passive vShards whose read result the barrier waits for.
    pub passive: BTreeSet<u32>,
    /// When this leader proposes the barrier's timeout entry, on its own
    /// clock. A proposal re-arms it, so a timeout entry a leader change lost
    /// is proposed again.
    pub timeout_due: Instant,
}

/// The first passive row whose broadcast value differs from the value the
/// coordinator read, described. `None` when every row matches.
///
/// A row reads back as its stored bytes, `Value::Bytes`, or `Value::Null`
/// when absent. A row no passive vShard returned, or one returned as any
/// other value, differs.
pub fn expected_reads_drift(
    spec: &DependentReadSpec,
    injected: &BTreeMap<PassiveReadKeyId, Value>,
) -> Option<String> {
    spec.expected.iter().find_map(|(row, expected)| {
        let found = match injected.get(row) {
            Some(Value::Bytes(bytes)) => Some(bytes.as_slice()),
            Some(Value::Null) => None,
            Some(other) => {
                return Some(format!(
                    "the passive read of {row:?} returned {other:?}, not stored bytes"
                ));
            }
            None => return Some(format!("no passive vShard returned {row:?}")),
        };
        (found != expected.as_deref())
            .then(|| format!("{row:?} changed since the coordinator read it"))
    })
}

#[cfg(test)]
mod tests {
    use nodedb_types::QualifiedCollection;

    use super::*;

    fn row() -> PassiveReadKeyId {
        PassiveReadKeyId::kv(
            QualifiedCollection::from_stored("items".to_owned()),
            b"alice:sword".to_vec(),
        )
    }

    fn spec(expected: Option<&[u8]>) -> DependentReadSpec {
        DependentReadSpec {
            passive_reads: BTreeMap::new(),
            expected: BTreeMap::from([(row(), expected.map(<[u8]>::to_vec))]),
        }
    }

    #[test]
    fn matching_bytes_report_no_drift() {
        let injected = BTreeMap::from([(row(), Value::Bytes(b"v1".to_vec()))]);
        assert_eq!(expected_reads_drift(&spec(Some(b"v1")), &injected), None);
    }

    #[test]
    fn changed_bytes_report_drift() {
        let injected = BTreeMap::from([(row(), Value::Bytes(b"v2".to_vec()))]);
        assert!(expected_reads_drift(&spec(Some(b"v1")), &injected).is_some());
    }

    #[test]
    fn a_row_gone_since_the_read_reports_drift() {
        let injected = BTreeMap::from([(row(), Value::Null)]);
        assert!(expected_reads_drift(&spec(Some(b"v1")), &injected).is_some());
    }

    #[test]
    fn an_absent_row_still_absent_reports_no_drift() {
        let injected = BTreeMap::from([(row(), Value::Null)]);
        assert_eq!(expected_reads_drift(&spec(None), &injected), None);
    }

    #[test]
    fn a_row_no_passive_returned_reports_drift() {
        assert!(expected_reads_drift(&spec(Some(b"v1")), &BTreeMap::new()).is_some());
    }
}
