// SPDX-License-Identifier: BUSL-1.1

//! Hash-chain verification over the whole audit log.
//!
//! Each entry's `prev_hash` must equal the hash of the entry before it.
//! Retention pruning removes the oldest entries, so the oldest surviving entry
//! links to a deleted one. Each prune records an `AuditCheckpoint` entry first.
//! Its detail names the first surviving seq and the hash of the last deleted entry.
//! The checkpoint is itself a normal chain link, so its detail is tamper-evident.

use std::collections::HashMap;

use super::entry::{AuditEntry, hash_entry};
use super::event::AuditEvent;

/// Detail token naming the first seq that survives a prune.
const OLDEST_KEPT_SEQ: &str = "oldest_kept_seq=";
/// Detail token naming the hash of the last entry a prune deletes.
const ANCHOR_HASH: &str = "anchor_hash=";

/// Build the detail of an `AuditCheckpoint` entry for one prune.
pub fn checkpoint_detail(pruned_count: u64, oldest_kept_seq: u64, anchor_hash: &str) -> String {
    format!(
        "pruned_count={pruned_count} {OLDEST_KEPT_SEQ}{oldest_kept_seq} {ANCHOR_HASH}{anchor_hash}"
    )
}

/// The `(oldest_kept_seq, anchor_hash)` pair a checkpoint entry records.
fn checkpoint_anchor(entry: &AuditEntry) -> Option<(u64, &str)> {
    if entry.event != AuditEvent::AuditCheckpoint {
        return None;
    }
    let mut seq = None;
    let mut hash = None;
    for token in entry.detail.split_whitespace() {
        if let Some(value) = token.strip_prefix(OLDEST_KEPT_SEQ) {
            seq = value.parse::<u64>().ok();
        } else if let Some(value) = token.strip_prefix(ANCHOR_HASH) {
            hash = Some(value);
        }
    }
    Some((seq?, hash?))
}

/// Outcome of one chain verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainReport {
    /// Entries in the requested seq range whose link passed.
    pub checked: u64,
    /// First seq in the range whose link failed.
    ///
    /// A link fails when an entry's `prev_hash` differs from its predecessor's hash.
    /// So an altered row is this seq or the seq before it.
    /// For the newest entry, a mismatch with the live tail hash also fails.
    pub broken_at: Option<u64>,
    /// Hash of the last entry that passed, or empty when none passed.
    pub last_hash: String,
}

/// Verifies the chain one entry at a time, holding no entry.
///
/// It keeps the checkpoint anchors, one per prune, and the last hash.
/// Feed the whole log in ascending seq order with one entry per seq.
/// Entries before `from_seq` still feed the link of the first entry in range.
pub struct ChainVerifier<'a> {
    from_seq: u64,
    to_seq: u64,
    /// The live log's last hash. It covers the newest entry, which no successor checks.
    tail_hash: &'a str,
    /// A later checkpoint can anchor the oldest entry, so that link is judged at the end.
    anchors: HashMap<u64, String>,
    /// The oldest entry's seq and `prev_hash`.
    oldest: Option<(u64, String)>,
    /// The hash the next entry must link to.
    expected: Option<String>,
    newest_seq: Option<u64>,
    /// Link checks ended: a link broke, or an entry passed `to_seq`.
    stopped: bool,
    report: ChainReport,
    /// Highest seq fed that lies in the requested range.
    last_in_range: Option<u64>,
}

impl<'a> ChainVerifier<'a> {
    /// A verifier that reports on the seqs in `from_seq..=to_seq`.
    pub fn new(from_seq: u64, to_seq: u64, tail_hash: &'a str) -> Self {
        Self {
            from_seq,
            to_seq,
            tail_hash,
            anchors: HashMap::new(),
            oldest: None,
            expected: None,
            newest_seq: None,
            stopped: false,
            report: ChainReport {
                checked: 0,
                broken_at: None,
                last_hash: String::new(),
            },
            last_in_range: None,
        }
    }

    /// Feed the next entry.
    pub fn push(&mut self, entry: &AuditEntry) {
        if let Some((seq, hash)) = checkpoint_anchor(entry) {
            self.anchors.insert(seq, hash.to_string());
        }
        self.newest_seq = Some(entry.seq);
        if (self.from_seq..=self.to_seq).contains(&entry.seq) {
            self.last_in_range = Some(entry.seq);
        }
        if self.stopped {
            return;
        }
        if entry.seq > self.to_seq {
            self.stopped = true;
            return;
        }
        let linked = match &self.expected {
            Some(prev) => entry.prev_hash == *prev,
            None => {
                self.oldest = Some((entry.seq, entry.prev_hash.clone()));
                true
            }
        };
        let hash = hash_entry(entry);
        if entry.seq >= self.from_seq {
            if !linked {
                self.report.broken_at = Some(entry.seq);
                self.stopped = true;
                return;
            }
            self.report.checked += 1;
            self.report.last_hash.clone_from(&hash);
        }
        self.expected = Some(hash);
    }

    /// The highest seq fed that lies in the requested range.
    pub fn last_in_range(&self) -> Option<u64> {
        self.last_in_range
    }

    /// End the scan and report.
    pub fn finish(self) -> ChainReport {
        // The oldest entry is either the genesis entry or a pruned head with a recorded anchor.
        if let Some((seq, prev_hash)) = &self.oldest
            && *seq >= self.from_seq
            && !((*seq == 1 && prev_hash.is_empty())
                || self.anchors.get(seq).is_some_and(|hash| hash == prev_hash))
        {
            return ChainReport {
                checked: 0,
                broken_at: Some(*seq),
                last_hash: String::new(),
            };
        }
        let mut report = self.report;
        if !self.stopped
            && let Some(newest) = self.newest_seq
            && newest >= self.from_seq
            && report.last_hash != self.tail_hash
        {
            report.broken_at = Some(newest);
        }
        report
    }
}

/// Verify the chain over `entries` and report on the seqs in `from_seq..=to_seq`.
///
/// `entries` must be the whole log in ascending seq order with one entry per seq.
#[cfg(test)]
pub fn verify_chain(
    entries: &[AuditEntry],
    from_seq: u64,
    to_seq: u64,
    tail_hash: &str,
) -> ChainReport {
    let mut verifier = ChainVerifier::new(from_seq, to_seq, tail_hash);
    for entry in entries {
        verifier.push(entry);
    }
    verifier.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::AuditLog;

    fn chain_of(details: &[&str]) -> (Vec<AuditEntry>, String) {
        let mut log = AuditLog::new(1_000);
        for detail in details {
            log.record(AuditEvent::AdminAction, None, "src", detail);
        }
        (
            log.all().iter().cloned().collect(),
            log.last_hash().to_string(),
        )
    }

    #[test]
    fn intact_chain_is_valid() {
        let (entries, tail) = chain_of(&["a", "b", "c"]);
        let report = verify_chain(&entries, 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, None);
        assert_eq!(report.checked, 3);
        assert_eq!(report.last_hash, tail);
    }

    #[test]
    fn altered_middle_row_breaks_at_its_successor() {
        let (mut entries, tail) = chain_of(&["a", "b", "c", "d"]);
        entries[1].detail = "tampered".to_string();
        let report = verify_chain(&entries, 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, Some(3));
    }

    #[test]
    fn altered_prev_hash_breaks_at_that_row() {
        let (mut entries, tail) = chain_of(&["a", "b", "c"]);
        entries[1].prev_hash = "0".repeat(64);
        let report = verify_chain(&entries, 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, Some(2));
    }

    #[test]
    fn altered_newest_row_breaks_against_the_tail_hash() {
        let (mut entries, tail) = chain_of(&["a", "b", "c"]);
        entries[2].detail = "tampered".to_string();
        let report = verify_chain(&entries, 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, Some(3));
    }

    #[test]
    fn deleted_row_breaks_at_the_next_row() {
        let (mut entries, tail) = chain_of(&["a", "b", "c", "d"]);
        entries.remove(1);
        let report = verify_chain(&entries, 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, Some(3));
    }

    #[test]
    fn unanchored_truncated_head_is_broken() {
        let (entries, tail) = chain_of(&["a", "b", "c"]);
        let report = verify_chain(&entries[1..], 1, u64::MAX, &tail);
        assert_eq!(report.broken_at, Some(2));
    }

    #[test]
    fn pruned_head_with_checkpoint_is_valid() {
        let mut log = AuditLog::new(1_000);
        log.record(AuditEvent::AdminAction, None, "src", "a");
        log.record(AuditEvent::AdminAction, None, "src", "b");
        log.record(AuditEvent::AdminAction, None, "src", "c");
        let anchor = hash_entry(&log.all()[1]);
        log.record(
            AuditEvent::AuditCheckpoint,
            None,
            "retention",
            &checkpoint_detail(2, 3, &anchor),
        );
        log.record(AuditEvent::AdminAction, None, "src", "d");
        let survivors: Vec<AuditEntry> = log.all().iter().skip(2).cloned().collect();
        let report = verify_chain(&survivors, 1, u64::MAX, log.last_hash());
        assert_eq!(report.broken_at, None);
        assert_eq!(report.checked, 3);
    }

    #[test]
    fn range_uses_earlier_entries_for_its_first_link() {
        let (entries, tail) = chain_of(&["a", "b", "c", "d"]);
        let report = verify_chain(&entries, 3, 3, &tail);
        assert_eq!(report.broken_at, None);
        assert_eq!(report.checked, 1);
    }
}
