// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for WAL archival capture sites.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A WAL segment due for deletion could not be archived to cold storage, so
/// the checkpoint held truncation back at that segment.
pub(in crate::diag) struct WalArchivalFailedTruncationHeld<'a> {
    /// Which archival step failed (`list_segments`, `recover`, `upload`).
    pub stage: &'a str,
    /// Stable class of the failure, as the cold-storage layer described it.
    pub error_class: &'a str,
    /// First LSN of the segment the archive is missing.
    pub segment_first_lsn: u64,
}

impl DomainContext for WalArchivalFailedTruncationHeld<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.wal_archival_failed_truncation_held"
    }

    fn grouping_key(&self) -> String {
        // Stage + error class name the fault. Segment and LSN are the
        // occurrence: one unreachable cold store spans many cycles and many
        // segments, and all of them belong in one growing report.
        format!("stage={};class={}", self.stage, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "stage": self.stage,
            "error_class": self.error_class,
            "segment_first_lsn": self.segment_first_lsn,
            "why_fatal": "the archive is the only copy of a WAL segment once truncation \
                          unlinks it, so a segment that fails to upload and is deleted \
                          anyway leaves a permanent hole that no point-in-time recovery \
                          can cross. Truncation stops at this segment instead, which \
                          holds the local WAL on disk until archival recovers",
            "operator_action": "restore the cold-storage endpoint named in this node's \
                                 config — credentials, bucket policy, reachability. The \
                                 local WAL grows until the next checkpoint archives this \
                                 segment, so treat WAL disk usage as the clock",
        })
    }
}
