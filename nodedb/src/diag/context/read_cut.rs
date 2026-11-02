// SPDX-License-Identifier: BUSL-1.1

//! Forensic payload for an ordinal reservation that outlived its write.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// An ordinal reservation stayed open past the age any write holds one.
pub(in crate::diag) struct OrdinalReservationLeaked {
    /// `file:line` of the reservation.
    pub site: String,
    /// The first ordinal the write reserved.
    pub ordinal: i64,
    /// How long the reservation had been open.
    pub open_for_ms: u64,
}

impl DomainContext for OrdinalReservationLeaked {
    fn domain_kind(&self) -> &'static str {
        "nodedb.ordinal_reservation_leaked"
    }

    fn grouping_key(&self) -> String {
        // One report per reservation site. The ordinal and the age are the
        // occurrence.
        format!("ordinal_reservation_leaked:{}", self.site)
    }

    fn to_json(&self) -> Value {
        json!({
            "site": self.site,
            "ordinal": self.ordinal,
            "open_for_ms": self.open_for_ms,
            "why_fatal": "a read cut at or above the ordinal waits for the reservation to \
                          release, so every snapshot read at or above it on this node fails \
                          at its statement deadline until the reservation is dropped",
            "operator_action": "find the task that holds the write at the site and why it \
                                 never completes; restarting the node drops the reservation",
        })
    }
}
