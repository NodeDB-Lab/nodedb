// SPDX-License-Identifier: BUSL-1.1

//! Forensic payload for a stored strict row the Data Plane could not render
//! as a MessagePack image.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A stored strict row that decodes neither as a Binary Tuple of its
/// collection's schema nor as a MessagePack map. Its image is withheld from
/// the Event Plane and from the write-set journal.
pub(in crate::diag) struct StrictRowImageUnrendered<'a> {
    /// Collection whose stored row did not render.
    pub collection: &'a str,
    /// Stable class of the decode failure (`not_a_tuple`, `schema_version`,
    /// `layout`, `encode`).
    pub fault: &'static str,
}

impl DomainContext for StrictRowImageUnrendered<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.strict_row_image_unrendered"
    }

    fn grouping_key(&self) -> String {
        // Collection and fault class name the root cause. The row is the
        // occurrence, so a scan over many bad rows files one report.
        format!("collection={};fault={}", self.collection, self.fault)
    }

    fn to_json(&self) -> Value {
        json!({
            "collection": self.collection,
            "fault": self.fault,
            "why_fatal": "the row's stored bytes do not decode against the schema this core \
                          holds. A trigger, change stream, materialized view, or CRDT peer \
                          that received the raw bytes would read them as a different row or \
                          fail to read them. The event is dead-lettered without the image, \
                          and a write whose journal needs the image is refused",
            "operator_action": "read the named collection's rows directly. One bad row points \
                                 at a truncated or overwritten body. Every row failing with \
                                 `schema_version` points at a core whose schema is older than \
                                 the rows it stores. Restore the collection from a snapshot or \
                                 rewrite the affected rows",
        })
    }
}
