// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for write-path and indexing capture sites.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A write whose redo record the Control-Plane funnel was supposed to mint
/// reached the client acknowledgement with no durable LSN to wait on.
pub(in crate::diag) struct WriteAckedWithoutDurability {
    /// Engine whose every write-class op is expected to mint a WAL redo on
    /// this path (`kv`, `vector`, `graph`, ...).
    pub engine: &'static str,
}

impl DomainContext for WriteAckedWithoutDurability {
    fn domain_kind(&self) -> &'static str {
        "nodedb.write_acked_without_durability"
    }

    fn grouping_key(&self) -> String {
        // Engine names the bug: a property of its WAL-append classifier.
        format!("engine={}", self.engine)
    }

    fn to_json(&self) -> Value {
        json!({
            "engine": self.engine,
            "why_fatal": "the funnel appended this write's redo itself, so a missing LSN \
                          means no record was minted at all — the durable-at-ack barrier \
                          is skipped and the client is told the write committed. This \
                          engine's state survives a restart only by WAL replay, so a \
                          'kill -9' after the ack loses an acknowledged write with no \
                          error anywhere",
            "operator_action": "inspect the named engine's arm in the Control-Plane WAL \
                                 append dispatch: a write-class op filed under the \
                                 'no durable record' group mints nothing. Either give it \
                                 a redo record or move the engine out of the set the \
                                 barrier holds to this invariant",
        })
    }
}

/// A document write was rejected because its full-text index update failed.
/// The row and index share one transaction, so the rejection is clean;
/// the report captures that the collection's index is refusing writes.
pub(in crate::diag) struct FtsIndexUpdateFailed<'a> {
    /// Collection whose inverted index rejected the document's terms.
    pub collection: &'a str,
    /// Global surrogate identity of the document that failed to index.
    pub surrogate: u32,
    /// Stable class of the failure, as the index layer described it.
    pub error_class: &'a str,
}

impl DomainContext for FtsIndexUpdateFailed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.fts_index_update_failed"
    }

    fn grouping_key(&self) -> String {
        // Collection + error class name the bug; surrogate is the occurrence,
        // or a bulk load would file one report per row.
        format!("collection={};cause={}", self.collection, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "collection": self.collection,
            "surrogate": self.surrogate,
            "error_class": self.error_class,
            "why_fatal": "the inverted index shares the row's write transaction, so the \
                          failure aborts the whole write and the client is told the write \
                          did not happen — nothing is silently half-applied. It is filed \
                          anyway because a structural cause makes EVERY write to this \
                          collection fail from here on, and the only symptom the operator \
                          sees is writes being refused with no indication that the index \
                          is what refused them",
            "operator_action": "read the error class: a transient cause (redb contention, \
                                 a full disk) clears once the resource does, while a \
                                 structural one (a corrupt or type-mismatched FTS table) \
                                 will re-fail on every write until the collection's index \
                                 is rebuilt",
        })
    }
}

/// A document batch insert arrived without a surrogate for every row, so
/// the rows have no cross-engine identity to index under. Every index is
/// keyed by surrogate, so the plan is rejected rather than stored unindexed.
pub(in crate::diag) struct BatchInsertWithoutSurrogates<'a> {
    /// Collection the malformed batch targeted.
    pub collection: &'a str,
    /// Rows the batch carried.
    pub document_count: usize,
    /// Surrogates it carried for them.
    pub surrogate_count: usize,
}

impl DomainContext for BatchInsertWithoutSurrogates<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.batch_insert_without_surrogates"
    }

    fn grouping_key(&self) -> String {
        // Collection names the bug; the two counts are the occurrence, not
        // the key, or retries would file one report per batch size.
        format!("collection={}", self.collection)
    }

    fn to_json(&self) -> Value {
        json!({
            "collection": self.collection,
            "document_count": self.document_count,
            "surrogate_count": self.surrogate_count,
            "why_fatal": "the batch is refused outright, so nothing is written and the \
                          client is told the insert did not happen. It is filed anyway \
                          because the defect is in whatever produced the plan, and that \
                          producer is invisible from the rejection: the alternative — \
                          storing the rows unindexed and reporting success — would leave \
                          rows that full-text, vector, spatial, and secondary-index \
                          lookups all silently omit",
            "operator_action": "identify the producer: a native batch-insert builder \
                                 assigns one surrogate per document, so a mismatch points \
                                 either at a client path that bypassed assignment or at a \
                                 truncated replicated write record",
        })
    }
}

/// A committed DELETE's post-commit cascade failed to remove one row's
/// entry from a secondary structure. The row's own transaction already
/// committed, so this failure cannot roll it back — the row is gone from
/// the primary store, but its entry survives in the named index.
pub(in crate::diag) struct OrphanedIndexEntryAfterDelete<'a> {
    /// Collection the deleted row belonged to.
    pub collection: &'a str,
    /// Which cascaded structure the entry was left behind in
    /// (`"inverted"`, `"secondary"`, `"graph_edge"`).
    pub index_kind: &'static str,
    /// Stable class of the failure, as the index layer described it.
    pub error_class: &'a str,
}

impl DomainContext for OrphanedIndexEntryAfterDelete<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.orphaned_index_entry_after_delete"
    }

    fn grouping_key(&self) -> String {
        // Collection + index kind + error class name the bug; the deleted
        // row's id is the occurrence, or a bulk delete over many rows would
        // file one report per row instead of one growing report.
        format!(
            "collection={};index={};cause={}",
            self.collection, self.index_kind, self.error_class
        )
    }

    fn to_json(&self) -> Value {
        json!({
            "collection": self.collection,
            "index_kind": self.index_kind,
            "error_class": self.error_class,
            "why_fatal": "the row's DELETE already committed by the time this cascade \
                          step runs, so the failure cannot be rolled back — the row is \
                          gone from the primary store, but the named index still carries \
                          an entry that points at nothing. A stale inverted-index posting \
                          scores a deleted row in full-text search, a stale secondary-index \
                          entry returns a document that no longer exists, and a stale graph \
                          edge traverses to a removed node",
            "operator_action": "read the error class: a transient cause (redb contention, \
                                 a full disk) clears on its own, and the orphaned entry is \
                                 pruned the next time the collection's index is rebuilt or \
                                 compacted; a structural cause (corrupt or type-mismatched \
                                 index table) will orphan every subsequent delete on this \
                                 collection until the index is rebuilt",
        })
    }
}

/// A stored Binary Tuple that does not decode against its collection's
/// strict schema. The bytes on disk are wrong, so the statement that read
/// them is refused rather than applied over a partial row set.
pub(in crate::diag) struct StrictRowUndecodable<'a> {
    /// Collection whose stored row did not decode.
    pub collection: &'a str,
    /// Storage key of the row that did not decode.
    pub doc_id: &'a str,
    /// Detection site inside the UPDATE path.
    pub site: &'static str,
}

impl DomainContext for StrictRowUndecodable<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.strict_row_undecodable"
    }

    fn grouping_key(&self) -> String {
        // Collection names the root cause: one schema, one stored form. The
        // row id is the occurrence, so a scan over many bad rows files one
        // report with a rising count.
        format!("collection={}", self.collection)
    }

    fn to_json(&self) -> Value {
        json!({
            "collection": self.collection,
            "doc_id": self.doc_id,
            "site": self.site,
            "why_fatal": "the row is stored state that no longer matches the schema this \
                          build decodes it with, so every statement reading it is refused \
                          from here on. The bytes are already on disk, so the damage \
                          outlives the statement and outlives the process — a restart \
                          re-reads the same row and fails the same way",
            "operator_action": "read the named row of the named collection directly: a \
                                 single bad row points at a truncated or partially written \
                                 body, while every row failing points at a schema whose \
                                 stored column layout no longer matches the catalog. \
                                 Restore the collection from a snapshot or rewrite the \
                                 named row",
        })
    }
}
