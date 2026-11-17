// SPDX-License-Identifier: BUSL-1.1

//! Which staged rows only trigger bodies wrote.
//!
//! A client statement and the BEFORE and SYNC AFTER bodies it fired stage
//! into one transaction. A row a body staged must commit with source
//! `Trigger`, so it fires no trigger. A row the client staged keeps the
//! transaction's source, even when a body staged it again later: the client's
//! write fires its triggers.
//!
//! A row tag names a row the way its write event names it:
//! - a document, KV or CRDT row by its identity in its collection. Every
//!   value mutation passes through `TxnOverlay::record_undo`, which calls
//!   [`RowTags::note`];
//! - a graph edge by its `(src, label, dst)` row id in its collection, and a
//!   node-label delta by its node id on the label stream. The graph staging
//!   calls [`TxnOverlay::note_row`] for each.
//!
//! A body write tags a row that was not staged before it. A client write
//! clears the tag. Every tag change is journaled, so a savepoint rollback
//! restores the tag the row held at the savepoint.
//!
//! A staging write also names the collections it writes. A collection only
//! body writes named commits whole under `Trigger`: the group covers rows no
//! row tag names, such as the base rows a body's TRUNCATE removes. A staging
//! write that adds a collection to a set journals the addition, so a savepoint
//! rollback removes a collection whose only write it undid.
//!
//! Array cells, vector, full-text, spatial, columnar and timeseries rows emit
//! no write event on any path, so no row tag names them.

use std::collections::{HashMap, HashSet};

use nodedb_types::RowIdentity;

use super::staged::{CollKey, TxnOverlay};
use super::staged_journal::JournalEntry;
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId};

/// Body-only rows of one transaction.
#[derive(Debug, Default)]
pub struct RowTags {
    /// Whether the staging write in progress runs for a trigger body.
    staging_body: bool,
    /// Whether any staging write ran for a client. A transaction no client
    /// wrote reports no body rows: its own source already covers them.
    saw_client: bool,
    body_rows: HashMap<CollKey, HashSet<RowIdentity>>,
    /// Collections a body staging write named.
    body_collections: HashSet<CollKey>,
    /// Collections a client staging write named.
    client_collections: HashSet<CollKey>,
}

/// One tag change, journaled so a savepoint rollback reverses it.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(super) enum TagUndo {
    /// A row tag set or cleared outside the value journal: a graph edge or a
    /// node-label delta. `prev_tagged` is the tag the row held before.
    Row {
        coll_key: CollKey,
        row: RowIdentity,
        prev_tagged: bool,
    },
    /// Collections a staging write added to the body or the client set.
    /// `first_client` says it was the transaction's first client write.
    Staging {
        body: bool,
        added: Vec<CollKey>,
        first_client: bool,
    },
}

impl RowTags {
    /// Note a mutation of `doc_id` in `coll_key`. `was_staged` says whether
    /// the row held a staged value before it. Returns whether the row was
    /// tagged before, for the undo journal.
    pub(super) fn note(
        &mut self,
        coll_key: &CollKey,
        doc_id: &RowIdentity,
        was_staged: bool,
    ) -> bool {
        let tagged = self.is_body_row(coll_key, doc_id);
        if self.staging_body {
            if !was_staged && !tagged {
                self.body_rows
                    .entry(coll_key.clone())
                    .or_default()
                    .insert(doc_id.clone());
            }
        } else if tagged {
            self.untag(coll_key, doc_id);
        }
        tagged
    }

    /// Restore the tag `doc_id` held before a rolled-back mutation.
    pub(super) fn restore(&mut self, coll_key: &CollKey, doc_id: &RowIdentity, tagged: bool) {
        if tagged {
            self.body_rows
                .entry(coll_key.clone())
                .or_default()
                .insert(doc_id.clone());
        } else {
            self.untag(coll_key, doc_id);
        }
    }

    /// Reverse one journaled tag change.
    pub(super) fn undo(&mut self, undo: TagUndo) {
        match undo {
            TagUndo::Row {
                coll_key,
                row,
                prev_tagged,
            } => self.restore(&coll_key, &row, prev_tagged),
            TagUndo::Staging {
                body,
                added,
                first_client,
            } => {
                let set = if body {
                    &mut self.body_collections
                } else {
                    &mut self.client_collections
                };
                for key in &added {
                    set.remove(key);
                }
                if first_client {
                    self.saw_client = false;
                }
            }
        }
    }

    /// Begin a staging write: `body` says it runs for a trigger body. Returns
    /// the change to journal, `None` when it changed no set.
    fn begin(
        &mut self,
        body: bool,
        collections: impl IntoIterator<Item = CollKey>,
    ) -> Option<TagUndo> {
        self.staging_body = body;
        let first_client = !body && !self.saw_client;
        if !body {
            self.saw_client = true;
        }
        let set = if body {
            &mut self.body_collections
        } else {
            &mut self.client_collections
        };
        let added: Vec<CollKey> = collections
            .into_iter()
            .filter(|key| set.insert(key.clone()))
            .collect();
        (first_client || !added.is_empty()).then_some(TagUndo::Staging {
            body,
            added,
            first_client,
        })
    }

    fn is_body_row(&self, coll_key: &CollKey, doc_id: &RowIdentity) -> bool {
        self.body_rows
            .get(coll_key)
            .is_some_and(|rows| rows.contains(doc_id))
    }

    fn untag(&mut self, coll_key: &CollKey, doc_id: &RowIdentity) {
        if let Some(rows) = self.body_rows.get_mut(coll_key) {
            rows.remove(doc_id);
            if rows.is_empty() {
                self.body_rows.remove(coll_key);
            }
        }
    }
}

/// What a transaction's trigger bodies staged, for its redo record.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BodyWrites {
    /// `(collection, row)` of every row only a body staged.
    pub rows: Vec<(String, String)>,
    /// Every collection only body writes named.
    pub collections: Vec<String>,
}

impl TxnOverlay {
    /// Begin a staging write under `source` that names `collections`. A
    /// `Trigger` write is a body's. Every other source is the client's.
    pub fn begin_staging(
        &mut self,
        source: EventSource,
        collections: impl IntoIterator<Item = CollKey>,
    ) {
        let body = matches!(source, EventSource::Trigger);
        if let Some(undo) = self.row_tags.begin(body, collections) {
            self.journal.push(JournalEntry::Tags(undo));
        }
    }

    /// Note a mutation of `row` in `coll_key` that stages outside the value
    /// journal: a graph edge or a node-label delta. `was_staged` says whether
    /// this transaction had staged the row before.
    pub fn note_row(
        &mut self,
        coll_key: CollKey,
        row: RowIdentity,
        was_staged: bool,
    ) -> crate::Result<()> {
        self.guard()?;
        let prev_tagged = self.row_tags.note(&coll_key, &row, was_staged);
        self.journal.push(JournalEntry::Tags(TagUndo::Row {
            coll_key,
            row,
            prev_tagged,
        }));
        self.spill_past_threshold()
    }

    /// What only trigger bodies staged, when a client also wrote in this
    /// transaction. Empty otherwise: the transaction's own source covers every
    /// row. `database_id` and `tenant_id` pick the transaction's collections.
    pub fn body_writes(&self, database_id: DatabaseId, tenant_id: TenantId) -> BodyWrites {
        let tags = &self.row_tags;
        if !tags.saw_client {
            return BodyWrites::default();
        }
        let ours = |(db, tenant, _): &CollKey| *db == database_id && *tenant == tenant_id;
        let rows = tags
            .body_rows
            .iter()
            .filter(|(key, _)| ours(key))
            .flat_map(|((_, _, collection), rows)| {
                rows.iter()
                    .map(move |row| (collection.clone(), row.as_str().to_owned()))
            })
            .collect();
        let collections = tags
            .body_collections
            .iter()
            .filter(|key| ours(key) && !tags.client_collections.contains(*key))
            .map(|(_, _, collection)| collection.clone())
            .collect();
        BodyWrites { rows, collections }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(coll: &str) -> CollKey {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    fn id(text: &str) -> RowIdentity {
        RowIdentity::from_user_key(text)
    }

    fn writes(overlay: &TxnOverlay) -> BodyWrites {
        let mut writes = overlay.body_writes(DatabaseId::new(1), TenantId::new(1));
        writes.rows.sort();
        writes.collections.sort();
        writes
    }

    #[test]
    fn a_body_row_is_tagged_and_a_client_row_is_not() {
        let mut overlay = TxnOverlay::new();
        overlay.begin_staging(EventSource::ImplicitClient, [key("orders")]);
        overlay
            .insert_put(key("orders"), 1, &id("client"), vec![1])
            .unwrap();
        overlay.begin_staging(EventSource::Trigger, [key("orders"), key("edges")]);
        overlay
            .insert_put(key("orders"), 2, &id("body"), vec![2])
            .unwrap();
        // A body write to the client's row keeps it the client's.
        overlay
            .insert_put(key("orders"), 1, &id("client"), vec![3])
            .unwrap();

        assert_eq!(
            writes(&overlay),
            BodyWrites {
                rows: vec![("orders".to_string(), "body".to_string())],
                collections: vec!["edges".to_string()],
            }
        );
    }

    #[test]
    fn a_savepoint_rollback_restores_the_tag() {
        let mut overlay = TxnOverlay::new();
        overlay.begin_staging(EventSource::User, []);
        overlay.begin_staging(EventSource::Trigger, []);
        overlay
            .insert_put(key("audit"), 1, &id("a"), vec![1])
            .unwrap();
        let marker = overlay.journal_len();
        overlay.begin_staging(EventSource::User, []);
        overlay
            .insert_put(key("audit"), 1, &id("a"), vec![2])
            .unwrap();
        assert!(writes(&overlay).rows.is_empty());

        overlay.rollback_to(marker).unwrap();
        assert_eq!(
            writes(&overlay).rows,
            vec![("audit".to_string(), "a".to_string())]
        );
    }

    #[test]
    fn a_transaction_no_client_wrote_reports_no_body_rows() {
        let mut overlay = TxnOverlay::new();
        overlay.begin_staging(EventSource::Trigger, [key("audit")]);
        overlay
            .insert_put(key("audit"), 1, &id("a"), vec![1])
            .unwrap();
        assert_eq!(writes(&overlay), BodyWrites::default());
    }

    /// A savepoint rollback that removes a collection's only write leaves the
    /// collection out of the commit's groups.
    #[test]
    fn a_savepoint_rollback_rewinds_the_collection_sets() {
        let mut overlay = TxnOverlay::new();
        overlay.begin_staging(EventSource::User, [key("orders")]);
        overlay
            .insert_put(key("orders"), 1, &id("o1"), vec![1])
            .unwrap();
        let marker = overlay.journal_len();
        overlay.begin_staging(EventSource::Trigger, [key("purged")]);
        overlay.mark_truncated(key("purged")).unwrap();
        assert_eq!(writes(&overlay).collections, vec!["purged".to_string()]);

        overlay.rollback_to(marker).unwrap();
        assert_eq!(writes(&overlay), BodyWrites::default());
    }

    /// Rolling back to before the first client write forgets that a client
    /// wrote: the body rows staged later are the transaction's own.
    #[test]
    fn a_savepoint_rollback_rewinds_the_first_client_write() {
        let mut overlay = TxnOverlay::new();
        let marker = overlay.journal_len();
        overlay.begin_staging(EventSource::User, [key("orders")]);
        overlay.rollback_to(marker).unwrap();
        overlay.begin_staging(EventSource::Trigger, [key("audit")]);
        overlay
            .insert_put(key("audit"), 1, &id("a"), vec![1])
            .unwrap();
        assert_eq!(writes(&overlay), BodyWrites::default());
    }

    /// A graph edge a body staged is tagged by its edge row id, and a client
    /// write of the same edge clears the tag. A savepoint rollback restores
    /// it.
    #[test]
    fn a_body_graph_edge_is_tagged_per_row() {
        let edge = crate::event::graph_cdc::edge_row_id("a", "KNOWS", "b");
        let other = crate::event::graph_cdc::edge_row_id("a", "KNOWS", "c");
        let mut overlay = TxnOverlay::new();
        overlay.begin_staging(EventSource::User, [key("social")]);
        overlay.note_row(key("social"), id(&other), false).unwrap();
        overlay.begin_staging(EventSource::Trigger, [key("social")]);
        overlay.note_row(key("social"), id(&edge), false).unwrap();
        assert_eq!(
            writes(&overlay).rows,
            vec![("social".to_string(), edge.clone())]
        );

        let marker = overlay.journal_len();
        overlay.begin_staging(EventSource::User, [key("social")]);
        overlay.note_row(key("social"), id(&edge), true).unwrap();
        assert!(writes(&overlay).rows.is_empty());
        overlay.rollback_to(marker).unwrap();
        assert_eq!(writes(&overlay).rows, vec![("social".to_string(), edge)]);
    }

    /// A node-label delta a body staged is tagged by its node id on the
    /// label stream.
    #[test]
    fn a_body_node_label_is_tagged_per_row() {
        let stream = crate::event::graph_cdc::GRAPH_LABEL_STREAM;
        let mut overlay = TxnOverlay::new();
        overlay.begin_staging(EventSource::User, [key("people")]);
        overlay
            .insert_put(key("people"), 1, &id("n1"), vec![1])
            .unwrap();
        overlay.begin_staging(EventSource::Trigger, [key(stream)]);
        overlay.note_row(key(stream), id("n1"), false).unwrap();
        assert_eq!(
            writes(&overlay).rows,
            vec![(stream.to_string(), "n1".to_string())]
        );
    }

    /// A CRDT row stages through the value journal, so a body's CRDT row is
    /// tagged by its document id beside the client's row in the same
    /// collection.
    #[test]
    fn a_body_crdt_row_is_tagged_per_row() {
        let mut overlay = TxnOverlay::new();
        overlay.begin_staging(EventSource::ImplicitClient, [key("notes")]);
        overlay
            .insert_put(key("notes"), 1, &id("client-note"), vec![1])
            .unwrap();
        overlay.begin_staging(EventSource::Trigger, [key("notes")]);
        overlay
            .insert_put(key("notes"), 2, &id("body-note"), vec![2])
            .unwrap();
        overlay
            .insert_tombstone(key("notes"), 3, &id("body-gone"))
            .unwrap();
        assert_eq!(
            writes(&overlay),
            BodyWrites {
                rows: vec![
                    ("notes".to_string(), "body-gone".to_string()),
                    ("notes".to_string(), "body-note".to_string()),
                ],
                collections: Vec::new(),
            }
        );
    }
}
