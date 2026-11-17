// SPDX-License-Identifier: BUSL-1.1

//! Replay of one stored document version or one stored sync-frame version
//! into a full-text index.
//!
//! A snapshot restore and a text-index rebuild both walk a collection's
//! stored versions in row order, then system-time order. Each version goes
//! through [`replay_version_into`] at its own ordinal. Both paths then
//! leave identical postings and identical text history. A live version
//! indexes its declared fields as an UPDATE does. A tombstone ends the
//! row's text as a DELETE does. Sync-frame versions replay the same way
//! through [`replay_frame_into`].
//!
//! The free functions take the index and the collection's declared fields
//! as arguments, so a rebuild thread runs them against its scratch index.
//! The `CoreLoop` methods run them against the live index with the core's
//! own fields.

use redb::WriteTransaction;

use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::fts_text::{declared_pairs, extract_fts_fields};
use crate::engine::document::store::CollectionConfig;
use crate::engine::sparse::inverted::{IndexDocScope, InvertedIndex};
use crate::types::TenantId;

/// One stored document version to replay.
pub(in crate::data::executor) struct TextVersion<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub surrogate: Surrogate,
    /// The version's system-time ordinal.
    pub ordinal: i64,
    /// The version's stored body. `None` for a tombstone or an erasure.
    pub body: Option<&'a [u8]>,
}

/// The index a replay writes and how it reads the collection's rows.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct ReplayTarget<'a> {
    pub inverted: &'a InvertedIndex,
    /// The collection's declared text fields, sorted and deduped.
    pub text_fields: &'a [String],
    /// The collection's registered config. `None` for a collection this
    /// core has no config for.
    pub config: Option<&'a CollectionConfig>,
}

/// Decode a stored body as the collection's write path encoded it. A
/// registered collection's live bodies must decode. An unregistered
/// collection's unreadable body has no fields to index, as on the insert
/// path.
fn decode_body(
    config: Option<&CollectionConfig>,
    body: &[u8],
) -> crate::Result<Option<serde_json::Value>> {
    match config {
        Some(cfg) => {
            let strict_schema = match &cfg.storage_mode {
                nodedb_physical::physical_plan::StorageMode::Strict { schema } => Some(schema),
                nodedb_physical::physical_plan::StorageMode::Schemaless => None,
            };
            crate::data::executor::doc_format::decode_document_or_binary_tuple(
                body,
                strict_schema,
                "stored row body",
            )
            .map(Some)
        }
        None => Ok(crate::data::executor::doc_format::decode_document(body).ok()),
    }
}

/// Replay `version` into `target`'s index inside `txn`. Does NOT commit.
pub(in crate::data::executor) fn replay_version_into(
    target: ReplayTarget<'_>,
    txn: &WriteTransaction,
    version: TextVersion<'_>,
) -> crate::Result<()> {
    let TextVersion {
        database_id,
        tid,
        collection,
        surrogate,
        ordinal,
        body,
    } = version;
    let doc_scope = IndexDocScope {
        database_id,
        tid: TenantId::new(tid),
        collection,
        surrogate,
        ordinal,
    };
    let doc = match body {
        Some(body) => decode_body(target.config, body)?,
        None => None,
    };
    match doc {
        Some(doc) => {
            // A collection with no declared text field holds no text.
            if target.text_fields.is_empty() {
                return Ok(());
            }
            let text = extract_fts_fields(&doc, target.text_fields);
            target
                .inverted
                .index_document_in_txn(txn, doc_scope, &text)
                .inspect_err(|e| {
                    crate::diag::fts_index_update_failed(e, collection, surrogate.as_u32());
                })
        }
        // A tombstone, an erasure, or a body with no fields ends the row's
        // text at the version's ordinal.
        None => target.inverted.remove_document_in_txn(txn, doc_scope),
    }
}

/// Index the declared fields of one sync-frame version of `doc` at
/// `doc.ordinal` into `target`'s index, inside `txn`. Does NOT commit.
///
/// `fields` is every pair the frame carried, empty for a tombstone. A frame
/// that fills no declared field ends the document's text, as a tombstone
/// does.
pub(in crate::data::executor) fn replay_frame_into(
    target: ReplayTarget<'_>,
    txn: &WriteTransaction,
    doc: IndexDocScope<'_>,
    fields: &[(String, String)],
) -> crate::Result<()> {
    let text = declared_pairs(fields, target.text_fields);
    if text.fields().is_empty() {
        target.inverted.remove_document_in_txn(txn, doc)
    } else {
        target.inverted.index_document_in_txn(txn, doc, &text)
    }
}

impl CoreLoop {
    /// The live index of this core with the declared fields and config of
    /// `collection`.
    pub(in crate::data::executor) fn live_replay_target(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> ReplayTarget<'_> {
        ReplayTarget {
            inverted: &self.inverted,
            text_fields: self.declared_text_fields(database_id, tid, collection),
            config: self.doc_config(database_id, tid, collection),
        }
    }

    /// Index the declared fields of one sync-frame version of `doc` at
    /// `doc.ordinal`, inside `txn`. Does NOT commit.
    ///
    /// The sync write path, a rebuild and a restore all index a frame
    /// through [`replay_frame_into`], so they leave identical postings and
    /// identical text history.
    pub(in crate::data::executor) fn replay_frame_text(
        &self,
        txn: &WriteTransaction,
        doc: IndexDocScope<'_>,
        fields: &[(String, String)],
    ) -> crate::Result<()> {
        let target = self.live_replay_target(doc.database_id, doc.tid.as_u64(), doc.collection);
        replay_frame_into(target, txn, doc, fields)
    }

    /// Replay `version` into the live text index inside `txn`. Does NOT
    /// commit.
    ///
    /// `config` is the collection's registered config: a registered
    /// collection's live bodies must decode, and a body that does not fails
    /// the replay.
    pub(in crate::data::executor) fn replay_version_text(
        &self,
        txn: &WriteTransaction,
        config: Option<&CollectionConfig>,
        version: TextVersion<'_>,
    ) -> crate::Result<()> {
        let target = ReplayTarget {
            config,
            ..self.live_replay_target(version.database_id, version.tid, version.collection)
        };
        replay_version_into(target, txn, version)
    }
}
