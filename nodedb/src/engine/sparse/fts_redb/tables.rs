// SPDX-License-Identifier: BUSL-1.1

//! Redb table definitions for the full-text search backend.
//!
//! Every table is keyed by a structural
//! `(database_id, tenant_id, collection, field, …)` tuple — matching the
//! EdgeStore pattern. `field` is the index's field key: empty for the
//! collection's whole-document index, the field name for a field index.
//! Per-`(database, tenant)` and per-collection drops become range scans over
//! the tuple instead of fragile lexical-prefix scans. Keys are formed only in
//! the sibling `keys` module.

use redb::TableDefinition;

/// Inverted index, one row per document per term: key =
/// `(database_id, tenant_id, collection, field, term, surrogate)`, value =
/// MessagePack-encoded `(term_freq, positions)` of that document's posting.
/// A term's posting list is the range of rows sharing its key prefix, in
/// surrogate order. Indexing or removing one document writes one row per
/// term, whatever the list length.
pub const POSTINGS: TableDefinition<PostingRowKey, &[u8]> = TableDefinition::new("text.postings");

/// `POSTINGS` key: `(database_id, tenant_id, collection, field, term, surrogate)`.
pub type PostingRowKey = (u64, u64, &'static str, &'static str, &'static str, u32);

/// Document lengths: key = `(database_id, tenant_id, collection, field, surrogate)`,
/// value = MessagePack-encoded `u32` token count.
/// The surrogate is stored as its raw `u32` value (redb native key type).
pub const DOC_LENGTHS: TableDefinition<(u64, u64, &str, &str, u32), &[u8]> =
    TableDefinition::new("text.doc_lengths");

/// Per-document indexed term set: key =
/// `(database_id, tenant_id, collection, field, surrogate)`, value =
/// MessagePack-encoded `Vec<String>` holding the distinct analyzed terms the
/// document currently contributes a posting to in that index.
///
/// This is what makes a re-index able to SUBTRACT. A re-index only sees the
/// new text's terms; without a record of the previous version's terms it
/// cannot tell which posting lists the document must be dropped from, so
/// words removed by an update keep matching the document forever. Keyed
/// exactly like `DOC_LENGTHS` so both are read in the same lookup pattern.
pub const DOC_TERMS: TableDefinition<(u64, u64, &str, &str, u32), &[u8]> =
    TableDefinition::new("text.doc_terms");

/// Field indexes a document occupies: key =
/// `(database_id, tenant_id, collection, surrogate)`, value = MessagePack
/// `Vec<String>` of non-empty field names. A re-index retracts the document
/// from every field index listed here that its new text no longer fills.
pub const DOC_FIELDS: TableDefinition<(u64, u64, &str, u32), &[u8]> =
    TableDefinition::new("text.doc_fields");

/// Index metadata blobs: key = `(database_id, tenant_id, collection, field, sub_key)`,
/// value = opaque blob. Sub-keys: `"fieldnorms"` per index; `"analyzer"`,
/// `"language"`, `"fuzzy"` under the whole-document index.
pub const INDEX_META: TableDefinition<(u64, u64, &str, &str, &str), &[u8]> =
    TableDefinition::new("text.meta");

/// Corpus stats: key = `(database_id, tenant_id, collection, field)`,
/// value = MessagePack-encoded `(doc_count, total_token_sum)`.
pub const STATS: TableDefinition<(u64, u64, &str, &str), &[u8]> =
    TableDefinition::new("text.stats");

/// Segment blobs: key = `(database_id, tenant_id, collection, field, segment_id)`,
/// value = compressed segment bytes. `segment_id` format `"L{level}:{id:016x}"`.
pub const SEGMENTS: TableDefinition<(u64, u64, &str, &str, &str), &[u8]> =
    TableDefinition::new("text.segments");

/// System-time start of a document's indexed version a retained snapshot
/// can read below: key = `(database_id, tenant_id, collection, field,
/// surrogate)`, value = the `sys_from` ordinal of the write that indexed it.
/// A live document without a row here is visible at every retained cut.
pub const TEXT_LIVE_FROM: TableDefinition<(u64, u64, &str, &str, u32), i64> =
    TableDefinition::new("text.live_from");

/// `TEXT_LIVE_FROM` ordered by ordinal: key = `(database_id, tenant_id,
/// collection, field, sys_from, surrogate)`, value unused. A read at a cut
/// ranges over the documents indexed after it.
pub const TEXT_LIVE_BY_FROM: TableDefinition<(u64, u64, &str, &str, i64, u32), u8> =
    TableDefinition::new("text.live_by_from");

/// The key of one ended text version: `(database_id, tenant_id, collection,
/// field, sys_to, surrogate, sys_from)`.
pub type TextEndedKey = (u64, u64, &'static str, &'static str, i64, u32, i64);

/// Ended document versions a retained snapshot can read, keyed by
/// [`TextEndedKey`]. The value is the MessagePack `Vec<String>` of the
/// version's analyzed tokens in order. A read at a cut ranges over the
/// versions ended after it. A purge ranges over the versions ended at or
/// below its horizon.
pub const TEXT_ENDED: TableDefinition<TextEndedKey, &[u8]> = TableDefinition::new("text.ended");
