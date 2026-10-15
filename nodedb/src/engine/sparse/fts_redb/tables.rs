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
