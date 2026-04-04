//! Redb table definitions for the full-text search backend.

use redb::TableDefinition;

/// Inverted index table: key = "{collection}:{term}", value = MessagePack-encoded Vec<Posting>.
pub const POSTINGS: TableDefinition<&str, &[u8]> = TableDefinition::new("text.postings");

/// Document length table: key = "{collection}:{doc_id}", value = MessagePack-encoded u32.
pub const DOC_LENGTHS: TableDefinition<&str, &[u8]> = TableDefinition::new("text.doc_lengths");

/// Index metadata: key = name, value = MessagePack bytes.
pub const INDEX_META: TableDefinition<&str, &[u8]> = TableDefinition::new("text.meta");

/// Segment blobs: key = "{collection}:seg:{id}", value = compressed segment bytes.
pub const SEGMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("text.segments");
