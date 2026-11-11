// SPDX-License-Identifier: Apache-2.0

//! Size limits on one stored row and on the descriptor its images depend on.
//!
//! A write-set part journals each stored row in one WAL record. These limits
//! bound every field of that record: the row's images and id, the collection
//! name it carries, the schema witness it references, and the growth a row
//! stored under an earlier schema version shows when it renders under a
//! later one.

use crate::columnar::StrictSchema;

/// The largest image of one stored row, in bytes: a document's accepted
/// body, its stored body, its new public event image, its row id, or an
/// edge's properties, endpoints or label.
///
/// A write that stores a larger image is refused before it installs, so
/// every node journals each stored row in one WAL record.
pub const MAX_ROW_IMAGE_BYTES: usize = 8 * 1024 * 1024;

/// The longest collection name, in bytes. A WAL collection tombstone holds
/// at most this many bytes of name too.
pub const MAX_COLLECTION_NAME_BYTES: usize = 255;

/// The largest encoded schema witness of a strict collection, in bytes: the
/// MessagePack of its storage mode, as a write-set part carries it.
pub const MAX_SCHEMA_WITNESS_BYTES: usize = 16 * 1024 * 1024;

/// The most bytes the columns `ALTER ADD COLUMN` added to a strict schema
/// can add to the image of a row stored before them. Such a row renders
/// every added column with its default, or `NULL`.
pub const MAX_ADDED_COLUMN_IMAGE_BYTES: usize = 1024 * 1024;

/// The largest prior event image of a row an update journals: an image at
/// [`MAX_ROW_IMAGE_BYTES`] when it was stored, with every column added since.
pub const MAX_OLD_EVENT_IMAGE_BYTES: usize = MAX_ROW_IMAGE_BYTES + MAX_ADDED_COLUMN_IMAGE_BYTES;

/// Bytes one added column costs a rendered row beyond its name and its
/// default text, at most: a `str 32` key header, the value's header and
/// payload past its text, and a map header that widens.
const ADDED_COLUMN_OVERHEAD: usize = 16;

/// The most bytes the columns added after version 1 of `schema` add to the
/// rendered image of a row stored under version 1.
///
/// A default is a literal: a quoted string renders as its text, a number
/// within 9 bytes, `TRUE`, `FALSE` and `NULL` within 1. No default renders
/// as `NULL`.
pub fn added_column_image_bytes(schema: &StrictSchema) -> usize {
    schema
        .columns
        .iter()
        .filter(|column| column.added_at_version > 1)
        .map(|column| {
            column
                .name
                .len()
                .saturating_add(column.default.as_deref().map_or(0, str::len))
                .saturating_add(ADDED_COLUMN_OVERHEAD)
        })
        .fold(0usize, usize::saturating_add)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::{ColumnDef, ColumnType};
    use crate::value::Value;

    /// A row stored before an added column renders it within the bound.
    #[test]
    fn an_added_default_renders_within_its_bound() {
        for default in [
            "'a quoted ''default'' text'",
            "-9223372036854775808",
            "1.5e300",
            "TRUE",
        ] {
            let mut column = ColumnDef::nullable("added_column", ColumnType::String);
            column.default = Some(default.to_string());
            column.added_at_version = 2;
            let mut schema = StrictSchema::new(vec![ColumnDef::required("id", ColumnType::Int64)])
                .expect("schema");
            schema.columns.push(column);
            schema.version = 2;
            let base: std::collections::HashMap<String, Value> =
                [("id".to_string(), Value::Integer(1))].into();
            let mut grown = base.clone();
            grown.insert(
                "added_column".to_string(),
                StrictSchema::parse_default_literal(default),
            );
            let before = crate::value_to_msgpack(&Value::Object(base)).expect("encode");
            let after = crate::value_to_msgpack(&Value::Object(grown)).expect("encode");
            assert!(
                after.len() - before.len() <= added_column_image_bytes(&schema),
                "{default}: {} bytes over {}",
                after.len() - before.len(),
                added_column_image_bytes(&schema)
            );
        }
    }
}
