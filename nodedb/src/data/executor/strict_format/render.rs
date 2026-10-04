// SPDX-License-Identifier: BUSL-1.1

//! Render a stored strict row as standard MessagePack, naming why it failed.
//!
//! A strict collection stores Binary Tuples. A MessagePack map stored before
//! the collection became strict is read as it is. Any other stored form is
//! corrupt: the image readers (the Event Plane, the write-set journal) never
//! receive it, so each failure carries the class that names its cause.

use nodedb_query::msgpack_scan::reader::skip_value;
use nodedb_types::columnar::StrictSchema;

use super::decode::binary_tuple_to_value;

/// Why a stored strict row did not render as MessagePack. Each class names
/// one root cause, never a row, so a report groups by it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StrictImageFault {
    /// The bytes are neither a Binary Tuple header nor one well-formed
    /// MessagePack map: a truncated or overwritten body.
    NotATuple,
    /// The tuple names schema version 0, or a version newer than the schema
    /// this core holds for the collection.
    SchemaVersion,
    /// The tuple header is valid but its column layout does not decode.
    Layout,
    /// The decoded row did not encode as MessagePack.
    Encode,
}

impl StrictImageFault {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NotATuple => "not_a_tuple",
            Self::SchemaVersion => "schema_version",
            Self::Layout => "layout",
            Self::Encode => "encode",
        }
    }
}

/// The MessagePack image of `stored`, a row of a strict collection.
///
/// `Ok(None)` when `stored` is one well-formed MessagePack map already, so
/// the stored bytes are the image. `Ok(Some(_))` for a Binary Tuple decoded
/// against `schema`.
pub(crate) fn strict_row_to_msgpack(
    stored: &[u8],
    schema: &StrictSchema,
) -> Result<Option<Vec<u8>>, StrictImageFault> {
    let is_map_header = stored
        .first()
        .is_some_and(|&first| (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF);
    if is_map_header {
        return match skip_value(stored, 0) {
            Some(end) if end == stored.len() => Ok(None),
            _ => Err(StrictImageFault::NotATuple),
        };
    }
    let version = nodedb_strict::TupleDecoder::new(schema)
        .schema_version(stored)
        .map_err(|_| StrictImageFault::NotATuple)?;
    if version == 0 || version > schema.version {
        return Err(StrictImageFault::SchemaVersion);
    }
    let value = binary_tuple_to_value(stored, schema).ok_or(StrictImageFault::Layout)?;
    nodedb_types::value_to_msgpack(&value)
        .map(Some)
        .map_err(|_| StrictImageFault::Encode)
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType};
    use nodedb_types::value::Value;

    use super::*;
    use crate::data::executor::strict_format::value_to_binary_tuple;

    fn schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::String),
            ColumnDef::nullable("name", ColumnType::String),
        ])
        .expect("valid schema")
    }

    fn tuple() -> Vec<u8> {
        let row = Value::Object(
            [
                ("id".to_string(), Value::String("r1".into())),
                ("name".to_string(), Value::String("alice".into())),
            ]
            .into_iter()
            .collect(),
        );
        value_to_binary_tuple(&row, &schema(), "c").expect("row encodes")
    }

    #[test]
    fn tuple_renders_as_msgpack_map() {
        let image = strict_row_to_msgpack(&tuple(), &schema())
            .expect("tuple decodes")
            .expect("a tuple converts");
        let value = nodedb_types::value_from_msgpack(&image).expect("image is msgpack");
        assert_eq!(
            value
                .as_object()
                .and_then(|o| o.get("name"))
                .and_then(|v| v.as_str()),
            Some("alice")
        );
    }

    #[test]
    fn stored_msgpack_map_is_its_own_image() {
        let map = nodedb_types::value_to_msgpack(&Value::Object(
            [("id".to_string(), Value::String("r1".into()))]
                .into_iter()
                .collect(),
        ))
        .expect("encode");
        assert_eq!(strict_row_to_msgpack(&map, &schema()), Ok(None));
    }

    #[test]
    fn corrupt_rows_name_their_fault() {
        let schema = schema();
        assert_eq!(
            strict_row_to_msgpack(b"garbage bytes", &schema),
            Err(StrictImageFault::NotATuple)
        );
        // A map header whose body is cut short is no map.
        assert_eq!(
            strict_row_to_msgpack(&[0x81, 0xa2, b'i'], &schema),
            Err(StrictImageFault::NotATuple)
        );
        let mut newer = tuple();
        newer[5..9].copy_from_slice(&(schema.version + 1).to_le_bytes());
        assert_eq!(
            strict_row_to_msgpack(&newer, &schema),
            Err(StrictImageFault::SchemaVersion)
        );
        let full = tuple();
        let truncated = &full[..full.len() - 3];
        assert_eq!(
            strict_row_to_msgpack(truncated, &schema),
            Err(StrictImageFault::Layout)
        );
    }
}
