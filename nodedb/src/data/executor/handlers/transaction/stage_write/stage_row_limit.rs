// SPDX-License-Identifier: BUSL-1.1

//! The row image limit a staged document row passes before it enters the
//! overlay.
//!
//! The commit install journals each row's stored body and its public event
//! image, so the stage holds both to the limit. A session statement refuses
//! there, before COMMIT. A Calvin stage refuses before its vote, so the
//! transaction votes abort and nothing of it installs.
//!
//! Rendering the event image costs a decode and an encode, so the stage
//! renders only a body that can render past the limit. A stored body
//! renders to at most [`RENDER_GROWTH`] times its length, plus a constant
//! per row:
//!
//! - A strict Binary Tuple column renders its value and its name. A fixed
//!   value grows at most from 16 bytes to 38 (a UUID as text), from 8 to 18
//!   (a duration as text) and from 4 to 9 per vector element. A variable
//!   value keeps its bytes, or grows from tagged MessagePack to plain at
//!   most from 4 bytes to 10 (an instant ext). Each column adds its name and
//!   at most [`RENDER_HEADERS`] header bytes.
//! - A schemaless body is plain or tagged MessagePack. Plain stays within
//!   1.8 times (a `float 32` as `float 64`) and tagged within 2.5 times. The
//!   identity column it gains adds its name, the identity and headers.

use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::columnar::StrictSchema;
use nodedb_types::{MAX_ROW_IMAGE_BYTES, RowIdentity};

use super::context::StageCtx;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::row_image_limit::{check_row_image, check_row_images};
use crate::data::executor::strict_format::strict_row_to_msgpack;
use crate::types::{DatabaseId, TenantId};

/// How many times its length a stored body renders to, at most.
const RENDER_GROWTH: usize = 3;

/// Header bytes one rendered field or row adds beyond its name and value:
/// a `str 32` key header, a value header up to an ext's, a widened map
/// header.
const RENDER_HEADERS: usize = 16;

/// Whether a stored body of `stored_len` bytes renders within the row limit
/// for certain, given the row's constant render cost.
fn renders_within_limit(stored_len: usize, constant: usize) -> bool {
    stored_len
        .saturating_mul(RENDER_GROWTH)
        .saturating_add(constant)
        <= MAX_ROW_IMAGE_BYTES
}

/// The constant render cost of a row of `schema`: every column's name and
/// headers.
fn strict_render_constant(schema: &StrictSchema) -> usize {
    schema
        .columns
        .iter()
        .map(|column| column.name.len().saturating_add(RENDER_HEADERS))
        .fold(RENDER_HEADERS, usize::saturating_add)
}

/// The constant render cost of the identity a schemaless row gains.
fn identity_render_constant(identity_column: &str, identity: &str) -> usize {
    identity_column
        .len()
        .saturating_add(identity.len())
        .saturating_add(RENDER_HEADERS)
}

impl CoreLoop {
    /// [`Self::stage_check_row_image`] for the row `ctx` stages.
    pub(super) fn stage_check_ctx_row_image(
        &self,
        ctx: &StageCtx<'_>,
        stored: &[u8],
    ) -> crate::Result<()> {
        self.stage_check_row_image(
            ctx.database_id,
            ctx.tid,
            ctx.collection,
            &ctx.document_id,
            stored,
        )
    }

    /// Refuse a staged row of `collection` whose id, stored body or public
    /// event image is over the row image limit.
    ///
    /// The event image renders as the install renders it: a strict Binary
    /// Tuple as MessagePack, a schemaless body with its identity column. A
    /// body the growth bound keeps within the limit is not rendered. A body
    /// that does not render has no image to measure here. The install
    /// refuses it with its own error.
    pub(in crate::data::executor) fn stage_check_row_image(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        identity: &RowIdentity,
        stored: &[u8],
    ) -> crate::Result<()> {
        check_row_images(collection, identity.as_str(), &[stored])?;
        let config_key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        let config = self.doc_configs.get(&config_key);
        let strict = config.and_then(|config| match &config.storage_mode {
            StorageMode::Strict { schema } => Some(schema),
            StorageMode::Schemaless => None,
        });
        let identity_column = config
            .and_then(|config| config.declared_key.as_deref())
            .unwrap_or(nodedb_types::DEFAULT_IDENTITY_COLUMN);
        let constant = identity_render_constant(identity_column, identity.as_str())
            .saturating_add(strict.map_or(0, strict_render_constant));
        if renders_within_limit(stored.len(), constant) {
            return Ok(());
        }
        let rendered = match strict.map(|schema| strict_row_to_msgpack(stored, schema)) {
            Some(Ok(Some(image))) => Some(image),
            Some(Err(_)) => None,
            Some(Ok(None)) | None => self
                .body_event_image(database_id, tid, collection, identity.as_str(), stored)
                .ok(),
        };
        match rendered {
            Some(image) => check_row_image(collection, identity.as_str(), image.len()),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::columnar::{ColumnDef, ColumnType};
    use nodedb_types::{NdbDateTime, NdbDuration, Value};

    use super::*;
    use crate::data::executor::doc_format::decode_document_value;
    use crate::data::executor::strict_format::value_to_binary_tuple;

    #[test]
    fn the_skip_holds_up_to_the_boundary_and_renders_past_it() {
        let constant = 4_321;
        let edge = (MAX_ROW_IMAGE_BYTES - constant) / RENDER_GROWTH;
        assert!(renders_within_limit(edge, constant));
        assert!(!renders_within_limit(edge + 1, constant));
        assert!(!renders_within_limit(usize::MAX, constant));
    }

    /// The fields that grow most when they render.
    fn growing_fields() -> Vec<(&'static str, Value)> {
        vec![
            (
                "u",
                Value::Uuid("01234567-89ab-cdef-0123-456789abcdef".into()),
            ),
            ("d", Value::Duration(NdbDuration::from_micros(i64::MIN + 1))),
            ("t", Value::DateTime(NdbDateTime::from_micros(0))),
            ("v", Value::Array(vec![Value::Float(1.5); 16])),
            (
                "a",
                Value::Array(vec![Value::DateTime(NdbDateTime::from_micros(0)); 64]),
            ),
            ("s", Value::String(String::new())),
        ]
    }

    fn row(fields: &[(&str, Value)]) -> Value {
        Value::Object(
            fields
                .iter()
                .map(|(name, value)| (name.to_string(), value.clone()))
                .collect::<HashMap<_, _>>(),
        )
    }

    #[test]
    fn a_strict_tuple_renders_within_the_growth_bound() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("u", ColumnType::Uuid),
            ColumnDef::nullable("d", ColumnType::Duration),
            ColumnDef::nullable("t", ColumnType::Timestamptz),
            ColumnDef::nullable("v", ColumnType::Vector(16)),
            ColumnDef::nullable("a", ColumnType::Array),
            ColumnDef::nullable("s", ColumnType::String),
            ColumnDef::nullable("absent", ColumnType::String),
        ])
        .expect("strict schema");
        let tuple =
            value_to_binary_tuple(&row(&growing_fields()), &schema, "c").expect("row encodes");
        let image = strict_row_to_msgpack(&tuple, &schema)
            .expect("tuple renders")
            .expect("a tuple converts");
        assert!(
            image.len() <= tuple.len() * RENDER_GROWTH + strict_render_constant(&schema),
            "{} bytes from a tuple of {}",
            image.len(),
            tuple.len()
        );
    }

    #[test]
    fn a_tagged_schemaless_body_renders_within_the_growth_bound() {
        let body = zerompk::to_msgpack_vec(&row(&growing_fields())).expect("tagged body");
        let mut value = decode_document_value(&body).expect("body decodes");
        if let Value::Object(fields) = &mut value {
            fields.insert("id".into(), Value::String("row-1".into()));
        }
        let image = nodedb_types::value_to_msgpack(&value).expect("image");
        assert!(
            image.len() <= body.len() * RENDER_GROWTH + identity_render_constant("id", "row-1"),
            "{} bytes from a body of {}",
            image.len(),
            body.len()
        );
    }
}
