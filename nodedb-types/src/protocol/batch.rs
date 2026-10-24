// SPDX-License-Identifier: Apache-2.0

//! Batch operation payload types.

use serde::{Deserialize, Serialize};

use crate::json_msgpack::JsonValue;

/// Most edges one batch statement may carry.
///
/// A protocol-level bound, so the client and the statement parser agree by
/// construction rather than by two copies of the number happening to match.
/// The server enforces it; the client checks it first so an oversized batch
/// fails with the limit named instead of a round trip and a parse error.
pub const MAX_EDGES_PER_BATCH_STATEMENT: usize = 1000;

/// Most SQL text one batch statement may carry, in bytes.
///
/// The edge count alone does not bound the work: a triple's fields are
/// arbitrary quoted strings, so a handful of edges with long node ids is far
/// larger than a thousand short ones. Whichever limit the statement exceeds
/// first is the one reported.
pub const MAX_BATCH_TEXT_BYTES: usize = 1 << 20;

/// A single vector in a batch insert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchVector {
    pub id: String,
    pub embedding: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl zerompk::ToMessagePack for BatchVector {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(3)?;
        writer.write_string(&self.id)?;
        self.embedding.write(writer)?;
        self.metadata
            .as_ref()
            .map(|v| JsonValue(v.clone()))
            .write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for BatchVector {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != 3 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 3,
                actual: len,
            });
        }
        let id = reader.read_string()?.into_owned();
        let embedding = Vec::<f32>::read(reader)?;
        let metadata = Option::<JsonValue>::read(reader)?.map(|v| v.0);
        Ok(Self {
            id,
            embedding,
            metadata,
        })
    }
}

/// A single document in a batch insert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchDocument {
    pub id: String,
    pub fields: serde_json::Value,
}

impl zerompk::ToMessagePack for BatchDocument {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        writer.write_string(&self.id)?;
        JsonValue(self.fields.clone()).write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for BatchDocument {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != 2 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 2,
                actual: len,
            });
        }
        let id = reader.read_string()?.into_owned();
        let fields = JsonValue::read(reader)?.0;
        Ok(Self { id, fields })
    }
}
