// SPDX-License-Identifier: Apache-2.0

//! Batch operation payload types.

use serde::{Deserialize, Serialize};

use crate::value::Value;

/// A single vector in a batch insert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchVector {
    pub id: String,
    pub embedding: Vec<f32>,
    /// The row's metadata: a `Value::Object`, each value tagged with its
    /// exact type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

impl zerompk::ToMessagePack for BatchVector {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(3)?;
        writer.write_string(&self.id)?;
        self.embedding.write(writer)?;
        self.metadata.write(writer)
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
        let metadata = Option::<Value>::read(reader)?;
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
    /// The document's fields: a `Value::Object`, each value tagged with its
    /// exact type.
    pub fields: Value,
}

impl zerompk::ToMessagePack for BatchDocument {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        writer.write_string(&self.id)?;
        self.fields.write(writer)
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
        let fields = Value::read(reader)?;
        Ok(Self { id, fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_vector_metadata_keeps_exact_types() {
        let metadata = Value::Object(
            [
                (
                    "price".to_string(),
                    Value::Decimal("12.50".parse().expect("decimal literal")),
                ),
                (
                    "uid".to_string(),
                    Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
                ),
                ("blob".to_string(), Value::Bytes(vec![0x01, 0xff])),
            ]
            .into(),
        );
        let vector = BatchVector {
            id: "v1".into(),
            embedding: vec![0.5, 1.0],
            metadata: Some(metadata.clone()),
        };
        let bytes = zerompk::to_msgpack_vec(&vector).expect("encode");
        let decoded: BatchVector = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded.id, "v1");
        assert_eq!(decoded.embedding, vec![0.5, 1.0]);
        assert_eq!(decoded.metadata, Some(metadata));
        let Some(Value::Object(fields)) = decoded.metadata else {
            panic!("metadata decodes as an object");
        };
        let Some(Value::Decimal(price)) = fields.get("price") else {
            panic!("price stays a Decimal");
        };
        assert_eq!(price.to_string(), "12.50");
    }

    #[test]
    fn batch_document_fields_keep_exact_types() {
        let fields = Value::Object(
            [
                (
                    "price".to_string(),
                    Value::Decimal("12.50".parse().expect("decimal literal")),
                ),
                (
                    "at".to_string(),
                    Value::DateTime(crate::datetime::NdbDateTime::from_micros(1_700_000_000)),
                ),
            ]
            .into(),
        );
        let doc = BatchDocument {
            id: "d1".into(),
            fields: fields.clone(),
        };
        let bytes = zerompk::to_msgpack_vec(&doc).expect("encode");
        let decoded: BatchDocument = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded.id, "d1");
        assert_eq!(decoded.fields, fields);
    }
}
