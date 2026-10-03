// SPDX-License-Identifier: Apache-2.0

//! WAL payload structs for FTS and Spatial sync records.
//!
//! Payload layout: variable-length LE encoding (no serde/msgpack dep).
//! Each payload carries the engine-specific fields plus the four provenance
//! fields (`producer_id`, `epoch`, `stream_id`, `seq`) inline so deduplication
//! never requires a separate decode step.
//!
//! Wire layout for all four payload types:
//!
//! ```text
//! ┌──────────────┬──────────┬───────────┬──────────┬──────────────┬──────────────┬────────────────────┐
//! │producer_id u64│ epoch u64│stream_id u64│  seq u64 │ name_len u32 │ collection   │ id_len u32 + id... │
//! └──────────────┴──────────┴───────────┴──────────┴──────────────┴──────────────┴────────────────────┘
//! ```
//! FtsIndex additionally carries `field_count u32` followed by
//! `field_len u32 + field bytes + text_len u32 + text bytes` per field.
//! SpatialPut additionally carries `field_len u32 + field bytes + geometry_len u32 + geometry bytes`.
//! SpatialDelete additionally carries `field_len u32 + field bytes`.
//! FtsDelete and SpatialDelete prefix the id with a presence tag `u8`:
//! `0` means the key's home binds no row and no id follows, `1` means the id
//! follows.
//!
//! These structs are net-new (no legacy records to stay compatible with).
//! Field set can be extended when the handler is wired; the length-prefixed
//! layout is forward-compatible.

use nodedb_types::sync::wire::SyncProvenance;

use crate::error::{Result, WalError};

// ── helpers ──────────────────────────────────────────────────────────────────

fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32> {
    buf.get(offset..offset + 4)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        .ok_or_else(|| WalError::InvalidPayload {
            detail: format!("truncated at offset {offset}, need 4 bytes"),
        })
}

fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64> {
    buf.get(offset..offset + 8)
        .map(|b| u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
        .ok_or_else(|| WalError::InvalidPayload {
            detail: format!("truncated at offset {offset}, need 8 bytes"),
        })
}

fn read_utf8_field(buf: &[u8], offset: usize) -> Result<(String, usize)> {
    let len = read_u32_le(buf, offset)? as usize;
    let start = offset + 4;
    buf.get(start..start + len)
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| (s.to_string(), start + len))
        .ok_or_else(|| WalError::InvalidPayload {
            detail: format!("invalid utf8 field at offset {offset}"),
        })
}

fn read_bytes_field(buf: &[u8], offset: usize) -> Result<(Vec<u8>, usize)> {
    let len = read_u32_le(buf, offset)? as usize;
    let start = offset + 4;
    buf.get(start..start + len)
        .map(|b| (b.to_vec(), start + len))
        .ok_or_else(|| WalError::InvalidPayload {
            detail: format!("truncated bytes field at offset {offset}"),
        })
}

fn push_u32_le(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn push_u64_le(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn push_str_field(buf: &mut Vec<u8>, s: &str) -> Result<()> {
    let bytes = s.as_bytes();
    if bytes.len() > u32::MAX as usize {
        return Err(WalError::InvalidPayload {
            detail: format!("string field too long: {} bytes", bytes.len()),
        });
    }
    push_u32_le(buf, bytes.len() as u32);
    buf.extend_from_slice(bytes);
    Ok(())
}

/// Presence tag of an optional string field: absent.
const FIELD_ABSENT: u8 = 0;
/// Presence tag of an optional string field: present, followed by the field.
const FIELD_PRESENT: u8 = 1;

fn push_opt_str_field(buf: &mut Vec<u8>, s: Option<&str>) -> Result<()> {
    match s {
        None => {
            buf.push(FIELD_ABSENT);
            Ok(())
        }
        Some(s) => {
            buf.push(FIELD_PRESENT);
            push_str_field(buf, s)
        }
    }
}

fn read_opt_utf8_field(buf: &[u8], offset: usize) -> Result<(Option<String>, usize)> {
    match buf.get(offset).copied() {
        Some(FIELD_ABSENT) => Ok((None, offset + 1)),
        Some(FIELD_PRESENT) => {
            let (s, next) = read_utf8_field(buf, offset + 1)?;
            Ok((Some(s), next))
        }
        Some(tag) => Err(WalError::InvalidPayload {
            detail: format!("optional field at offset {offset} has unknown presence tag {tag}"),
        }),
        None => Err(WalError::InvalidPayload {
            detail: format!("truncated at offset {offset}, need a presence tag"),
        }),
    }
}

fn push_bytes_field(buf: &mut Vec<u8>, data: &[u8]) -> Result<()> {
    if data.len() > u32::MAX as usize {
        return Err(WalError::InvalidPayload {
            detail: format!("bytes field too long: {} bytes", data.len()),
        });
    }
    push_u32_le(buf, data.len() as u32);
    buf.extend_from_slice(data);
    Ok(())
}

// ── common provenance decode/encode helpers ───────────────────────────────────

fn read_provenance(buf: &[u8]) -> Result<(SyncProvenance, usize)> {
    let producer_id = read_u64_le(buf, 0)?;
    let epoch = read_u64_le(buf, 8)?;
    let stream_id = read_u64_le(buf, 16)?;
    let seq = read_u64_le(buf, 24)?;
    Ok((
        SyncProvenance {
            producer_id,
            epoch,
            stream_id,
            seq,
        },
        32,
    ))
}

fn push_provenance(buf: &mut Vec<u8>, prov: &SyncProvenance) {
    push_u64_le(buf, prov.producer_id);
    push_u64_le(buf, prov.epoch);
    push_u64_le(buf, prov.stream_id);
    push_u64_le(buf, prov.seq);
}

// ── FtsIndexPayload ────────────────────────────────────────────────────────────

/// WAL payload for `RecordType::FtsIndex`.
///
/// Carries the minimum fields needed for Data-Plane replay: the collection
/// name, document identifier, the document's `(field, text)` pairs, and
/// producer provenance for idempotency checks. Empty `fields` remove the
/// document from every index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsIndexPayload {
    /// Producer provenance for idempotency checks.
    pub provenance: SyncProvenance,
    /// Target collection name.
    pub collection: String,
    /// External document identifier.
    pub doc_id: String,
    /// `(field, text)` per top-level string field.
    pub fields: Vec<(String, String)>,
}

/// Smallest encoding of one `(field, text)` pair: two empty length-prefixed
/// strings.
const MIN_FIELD_PAIR_BYTES: usize = 8;

impl FtsIndexPayload {
    pub fn new(
        provenance: SyncProvenance,
        collection: impl Into<String>,
        doc_id: impl Into<String>,
        fields: Vec<(String, String)>,
    ) -> Self {
        Self {
            provenance,
            collection: collection.into(),
            doc_id: doc_id.into(),
            fields,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        push_provenance(&mut buf, &self.provenance);
        push_str_field(&mut buf, &self.collection)?;
        push_str_field(&mut buf, &self.doc_id)?;
        let count = u32::try_from(self.fields.len()).map_err(|_| WalError::InvalidPayload {
            detail: format!("too many FTS fields: {}", self.fields.len()),
        })?;
        push_u32_le(&mut buf, count);
        for (field, text) in &self.fields {
            push_str_field(&mut buf, field)?;
            push_str_field(&mut buf, text)?;
        }
        Ok(buf)
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let (provenance, mut off) = read_provenance(buf)?;
        let (collection, next) = read_utf8_field(buf, off)?;
        off = next;
        let (doc_id, next) = read_utf8_field(buf, off)?;
        off = next;
        let count = read_u32_le(buf, off)? as usize;
        off += 4;
        let remaining = buf.len().saturating_sub(off);
        if count > remaining / MIN_FIELD_PAIR_BYTES {
            return Err(WalError::InvalidPayload {
                detail: format!(
                    "FTS field count {count} exceeds what {remaining} remaining bytes can hold"
                ),
            });
        }
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            let (field, next) = read_utf8_field(buf, off)?;
            let (text, next) = read_utf8_field(buf, next)?;
            off = next;
            fields.push((field, text));
        }
        Ok(Self {
            provenance,
            collection,
            doc_id,
            fields,
        })
    }
}

// ── FtsDeletePayload ───────────────────────────────────────────────────────────

/// WAL payload for `RecordType::FtsDelete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsDeletePayload {
    pub provenance: SyncProvenance,
    pub collection: String,
    /// The deleted document's identifier. `None` when the key's home binds
    /// no row: the delete removes nothing and still commits its provenance.
    pub doc_id: Option<String>,
}

impl FtsDeletePayload {
    pub fn new(
        provenance: SyncProvenance,
        collection: impl Into<String>,
        doc_id: Option<String>,
    ) -> Self {
        Self {
            provenance,
            collection: collection.into(),
            doc_id,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        push_provenance(&mut buf, &self.provenance);
        push_str_field(&mut buf, &self.collection)?;
        push_opt_str_field(&mut buf, self.doc_id.as_deref())?;
        Ok(buf)
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let (provenance, mut off) = read_provenance(buf)?;
        let (collection, next) = read_utf8_field(buf, off)?;
        off = next;
        let (doc_id, _) = read_opt_utf8_field(buf, off)?;
        Ok(Self {
            provenance,
            collection,
            doc_id,
        })
    }
}

// ── SpatialPutPayload ──────────────────────────────────────────────────────────

/// WAL payload for `RecordType::SpatialPut`.
///
/// `geometry_bytes` is a MessagePack-serialised `nodedb_types::geometry::Geometry`
/// value (the same encoding used in `SpatialInsertMsg`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpatialPutPayload {
    pub provenance: SyncProvenance,
    pub collection: String,
    pub field: String,
    pub doc_id: String,
    pub geometry_bytes: Vec<u8>,
}

impl SpatialPutPayload {
    pub fn new(
        provenance: SyncProvenance,
        collection: impl Into<String>,
        field: impl Into<String>,
        doc_id: impl Into<String>,
        geometry_bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            provenance,
            collection: collection.into(),
            field: field.into(),
            doc_id: doc_id.into(),
            geometry_bytes: geometry_bytes.into(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        push_provenance(&mut buf, &self.provenance);
        push_str_field(&mut buf, &self.collection)?;
        push_str_field(&mut buf, &self.field)?;
        push_str_field(&mut buf, &self.doc_id)?;
        push_bytes_field(&mut buf, &self.geometry_bytes)?;
        Ok(buf)
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let (provenance, mut off) = read_provenance(buf)?;
        let (collection, next) = read_utf8_field(buf, off)?;
        off = next;
        let (field, next) = read_utf8_field(buf, off)?;
        off = next;
        let (doc_id, next) = read_utf8_field(buf, off)?;
        off = next;
        let (geometry_bytes, _) = read_bytes_field(buf, off)?;
        Ok(Self {
            provenance,
            collection,
            field,
            doc_id,
            geometry_bytes,
        })
    }
}

// ── SpatialDeletePayload ───────────────────────────────────────────────────────

/// WAL payload for `RecordType::SpatialDelete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpatialDeletePayload {
    pub provenance: SyncProvenance,
    pub collection: String,
    pub field: String,
    /// The deleted row's identifier. `None` when the key's home binds no
    /// row: the delete removes nothing and still commits its provenance.
    pub doc_id: Option<String>,
}

impl SpatialDeletePayload {
    pub fn new(
        provenance: SyncProvenance,
        collection: impl Into<String>,
        field: impl Into<String>,
        doc_id: Option<String>,
    ) -> Self {
        Self {
            provenance,
            collection: collection.into(),
            field: field.into(),
            doc_id,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        push_provenance(&mut buf, &self.provenance);
        push_str_field(&mut buf, &self.collection)?;
        push_str_field(&mut buf, &self.field)?;
        push_opt_str_field(&mut buf, self.doc_id.as_deref())?;
        Ok(buf)
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let (provenance, mut off) = read_provenance(buf)?;
        let (collection, next) = read_utf8_field(buf, off)?;
        off = next;
        let (field, next) = read_utf8_field(buf, off)?;
        off = next;
        let (doc_id, _) = read_opt_utf8_field(buf, off)?;
        Ok(Self {
            provenance,
            collection,
            field,
            doc_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prov(producer_id: u64, epoch: u64, stream_id: u64, seq: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id,
            epoch,
            stream_id,
            seq,
        }
    }

    fn pairs(fields: &[(&str, &str)]) -> Vec<(String, String)> {
        fields
            .iter()
            .map(|(f, t)| ((*f).to_string(), (*t).to_string()))
            .collect()
    }

    #[test]
    fn fts_index_roundtrip() {
        let p = FtsIndexPayload::new(
            prov(0xCAFE_BABE, 3, 7, 42),
            "articles",
            "doc-1",
            pairs(&[("body", "hello world"), ("title", "Rust"), ("empty", "")]),
        );
        let bytes = p.to_bytes().unwrap();
        assert_eq!(FtsIndexPayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn fts_index_no_fields_roundtrip() {
        let p = FtsIndexPayload::new(prov(0, 0, 0, 0), "c", "d", Vec::new());
        assert_eq!(
            FtsIndexPayload::from_bytes(&p.to_bytes().unwrap()).unwrap(),
            p
        );
    }

    #[test]
    fn fts_delete_roundtrip() {
        let p = FtsDeletePayload::new(prov(1, 2, 3, 4), "articles", Some("doc-99".to_string()));
        let bytes = p.to_bytes().unwrap();
        assert_eq!(FtsDeletePayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn fts_delete_of_an_unbound_key_roundtrip() {
        let p = FtsDeletePayload::new(prov(1, 2, 3, 4), "articles", None);
        let bytes = p.to_bytes().unwrap();
        assert_eq!(FtsDeletePayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn unknown_presence_tag_rejected() {
        let p = FtsDeletePayload::new(prov(1, 2, 3, 4), "articles", None);
        let mut bytes = p.to_bytes().unwrap();
        if let Some(tag) = bytes.last_mut() {
            *tag = 7;
        }
        assert!(FtsDeletePayload::from_bytes(&bytes).is_err());
    }

    #[test]
    fn spatial_put_roundtrip() {
        let p = SpatialPutPayload::new(
            prov(5, 6, 7, 8),
            "places",
            "loc",
            "poi-1",
            vec![0xDE, 0xAD, 0xBE, 0xEF],
        );
        let bytes = p.to_bytes().unwrap();
        assert_eq!(SpatialPutPayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn spatial_put_empty_geometry_roundtrip() {
        let p = SpatialPutPayload::new(prov(0, 0, 0, 0), "c", "f", "d", vec![]);
        assert_eq!(
            SpatialPutPayload::from_bytes(&p.to_bytes().unwrap()).unwrap(),
            p
        );
    }

    #[test]
    fn spatial_delete_roundtrip() {
        let p = SpatialDeletePayload::new(
            prov(9, 10, 11, 12),
            "places",
            "loc",
            Some("poi-1".to_string()),
        );
        let bytes = p.to_bytes().unwrap();
        assert_eq!(SpatialDeletePayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn spatial_delete_of_an_unbound_key_roundtrip() {
        let p = SpatialDeletePayload::new(prov(9, 10, 11, 12), "places", "loc", None);
        let bytes = p.to_bytes().unwrap();
        assert_eq!(SpatialDeletePayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn truncated_buf_rejected() {
        let p = FtsIndexPayload::new(prov(1, 2, 3, 4), "col", "id", pairs(&[("body", "text")]));
        let bytes = p.to_bytes().unwrap();
        // Truncated to just provenance — should fail on collection field.
        assert!(FtsIndexPayload::from_bytes(&bytes[..32]).is_err());
        // Truncated inside the last field's text.
        assert!(FtsIndexPayload::from_bytes(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn fts_index_field_count_past_the_buffer_is_rejected() {
        let p = FtsIndexPayload::new(prov(1, 2, 3, 4), "col", "id", Vec::new());
        let mut bytes = p.to_bytes().unwrap();
        let count_at = bytes.len() - 4;
        bytes[count_at..].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(FtsIndexPayload::from_bytes(&bytes).is_err());
    }
}
