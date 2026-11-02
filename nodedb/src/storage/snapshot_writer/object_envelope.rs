// SPDX-License-Identifier: BUSL-1.1

//! Authenticated framing for individual object-store snapshot objects.

use nodedb_wal::crypto::{AUTH_TAG_SIZE, SEGMENT_ENVELOPE_PREAMBLE_SIZE, WalEncryptionKey};

use crate::storage::segment::{
    SegmentFooter, decrypt_untrusted_segment_bytes, encrypt_untrusted_segment_bytes,
};
use crate::types::Lsn;

/// Hard pre-fetch ceiling for every untrusted snapshot object, including AEAD
/// envelope framing. This is deliberately lower than the generic segment
/// limit because object-store reads are fully buffered.
pub(super) const MAX_SNAPSHOT_OBJECT_BYTES: u64 = 256 * 1024 * 1024;

/// Room reserved in every object for the context, footer, preamble, and tag.
/// A chunk payload of at most `MAX_SNAPSHOT_OBJECT_BYTES - CHUNK_HEADROOM`
/// always fits one object.
pub(super) const CHUNK_HEADROOM: u64 = 64 * 1024;

pub(super) const SNAPSHOT_MANIFEST_KIND: u8 = 0;
/// A content-addressed chunk. Its context name is the chunk id.
pub(super) const SNAPSHOT_CHUNK_KIND: u8 = 1;

const SNAPSHOT_CONTEXT_MAGIC: [u8; 4] = *b"SNCT";
const SNAPSHOT_CONTEXT_VERSION: u8 = 1;
const SNAPSHOT_CONTEXT_FIXED_BYTES: usize = 4 + 1 + 1 + 2;

/// What one object is, bound into its authenticated payload so an object
/// moved to another name or kind fails to open.
///
/// A manifest's name is its snapshot prefix. A chunk's name is its id: the
/// chunk is shared by every base that lists it, so its context names no base.
#[derive(Debug, Clone, Copy)]
pub(super) struct ObjectContext<'a> {
    pub name: &'a str,
    pub kind: u8,
}

fn context_bytes(ctx: ObjectContext<'_>) -> crate::Result<Vec<u8>> {
    if !matches!(ctx.kind, SNAPSHOT_MANIFEST_KIND | SNAPSHOT_CHUNK_KIND) {
        return Err(crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("invalid snapshot object kind {}", ctx.kind),
        });
    }
    let name_len = u16::try_from(ctx.name.len()).map_err(|_| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: "snapshot object name is too long for object context".into(),
    })?;
    let mut context = Vec::with_capacity(SNAPSHOT_CONTEXT_FIXED_BYTES + ctx.name.len());
    context.extend_from_slice(&SNAPSHOT_CONTEXT_MAGIC);
    context.push(SNAPSHOT_CONTEXT_VERSION);
    context.push(ctx.kind);
    context.extend_from_slice(&name_len.to_le_bytes());
    context.extend_from_slice(ctx.name.as_bytes());
    Ok(context)
}

pub(super) fn encrypt_snapshot_object(
    bytes: &[u8],
    ctx: ObjectContext<'_>,
    node_name: &str,
    watermark: u64,
    key: &WalEncryptionKey,
) -> crate::Result<Vec<u8>> {
    let context = context_bytes(ctx)?;
    let payload_len =
        context
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: "snapshot object context length overflow".into(),
            })?;
    let envelope_len = payload_len
        .checked_add(SegmentFooter::size())
        .and_then(|size| size.checked_add(SEGMENT_ENVELOPE_PREAMBLE_SIZE))
        .and_then(|size| size.checked_add(AUTH_TAG_SIZE))
        .ok_or_else(|| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: "snapshot envelope length overflow".into(),
        })?;
    check_snapshot_object_size(
        u64::try_from(envelope_len).map_err(|_| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: "snapshot envelope length does not fit object metadata".into(),
        })?,
        "snapshot object",
    )?;

    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&context);
    payload.extend_from_slice(bytes);
    let lsn = Lsn::new(watermark);
    let footer = SegmentFooter::new(node_name, crc32c::crc32c(&payload), lsn, lsn);
    encrypt_untrusted_segment_bytes(&payload, &footer, key)
}

pub(super) fn decrypt_snapshot_object(
    raw: &[u8],
    ctx: ObjectContext<'_>,
    key: &WalEncryptionKey,
) -> crate::Result<Vec<u8>> {
    let expected_context = context_bytes(ctx)?;
    let payload = decrypt_untrusted_segment_bytes(raw, key)?;
    let content = payload
        .strip_prefix(expected_context.as_slice())
        .ok_or_else(|| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: "snapshot object context does not match requested object".into(),
        })?;
    Ok(content.to_vec())
}

pub(super) fn check_snapshot_object_size(size: u64, object_name: &str) -> crate::Result<()> {
    if size > MAX_SNAPSHOT_OBJECT_BYTES {
        return Err(crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("{object_name} exceeds snapshot object resource limit: {size} bytes"),
        });
    }
    Ok(())
}
