// SPDX-License-Identifier: BUSL-1.1

//! A row with every image, its name and its schema witness at the limit
//! plans into one part at the WAL record limit, and the part's record fits
//! that limit.

use std::sync::Arc;

use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::RowIdentity;
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

use super::*;
use crate::bridge::envelope::{EdgeImage, RowVersion, WriteSetEntry};
use crate::control::server::wal_dispatch::{GroupOrigin, WriteSetTarget, plan_group_parts};
use crate::types::document_event::DocumentPutEvent;
use crate::types::hash_chain::ChainHead;
use crate::types::installed_document::InstalledDocument;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::{EdgePutRedo, WriteGroup, WriteGroupRecord};

/// An image of `len` bytes.
fn image(len: usize) -> Vec<u8> {
    vec![0xff; len]
}

/// A collection name at the limit, with the longest database qualifier.
fn longest_name() -> String {
    "c".repeat(MAX_COLLECTION_NAME_BYTES + QUALIFIER_BYTES)
}

fn target(collection: &str) -> WriteSetTarget<'_> {
    WriteSetTarget {
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        database_id: DatabaseId::DEFAULT,
        collection,
        origin: GroupOrigin { lsn: Lsn::new(7) },
    }
}

/// A strict schema whose encoded witness is close under the limit.
fn strict_at_limit() -> StorageMode {
    const NAME_BYTES: usize = 4096;
    let witness_bytes = |mode: &StorageMode| {
        zerompk::to_msgpack_vec(&vec![mode.clone()])
            .expect("encode witness")
            .len()
    };
    let column = |index: usize| {
        ColumnDef::nullable(
            format!("{index:08}{}", "n".repeat(NAME_BYTES)),
            ColumnType::String,
        )
    };
    let mode_of = |count: usize| StorageMode::Strict {
        schema: StrictSchema::new(
            std::iter::once(ColumnDef::required("_rowid", ColumnType::Int64))
                .chain((0..count).map(&column))
                .collect(),
        )
        .expect("strict schema"),
    };
    // Every column costs the same, so the count follows from one measure.
    let one = witness_bytes(&mode_of(1));
    let per_column = witness_bytes(&mode_of(2)) - one;
    let mode = mode_of((MAX_SCHEMA_WITNESS_BYTES - one) / per_column);
    let bytes = witness_bytes(&mode);
    assert!(bytes <= MAX_SCHEMA_WITNESS_BYTES, "{bytes}");
    assert!(bytes + 2 * per_column > MAX_SCHEMA_WITNESS_BYTES, "{bytes}");
    mode
}

/// An installed row whose id, bodies and event images are all at the limit.
fn row_at_limit(schema: StorageMode) -> WriteSetEntry {
    WriteSetEntry::installed_put(
        u32::MAX,
        RowIdentity::from_user_key("k".repeat(MAX_ROW_IMAGE_BYTES)),
        InstalledDocument {
            canonical: image(MAX_ROW_IMAGE_BYTES),
            stored: image(MAX_ROW_IMAGE_BYTES),
            version: RowVersion {
                sys_from: i64::MAX,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
            },
            schema: Arc::new(schema),
            chain_head: Some(ChainHead {
                seq: u64::MAX,
                hash: "f".repeat(64),
            }),
        },
        DocumentPutEvent {
            new_value: image(MAX_ROW_IMAGE_BYTES),
            old_value: Some(image(MAX_OLD_EVENT_IMAGE_BYTES)),
        },
    )
}

/// The one part `entry` plans into at the WAL record limit, as the record
/// the part appends.
fn single_part(collection: &str, entry: WriteSetEntry) -> WriteGroupRecord {
    let parts = plan_group_parts(&target(collection), &[entry], MAX_RECORD_PAYLOAD)
        .expect("a row at the limit plans");
    assert_eq!(parts.runs.len(), 1, "a row at the limit takes one part");
    let (_, ops, schemas) = parts.runs.into_iter().next().expect("one part");
    WriteGroupRecord {
        schemas,
        group: WriteGroup::part_of(7, 1, 1),
        ops,
        redo: None,
    }
}

#[test]
fn an_installed_row_at_the_limit_fits_one_part_at_the_wal_limit() {
    let collection = longest_name();
    for schema in [StorageMode::Schemaless, strict_at_limit()] {
        let record = single_part(&collection, row_at_limit(schema));
        let payload = &record.ops[0].payload;
        // The entry's fixed fields stay within their reserve.
        assert!(
            payload.len()
                <= 4 * (MAX_ROW_IMAGE_BYTES + HEADER_32)
                    + MAX_OLD_EVENT_IMAGE_BYTES
                    + HEADER_32
                    + COLLECTION_NAME_RESERVE
                    + ENTRY_FRAMING_RESERVE,
            "sub-record payload of {} bytes",
            payload.len()
        );
        assert!(payload.len() + SUB_RECORD_OVERHEAD <= MAX_INSTALLED_ROW_PART_COST);
        let bytes = record.to_bytes().expect("encode part");
        assert!(
            bytes.len() <= MAX_RECORD_PAYLOAD,
            "a part of {} bytes exceeds the WAL record limit",
            bytes.len()
        );
    }
}

#[test]
fn an_edge_at_the_limit_fits_one_part_at_the_wal_limit() {
    let collection = longest_name();
    let edge = WriteSetEntry::edge(EdgeImage::Put(EdgePutRedo {
        collection: collection.clone(),
        src_id: "s".repeat(MAX_ROW_IMAGE_BYTES),
        label: "l".repeat(MAX_ROW_IMAGE_BYTES),
        dst_id: "d".repeat(MAX_ROW_IMAGE_BYTES),
        properties: image(MAX_ROW_IMAGE_BYTES),
        src_surrogate: 1,
        dst_surrogate: 2,
        system_from: Some(i64::MAX),
        applied: Some(i64::MAX),
        valid_from_ms: Some(i64::MIN),
        valid_until_ms: Some(i64::MAX),
    }));
    let record = single_part(&collection, edge);
    assert!(record.ops[0].payload.len() + SUB_RECORD_OVERHEAD <= MAX_EDGE_PART_COST);
    let bytes = record.to_bytes().expect("encode part");
    assert!(bytes.len() <= MAX_RECORD_PAYLOAD, "{} bytes", bytes.len());
}
