// SPDX-License-Identifier: BUSL-1.1

//! The KV `Put` payloads the Event Plane's WAL replay reads, and the events
//! they replay as.
//!
//! Each decoder accepts the one shape its writer produces. A `kv_put` or
//! `kv_batch_put` shape carries each row's surrogate. A record whose surrogate
//! is `Surrogate::ZERO`, or any other shape, decodes to `None`. A `kv_rewrite`
//! carries none: the row keeps its bound identity.

use std::sync::Arc;

use nodedb_types::RowIdentity;

use crate::event::image_fault::ImageFault;
use crate::event::types::{RecordPosition, RowId, WriteEvent, WriteOp};
use crate::event::wal_replay_scope::ReplayScope;

/// The `(key, value)` entries of a KV batch put.
type KvEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// The declared columns a KV record carries. A replayed value re-types
/// under them, as the live write's stored value did.
type Declared = Vec<nodedb_physical::physical_plan::DeclaredColumn>;

/// The fields an event needs out of a `kv_put` record.
struct KvPutEventFields {
    collection: String,
    key: Vec<u8>,
    value: Vec<u8>,
    declared: Declared,
}

/// The fields an event needs out of a `kv_put` record, `("kv_put",
/// collection, key, value, ttl_ms, expire_at_ms, surrogate, declared)`.
fn decode_kv_put_event_fields(payload: &[u8]) -> Option<KvPutEventFields> {
    let (disc, collection, key, value, _ttl, _expire, surrogate, declared) =
        zerompk::from_msgpack::<(
            &str,
            String,
            Vec<u8>,
            Vec<u8>,
            u64,
            Option<u64>,
            u32,
            Declared,
        )>(payload)
        .ok()?;
    (disc == "kv_put" && surrogate != nodedb_types::Surrogate::ZERO.as_u32()).then_some(
        KvPutEventFields {
            collection,
            key,
            value,
            declared,
        },
    )
}

/// The `(collection, entries)` an event needs out of a `kv_batch_put` record,
/// `("kv_batch_put", collection, entries, ttl_ms, expire_at_ms, surrogates,
/// declared)`. The record carries one bound surrogate per entry. The batch
/// event carries no row value, so its columns re-type nothing here.
fn decode_kv_batch_put_event_fields(payload: &[u8]) -> Option<(String, KvEntries)> {
    let (disc, collection, entries, _ttl, _expire, surrogates, _declared) =
        zerompk::from_msgpack::<(
            &str,
            String,
            KvEntries,
            u64,
            Option<u64>,
            Vec<u32>,
            Declared,
        )>(payload)
        .ok()?;
    let bound = surrogates.len() == entries.len()
        && surrogates
            .iter()
            .all(|s| *s != nodedb_types::Surrogate::ZERO.as_u32());
    (disc == "kv_batch_put" && bound).then_some((collection, entries))
}

/// The `(collection, key, value)` an event needs out of a `kv_rewrite`
/// record, `("kv_rewrite", collection, key, value, ttl_ms, expire_at_ms)`. A
/// rewrite carries no surrogate: the row keeps its bound identity.
fn decode_kv_rewrite_event_fields(payload: &[u8]) -> Option<(String, Vec<u8>, Vec<u8>)> {
    let (disc, collection, key, value, _ttl, _expire) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, u64)>(payload).ok()?;
    (disc == "kv_rewrite").then_some((collection, key, value))
}

/// `value` re-typed under `declared`, the value the live write stored.
/// `None` when it does not re-type: the event then names the fault.
fn stored_value<'a>(
    value: &'a [u8],
    declared: &[nodedb_physical::physical_plan::DeclaredColumn],
) -> Option<std::borrow::Cow<'a, [u8]>> {
    match crate::data::executor::handlers::kv::declared_body::coerce_kv_body(value, declared) {
        Ok(stored) => Some(stored),
        Err(e) => {
            tracing::warn!(error = %e, "KV replay value does not re-type under its columns");
            None
        }
    }
}

/// The `{key, value}` row image of a replayed KV body. `None` when the row
/// does not encode: the event then names the fault ([`ImageFault`]) and the
/// delivery pipeline dead-letters it.
fn replay_row_image(key: &str, value: &[u8]) -> Option<Vec<u8>> {
    match nodedb_query::msgpack_scan::kv_row_msgpack(key, value) {
        Ok(row) => Some(row),
        Err(e) => {
            tracing::warn!(key, error = %e, "KV replay row image does not encode");
            None
        }
    }
}

/// The event a KV put-family `Put` payload replays as, or `None` when the
/// payload is not a bound record of that family. `kv_put` and `kv_rewrite`
/// name one row, and `kv_batch_put` names its batch.
pub(super) fn parse_kv_put_family(
    payload: &[u8],
    scope: &ReplayScope,
    sequence: &mut u64,
) -> Option<WriteEvent> {
    let ReplayScope {
        database_id,
        tenant_id,
        vshard_id,
        lsn,
        sources,
        commit_hlc,
    } = *scope;
    // Try KV put first: `("kv_put", collection, key, value, ttl_ms,
    // expire_at_ms, surrogate, declared)` with a bound surrogate. The event
    // stream keys on the raw KV key.
    if let Some(KvPutEventFields {
        collection,
        key,
        value,
        declared,
    }) = decode_kv_put_event_fields(payload)
    {
        *sequence += 1;
        let key_str = String::from_utf8_lossy(&key);
        // The same `{key, value}` row image the live KV write event carries:
        // the value re-typed under the record's columns.
        let row =
            stored_value(&value, &declared).and_then(|stored| replay_row_image(&key_str, &stored));
        let (system_time_ms, valid_time_ms) =
            crate::event::bitemporal_extract::extract_stamps(row.as_deref());
        // AUDIT_DML rows replayed from WAL after a crash carry user_id = None and
        // statement_digest = None; pre-crash audit rows are durable in the catalog.
        // Widening the WAL record format to carry these fields is tracked separately.
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Insert,
            row_id: RowId::row(RowIdentity::from_user_key(key_str.into_owned())),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.other,
            image_fault: ImageFault::of(row.is_none(), false),
            new_value: row.as_deref().map(Arc::from),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
            commit_hlc,
        });
    }

    // Try KV rewrite: a new value for an existing row, the live apply's
    // `Update` event. The record holds no pre-image, so `old_value` is empty.
    if let Some((collection, key, value)) = decode_kv_rewrite_event_fields(payload) {
        *sequence += 1;
        let key_str = String::from_utf8_lossy(&key);
        let row = replay_row_image(&key_str, &value);
        let (system_time_ms, valid_time_ms) =
            crate::event::bitemporal_extract::extract_stamps(row.as_deref());
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Update,
            row_id: RowId::row(RowIdentity::from_user_key(key_str.into_owned())),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.other,
            image_fault: ImageFault::of(row.is_none(), false),
            new_value: row.as_deref().map(Arc::from),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
            commit_hlc,
        });
    }

    // Try KV batch put, the one shape its writer produces.
    if let Some((collection, entries)) = decode_kv_batch_put_event_fields(payload) {
        // Emit one event for the batch (BulkInsert).
        *sequence += 1;
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::BulkInsert {
                count: entries.len() as u32,
            },
            row_id: RowId::Batch,
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.other,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc,
            image_fault: None,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bound_kv_put_decodes_and_an_unbound_or_retired_one_does_not() {
        let bound = zerompk::to_msgpack_vec(&(
            "kv_put",
            "c",
            vec![1u8],
            vec![2u8],
            0u64,
            None::<u64>,
            7u32,
            Declared::new(),
        ))
        .expect("encode");
        assert!(decode_kv_put_event_fields(&bound).is_some());
        let unbound = zerompk::to_msgpack_vec(&(
            "kv_put",
            "c",
            vec![1u8],
            vec![2u8],
            0u64,
            None::<u64>,
            0u32,
            Declared::new(),
        ))
        .expect("encode");
        assert!(decode_kv_put_event_fields(&unbound).is_none());
        let retired =
            zerompk::to_msgpack_vec(&("kv_put", "c", vec![1u8], vec![2u8], 0u64)).expect("encode");
        assert!(decode_kv_put_event_fields(&retired).is_none());
    }

    /// The live write stores `1.005` under `DECIMAL(5,2)` as `1.01`, and its
    /// event carries that stored value. A catchup replay of its record
    /// carries the same row image.
    #[test]
    fn a_replayed_kv_put_carries_the_value_the_live_write_stored() {
        use crate::event::types::EventSource;
        use crate::event::wal_replay_scope::RowSources;
        use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
        use nodedb_physical::physical_plan::DeclaredColumn;

        let declared =
            vec![DeclaredColumn::from_declared("value", "DECIMAL(5,2)").expect("decimal")];
        let record = zerompk::to_msgpack_vec(&(
            "kv_put",
            "c",
            b"k".to_vec(),
            b"1.005".to_vec(),
            0u64,
            None::<u64>,
            7u32,
            declared.clone(),
        ))
        .expect("encode");
        let scope = ReplayScope {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            lsn: Lsn::new(42),
            sources: RowSources::uniform(EventSource::User),
            commit_hlc: None,
        };
        let mut sequence = 0;
        let event = parse_kv_put_family(&record, &scope, &mut sequence).expect("a KV put event");

        let live_stored =
            crate::data::executor::handlers::kv::declared_body::coerce_kv_body(b"1.005", &declared)
                .expect("re-types");
        assert_eq!(
            &*live_stored, b"1.01",
            "the live write stores the fitted value"
        );
        let live_image =
            nodedb_query::msgpack_scan::kv_row_msgpack("k", &live_stored).expect("live image");
        assert_eq!(event.new_value.as_deref(), Some(live_image.as_slice()));
        assert!(event.image_fault.is_none());
    }

    #[test]
    fn a_kv_batch_put_decodes_only_with_one_bound_surrogate_per_entry() {
        let entries: KvEntries = vec![(vec![1], vec![2]), (vec![3], vec![4])];
        let encode = |surrogates: Vec<u32>| {
            zerompk::to_msgpack_vec(&(
                "kv_batch_put",
                "c",
                &entries,
                0u64,
                None::<u64>,
                surrogates,
                Declared::new(),
            ))
            .expect("encode")
        };
        assert!(decode_kv_batch_put_event_fields(&encode(vec![5, 6])).is_some());
        assert!(decode_kv_batch_put_event_fields(&encode(vec![5, 0])).is_none());
        assert!(decode_kv_batch_put_event_fields(&encode(vec![5])).is_none());
    }
}
