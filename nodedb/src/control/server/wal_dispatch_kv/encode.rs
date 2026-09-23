// SPDX-License-Identifier: BUSL-1.1

//! Pure payload encoders for KV WAL records.

use nodedb_physical::physical_plan::KvCounterShape;
use nodedb_physical::physical_plan::UpdateValue;

/// Serialize `value` to a MessagePack WAL payload, wrapping any encode error
/// into a `crate::Error::Serialization` tagged with `context`.
fn encode<T: zerompk::ToMessagePack>(context: &str, value: &T) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal kv {context}: {e}"),
    })
}

/// Encode a `kv_put` WAL payload: `("kv_put", collection, key, value, ttl_ms,
/// expire_at_ms, surrogate)`. A row with no surrogate replays at identity `0`,
/// read as "always visible" by clone-snapshot. Two shorter legacy shapes stay decodable.
pub(crate) fn encode_kv_put(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    expire_at_ms: Option<u64>,
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "put",
        &(
            "kv_put",
            collection,
            key,
            value,
            ttl_ms,
            expire_at_ms,
            surrogate,
        ),
    )
}

/// Encode a `kv_insert_on_conflict_update` WAL payload. Delta record: `value`
/// is the pre-merge `EXCLUDED` row, `updates` carries `DO UPDATE SET` inputs;
/// replay re-runs the merge. `expire_at_ms` appends a 7th element when `Some`.
pub(crate) fn encode_kv_insert_on_conflict_update(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    updates: &[(String, UpdateValue)],
    expire_at_ms: Option<u64>,
) -> crate::Result<Vec<u8>> {
    match expire_at_ms {
        None => encode(
            "insert on conflict update",
            &(
                "kv_insert_on_conflict_update",
                collection,
                key,
                value,
                ttl_ms,
                updates,
            ),
        ),
        Some(expire_at_ms) => encode(
            "insert on conflict update",
            &(
                "kv_insert_on_conflict_update",
                collection,
                key,
                value,
                ttl_ms,
                updates,
                expire_at_ms,
            ),
        ),
    }
}

/// Fields of a `kv_transfer` WAL payload, bundled so [`encode_kv_transfer`]
/// stays under the `too_many_arguments` clippy threshold.
pub(crate) struct KvTransferFields<'a> {
    pub collection: &'a str,
    pub source_key: &'a [u8],
    pub dest_key: &'a [u8],
    pub field: &'a str,
    pub amount: f64,
    pub debit_surrogate: u32,
    pub credit_surrogate: u32,
}

/// Encode a `kv_transfer` delta WAL payload: `("kv_transfer", collection,
/// source_key, dest_key, field, amount, debit_surrogate, credit_surrogate)`.
/// Delta record: replay re-executes `compute_transfer`, not a captured post-image.
pub(crate) fn encode_kv_transfer(f: KvTransferFields<'_>) -> crate::Result<Vec<u8>> {
    encode(
        "transfer",
        &(
            "kv_transfer",
            f.collection,
            f.source_key,
            f.dest_key,
            f.field,
            f.amount,
            f.debit_surrogate,
            f.credit_surrogate,
        ),
    )
}

/// Encode a `kv_transfer_item` delta WAL payload: `("kv_transfer_item",
/// source_collection, dest_collection, item_key, dest_key, surrogate)`. Replay
/// re-verifies source ownership and re-executes the delete+insert pair.
pub(crate) fn encode_kv_transfer_item(
    source_collection: &str,
    dest_collection: &str,
    item_key: &[u8],
    dest_key: &[u8],
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "transfer item",
        &(
            "kv_transfer_item",
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
        ),
    )
}

/// Encode a `kv_cas` WAL payload: `("kv_cas", collection, key, expected,
/// new_value, surrogate)`. Carries the CAS inputs, not the live result —
/// replay re-runs the compare against whatever value is present.
pub(crate) fn encode_kv_cas(
    collection: &str,
    key: &[u8],
    expected: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "cas",
        &("kv_cas", collection, key, expected, new_value, surrogate),
    )
}

/// Encode a `kv_incr_float` WAL payload: `("kv_incr_float", collection, key,
/// delta, surrogate, shape)`. `delta` is the client's decimal text. Delta
/// record: replay re-runs `incr_float` on the present value, and an absent key
/// takes `shape`.
pub(crate) fn encode_kv_incr_float(
    collection: &str,
    key: &[u8],
    delta: &str,
    surrogate: u32,
    shape: &KvCounterShape,
) -> crate::Result<Vec<u8>> {
    encode(
        "incr_float",
        &("kv_incr_float", collection, key, delta, surrogate, shape),
    )
}

/// Encode a `kv_field_set` WAL payload: `("kv_field_set", collection, key,
/// updates, surrogate, if_present)`. Delta record: `updates` carries
/// field-level inputs, not the post-merge document — replay re-runs
/// `merge_field_updates`. `if_present` pins the SQL-UPDATE-vs-HSET no-op
/// rule so replay matches the live decision.
pub(crate) fn encode_kv_field_set(
    collection: &str,
    key: &[u8],
    updates: &[(String, Vec<u8>)],
    surrogate: u32,
    if_present: bool,
) -> crate::Result<Vec<u8>> {
    encode(
        "field set",
        &(
            "kv_field_set",
            collection,
            key,
            updates,
            surrogate,
            if_present,
        ),
    )
}

/// Encode a `kv_getset` WAL payload: `("kv_getset", collection, key,
/// new_value, surrogate)`.
pub(crate) fn encode_kv_getset(
    collection: &str,
    key: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "getset",
        &("kv_getset", collection, key, new_value, surrogate),
    )
}

/// Encode a `kv_delete` WAL payload: `("kv_delete", collection, keys)`.
pub(crate) fn encode_kv_delete(collection: &str, keys: &[Vec<u8>]) -> crate::Result<Vec<u8>> {
    encode("delete", &("kv_delete", collection, keys))
}

/// Encode a `kv_batch_put` WAL payload: `("kv_batch_put", collection, entries,
/// ttl_ms, expire_at_ms, surrogates)`. `surrogates` is positional against
/// `entries`. Two shorter legacy shapes remain decodable.
pub(crate) fn encode_kv_batch_put(
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    expire_at_ms: Option<u64>,
    surrogates: &[u32],
) -> crate::Result<Vec<u8>> {
    encode(
        "batch put",
        &(
            "kv_batch_put",
            collection,
            entries,
            ttl_ms,
            expire_at_ms,
            surrogates,
        ),
    )
}

/// Encode a `kv_expire` WAL payload: `("kv_expire", collection, key, ttl_ms,
/// expire_at_ms)`. `ttl_ms == 0` means "expire now", not "no TTL" — the
/// instant is always resolved and carried.
pub(crate) fn encode_kv_expire(
    collection: &str,
    key: &[u8],
    ttl_ms: u64,
    expire_at_ms: u64,
) -> crate::Result<Vec<u8>> {
    encode(
        "expire",
        &("kv_expire", collection, key, ttl_ms, expire_at_ms),
    )
}

/// Encode a `kv_persist` WAL payload: `("kv_persist", collection, key)`.
pub(crate) fn encode_kv_persist(collection: &str, key: &[u8]) -> crate::Result<Vec<u8>> {
    encode("persist", &("kv_persist", collection, key))
}

/// Encode a `kv_register_index` WAL payload: `("kv_register_index",
/// collection, field, field_position, backfill)`. `backfill` is not derivable —
/// replay must reproduce whichever the user chose.
pub(crate) fn encode_kv_register_index(
    collection: &str,
    field: &str,
    field_position: usize,
    backfill: bool,
) -> crate::Result<Vec<u8>> {
    encode(
        "register index",
        &(
            "kv_register_index",
            collection,
            field,
            field_position,
            backfill,
        ),
    )
}

/// Encode a `kv_drop_index` WAL payload: `("kv_drop_index", collection,
/// field)`.
pub(crate) fn encode_kv_drop_index(collection: &str, field: &str) -> crate::Result<Vec<u8>> {
    encode("drop index", &("kv_drop_index", collection, field))
}

/// Fields of a `kv_incr` WAL payload.
pub(crate) struct KvIncrRecord<'a> {
    pub collection: &'a str,
    pub key: &'a [u8],
    pub delta: i64,
    /// `0` preserves the existing TTL.
    pub ttl_ms: u64,
    pub surrogate: u32,
    /// The row an absent key becomes.
    pub shape: &'a KvCounterShape,
    /// The absolute expiry the live write resolved. `Some` only when
    /// `ttl_ms > 0`, so replay installs the exact instant.
    pub expire_at_ms: Option<u64>,
}

/// Encode a `kv_incr` WAL payload: `("kv_incr", collection, key, delta,
/// ttl_ms, surrogate, shape, expire_at_ms)`.
pub(crate) fn encode_kv_incr(record: KvIncrRecord<'_>) -> crate::Result<Vec<u8>> {
    let KvIncrRecord {
        collection,
        key,
        delta,
        ttl_ms,
        surrogate,
        shape,
        expire_at_ms,
    } = record;
    encode(
        "incr",
        &(
            "kv_incr",
            collection,
            key,
            delta,
            ttl_ms,
            surrogate,
            shape,
            expire_at_ms,
        ),
    )
}

/// Fields of a `kv_register_sorted_index` WAL payload, bundled so
/// [`encode_kv_register_sorted_index`] stays under the `too_many_arguments`
/// clippy threshold.
pub(crate) struct KvRegisterSortedIndexFields<'a> {
    pub collection: &'a str,
    pub index_name: &'a str,
    pub sort_columns: &'a [(String, String)],
    pub key_column: &'a str,
    pub window_type: &'a str,
    pub window_timestamp_column: &'a str,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
}

/// Encode a `kv_register_sorted_index` WAL payload: `("kv_register_sorted_index",
/// collection, index_name, sort_columns, key_column, window_type,
/// window_timestamp_column, window_start_ms, window_end_ms)`.
pub(crate) fn encode_kv_register_sorted_index(
    f: KvRegisterSortedIndexFields<'_>,
) -> crate::Result<Vec<u8>> {
    encode(
        "register sorted index",
        &(
            "kv_register_sorted_index",
            f.collection,
            f.index_name,
            f.sort_columns,
            f.key_column,
            f.window_type,
            f.window_timestamp_column,
            f.window_start_ms,
            f.window_end_ms,
        ),
    )
}

/// Encode a `kv_drop_sorted_index` WAL payload: `("kv_drop_sorted_index",
/// index_name)`.
pub(crate) fn encode_kv_drop_sorted_index(index_name: &str) -> crate::Result<Vec<u8>> {
    encode("drop sorted index", &("kv_drop_sorted_index", index_name))
}

/// Encode a `kv_predicate_update` WAL payload:
/// `("kv_predicate_update", collection, filters, updates)`. Delta record —
/// which rows match is only known once scanned, so replay re-runs the predicate.
pub(crate) fn encode_kv_predicate_update(
    collection: &str,
    filters: &[u8],
    updates: &[(String, Vec<u8>)],
) -> crate::Result<Vec<u8>> {
    encode(
        "predicate update",
        &("kv_predicate_update", collection, filters, updates),
    )
}

/// Encode a `kv_predicate_delete` WAL payload:
/// `("kv_predicate_delete", collection, filters)`. See
/// [`encode_kv_predicate_update`] for why the predicate travels.
pub(crate) fn encode_kv_predicate_delete(
    collection: &str,
    filters: &[u8],
) -> crate::Result<Vec<u8>> {
    encode(
        "predicate delete",
        &("kv_predicate_delete", collection, filters),
    )
}

/// Encode a `kv_truncate` WAL payload: `("kv_truncate", collection)`.
pub(crate) fn encode_kv_truncate(collection: &str) -> crate::Result<Vec<u8>> {
    encode("truncate", &("kv_truncate", collection))
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{KvCounterShape, UpdateValue};

    use super::{
        KvIncrRecord, KvTransferFields, encode_kv_batch_put, encode_kv_cas, encode_kv_expire,
        encode_kv_field_set, encode_kv_getset, encode_kv_incr, encode_kv_incr_float,
        encode_kv_insert_on_conflict_update, encode_kv_put, encode_kv_register_index,
        encode_kv_transfer, encode_kv_transfer_item,
    };

    #[test]
    fn kv_put_carries_the_row_surrogate() {
        let entry = encode_kv_put("users", b"k1", b"v1", 5_000, None, 77).unwrap();

        let (disc, collection, key, value, ttl_ms, expire_at_ms, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(
                &entry,
            )
            .unwrap();
        assert_eq!(disc, "kv_put");
        assert_eq!(collection, "users");
        assert_eq!(key, b"k1");
        assert_eq!(value, b"v1");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, None);
        assert_eq!(
            surrogate, 77,
            "the row's cross-engine identity must survive a crash, not be \
             re-derived as zero on replay"
        );

        // Neither pre-surrogate shape may alias the current one — replay tries
        // all three and must never mistake one for another.
        assert!(zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(&entry).is_err());
        assert!(
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, u64)>(&entry).is_err()
        );
    }

    #[test]
    fn kv_put_with_expire_at_carries_absolute_instant() {
        let entry =
            encode_kv_put("users", b"k1", b"v1", 5_000, Some(1_700_000_000_000), 9).unwrap();

        let (disc, collection, key, value, ttl_ms, expire_at_ms, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(
                &entry,
            )
            .unwrap();
        assert_eq!(disc, "kv_put");
        assert_eq!(collection, "users");
        assert_eq!(key, b"k1");
        assert_eq!(value, b"v1");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, Some(1_700_000_000_000));
        assert_eq!(surrogate, 9);
    }

    #[test]
    fn kv_batch_put_carries_one_surrogate_per_entry() {
        let entries = vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ];
        let entry = encode_kv_batch_put("users", &entries, 5_000, None, &[3, 4]).unwrap();

        let (disc, collection, decoded_entries, ttl_ms, expire_at_ms, surrogates) =
            zerompk::from_msgpack::<(
                &str,
                String,
                Vec<(Vec<u8>, Vec<u8>)>,
                u64,
                Option<u64>,
                Vec<u32>,
            )>(&entry)
            .unwrap();
        assert_eq!(disc, "kv_batch_put");
        assert_eq!(collection, "users");
        assert_eq!(decoded_entries, entries);
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, None);
        assert_eq!(
            surrogates,
            vec![3, 4],
            "surrogates are positional against entries"
        );

        assert!(
            zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64)>(&entry).is_err()
        );
        assert!(
            zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64, u64)>(&entry)
                .is_err()
        );
    }

    #[test]
    fn kv_batch_put_with_expire_at_carries_absolute_instant() {
        let entries = vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ];
        let entry = encode_kv_batch_put("users", &entries, 5_000, Some(1_700_000_000_000), &[3, 4])
            .unwrap();

        let (disc, collection, decoded_entries, ttl_ms, expire_at_ms, surrogates) =
            zerompk::from_msgpack::<(
                &str,
                String,
                Vec<(Vec<u8>, Vec<u8>)>,
                u64,
                Option<u64>,
                Vec<u32>,
            )>(&entry)
            .unwrap();
        assert_eq!(disc, "kv_batch_put");
        assert_eq!(collection, "users");
        assert_eq!(decoded_entries, entries);
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, Some(1_700_000_000_000));
        assert_eq!(surrogates, vec![3, 4]);
    }

    #[test]
    fn kv_transfer_encodes_delta_shape_with_both_surrogates() {
        let entry = encode_kv_transfer(KvTransferFields {
            collection: "accounts",
            source_key: b"alice",
            dest_key: b"bob",
            field: "balance",
            amount: 30.0,
            debit_surrogate: 7,
            credit_surrogate: 8,
        })
        .unwrap();

        let (
            disc,
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
        ) = zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, String, f64, u32, u32)>(
            &entry,
        )
        .unwrap();
        assert_eq!(disc, "kv_transfer");
        assert_eq!(collection, "accounts");
        assert_eq!(source_key, b"alice");
        assert_eq!(dest_key, b"bob");
        assert_eq!(field, "balance");
        assert_eq!(amount, 30.0);
        assert_eq!(debit_surrogate, 7);
        assert_eq!(credit_surrogate, 8);
    }

    #[test]
    fn kv_transfer_item_encodes_delta_shape_with_surrogate() {
        let entry =
            encode_kv_transfer_item("inventory", "trades", b"sword_1", b"sword_moved", 42).unwrap();

        let (disc, source_collection, dest_collection, item_key, dest_key, surrogate) =
            zerompk::from_msgpack::<(&str, String, String, Vec<u8>, Vec<u8>, u32)>(&entry).unwrap();
        assert_eq!(disc, "kv_transfer_item");
        assert_eq!(source_collection, "inventory");
        assert_eq!(dest_collection, "trades");
        assert_eq!(item_key, b"sword_1");
        assert_eq!(dest_key, b"sword_moved");
        assert_eq!(surrogate, 42);
    }

    #[test]
    fn kv_cas_encodes_expected_and_new_value_with_surrogate() {
        let entry = encode_kv_cas("state", b"p1", b"idle", b"in_match", 9).unwrap();

        let (disc, collection, key, expected, new_value, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, Vec<u8>, u32)>(&entry)
                .unwrap();
        assert_eq!(disc, "kv_cas");
        assert_eq!(collection, "state");
        assert_eq!(key, b"p1");
        assert_eq!(expected, b"idle");
        assert_eq!(new_value, b"in_match");
        assert_eq!(surrogate, 9);
    }

    #[test]
    fn kv_incr_float_encodes_decimal_delta_with_surrogate_and_shape() {
        let entry =
            encode_kv_incr_float("scores", b"dmg", "3.125", 5, &KvCounterShape::Raw).unwrap();

        let (disc, collection, key, delta, surrogate, shape) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, String, u32, KvCounterShape)>(&entry)
                .unwrap();
        assert_eq!(disc, "kv_incr_float");
        assert_eq!(collection, "scores");
        assert_eq!(key, b"dmg");
        assert_eq!(delta, "3.125");
        assert_eq!(surrogate, 5);
        assert_eq!(shape, KvCounterShape::Raw);
    }

    #[test]
    fn kv_field_set_encodes_updates_with_surrogate() {
        let updates = vec![
            ("score".to_string(), b"42".to_vec()),
            ("name".to_string(), b"alice".to_vec()),
        ];
        let entry = encode_kv_field_set("players", b"p1", &updates, 11, true).unwrap();

        let (disc, collection, key, decoded_updates, surrogate, if_present) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<(String, Vec<u8>)>, u32, bool)>(
                &entry,
            )
            .unwrap();
        assert_eq!(disc, "kv_field_set");
        assert_eq!(collection, "players");
        assert_eq!(key, b"p1");
        assert_eq!(decoded_updates, updates);
        assert_eq!(surrogate, 11);
        assert!(if_present);
    }

    #[test]
    fn kv_insert_on_conflict_update_without_expire_at_carries_updates() {
        let updates = vec![("score".to_string(), UpdateValue::Literal(b"42".to_vec()))];
        let entry =
            encode_kv_insert_on_conflict_update("players", b"p1", b"excluded", 0, &updates, None)
                .unwrap();

        let (disc, collection, key, value, ttl_ms, decoded_updates) = zerompk::from_msgpack::<(
            &str,
            String,
            Vec<u8>,
            Vec<u8>,
            u64,
            Vec<(String, UpdateValue)>,
        )>(&entry)
        .unwrap();
        assert_eq!(disc, "kv_insert_on_conflict_update");
        assert_eq!(collection, "players");
        assert_eq!(key, b"p1");
        assert_eq!(value, b"excluded");
        assert_eq!(ttl_ms, 0);
        assert_eq!(decoded_updates, updates);

        // The extended (with-expiry) shape must not alias this one.
        assert!(
            zerompk::from_msgpack::<(
                &str,
                String,
                Vec<u8>,
                Vec<u8>,
                u64,
                Vec<(String, UpdateValue)>,
                u64
            )>(&entry)
            .is_err(),
            "six-element payload must not decode as the seven-element tuple"
        );
    }

    #[test]
    fn kv_insert_on_conflict_update_with_expire_at_carries_absolute_instant() {
        let updates = vec![("score".to_string(), UpdateValue::Literal(b"42".to_vec()))];
        let entry = encode_kv_insert_on_conflict_update(
            "players",
            b"p1",
            b"excluded",
            5_000,
            &updates,
            Some(1_700_000_000_000),
        )
        .unwrap();

        let (disc, collection, key, value, ttl_ms, decoded_updates, expire_at_ms) =
            zerompk::from_msgpack::<(
                &str,
                String,
                Vec<u8>,
                Vec<u8>,
                u64,
                Vec<(String, UpdateValue)>,
                u64,
            )>(&entry)
            .unwrap();
        assert_eq!(disc, "kv_insert_on_conflict_update");
        assert_eq!(collection, "players");
        assert_eq!(key, b"p1");
        assert_eq!(value, b"excluded");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(decoded_updates, updates);
        assert_eq!(expire_at_ms, 1_700_000_000_000);
    }

    #[test]
    fn kv_register_index_round_trips_backfill_flag() {
        let entry_backfill_true = encode_kv_register_index("players", "name", 2, true).unwrap();
        let (disc, collection, field, field_position, backfill) =
            zerompk::from_msgpack::<(&str, String, String, usize, bool)>(&entry_backfill_true)
                .unwrap();
        assert_eq!(disc, "kv_register_index");
        assert_eq!(collection, "players");
        assert_eq!(field, "name");
        assert_eq!(field_position, 2);
        assert!(backfill);

        let entry_backfill_false = encode_kv_register_index("players", "name", 2, false).unwrap();
        let (_, _, _, _, backfill_false) =
            zerompk::from_msgpack::<(&str, String, String, usize, bool)>(&entry_backfill_false)
                .unwrap();
        assert!(!backfill_false);

        // The two payloads must not be byte-identical: the backfill flag is
        // the only difference and it must actually change the encoded bytes.
        assert_ne!(entry_backfill_true, entry_backfill_false);
    }

    #[test]
    fn kv_expire_always_carries_the_resolved_absolute_instant() {
        let entry = encode_kv_expire("sessions", b"tok1", 5_000, 6_000).unwrap();

        let (disc, collection, key, ttl_ms, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, u64, u64)>(&entry).unwrap();
        assert_eq!(disc, "kv_expire");
        assert_eq!(collection, "sessions");
        assert_eq!(key, b"tok1");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, 6_000);
    }

    #[test]
    fn kv_expire_with_zero_ttl_still_carries_an_absolute_instant() {
        // ttl_ms == 0 means "expire right now" for EXPIRE, not "no TTL" as for PUT.
        let entry = encode_kv_expire("sessions", b"tok2", 0, 1_234).unwrap();

        let (disc, collection, key, ttl_ms, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, u64, u64)>(&entry).unwrap();
        assert_eq!(disc, "kv_expire");
        assert_eq!(collection, "sessions");
        assert_eq!(key, b"tok2");
        assert_eq!(ttl_ms, 0);
        assert_eq!(expire_at_ms, 1_234);
    }

    /// The decoded `kv_incr` tuple.
    type IncrTuple = (
        String,
        String,
        Vec<u8>,
        i64,
        u64,
        u32,
        KvCounterShape,
        Option<u64>,
    );

    #[test]
    fn kv_incr_carries_shape_and_no_expiry_when_ttl_is_preserved() {
        let shape = KvCounterShape::Typed {
            column: Some("n".into()),
            template: vec![0x80],
        };
        let entry = encode_kv_incr(KvIncrRecord {
            collection: "counters",
            key: b"hits",
            delta: 3,
            ttl_ms: 0,
            surrogate: 7,
            shape: &shape,
            expire_at_ms: None,
        })
        .unwrap();

        let (disc, collection, key, delta, ttl_ms, surrogate, decoded_shape, expire_at_ms) =
            zerompk::from_msgpack::<IncrTuple>(&entry).unwrap();
        assert_eq!(disc, "kv_incr");
        assert_eq!(collection, "counters");
        assert_eq!(key, b"hits");
        assert_eq!(delta, 3);
        assert_eq!(ttl_ms, 0);
        assert_eq!(surrogate, 7);
        assert_eq!(decoded_shape, shape);
        assert_eq!(expire_at_ms, None);
    }

    #[test]
    fn kv_incr_with_expire_at_carries_absolute_instant() {
        let entry = encode_kv_incr(KvIncrRecord {
            collection: "counters",
            key: b"daily",
            delta: 1,
            ttl_ms: 86_400_000,
            surrogate: 9,
            shape: &KvCounterShape::Raw,
            expire_at_ms: Some(1_700_000_000_000),
        })
        .unwrap();

        let (disc, _, _, delta, ttl_ms, surrogate, _, expire_at_ms) =
            zerompk::from_msgpack::<IncrTuple>(&entry).unwrap();
        assert_eq!(disc, "kv_incr");
        assert_eq!(delta, 1);
        assert_eq!(ttl_ms, 86_400_000);
        assert_eq!(surrogate, 9);
        assert_eq!(expire_at_ms, Some(1_700_000_000_000));
    }

    #[test]
    fn kv_getset_encodes_new_value_with_surrogate() {
        let entry = encode_kv_getset("session", b"tok", b"new-token", 3).unwrap();

        let (disc, collection, key, new_value, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u32)>(&entry).unwrap();
        assert_eq!(disc, "kv_getset");
        assert_eq!(collection, "session");
        assert_eq!(key, b"tok");
        assert_eq!(new_value, b"new-token");
        assert_eq!(surrogate, 3);
    }
}
