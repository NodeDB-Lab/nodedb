//! Atomic KV operations: INCR, INCR_FLOAT, CAS, GETSET.
//!
//! All operations are atomic within a single TPC core (which owns the key's
//! hash slot). No cross-core coordination is needed because each key maps
//! to exactly one core.

use super::engine::KvEngine;
use super::engine_helpers::{expiry_key, table_key};
use super::entry::NO_EXPIRY;
use super::hash_table::KvHashTable;

/// Result of a compare-and-swap operation.
pub struct CasResult {
    /// Whether the swap succeeded (current == expected).
    pub success: bool,
    /// The value that was present at the time of the CAS.
    /// `None` if the key did not exist.
    pub current_value: Option<Vec<u8>>,
}

/// Errors specific to atomic KV operations.
#[derive(Debug)]
pub enum AtomicError {
    /// Value is not the expected numeric type for INCR/DECR.
    TypeMismatch { detail: String },
    /// Integer overflow on INCR/DECR.
    Overflow,
}

impl KvEngine {
    /// Atomically increment an i64 value by `delta`. Returns the new value.
    ///
    /// - If key doesn't exist: initializes to 0, adds delta, returns delta.
    /// - If value is not a MessagePack integer: returns `TypeMismatch`.
    /// - On i64 overflow: returns `Overflow` (never wraps silently).
    /// - TTL behavior: if `ttl_ms > 0` and key is new, sets TTL.
    ///   If key exists and `ttl_ms > 0`, resets TTL. If `ttl_ms == 0`, preserves.
    pub fn incr(
        &mut self,
        tenant_id: u32,
        collection: &str,
        key: &[u8],
        delta: i64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<i64, AtomicError> {
        let tkey = table_key(tenant_id, collection);
        let table = self.ensure_table(tkey, tenant_id, collection);

        let current = table.get(key, now_ms).map(|v| v.to_vec());
        let old_i64 = match &current {
            None => 0i64,
            Some(bytes) => decode_msgpack_i64(bytes)?,
        };

        let new_i64 = old_i64.checked_add(delta).ok_or(AtomicError::Overflow)?;

        let new_bytes = rmp_serde::to_vec(&new_i64).expect("i64 always serializes");
        self.atomic_put(
            tenant_id,
            collection,
            tkey,
            key,
            &new_bytes,
            ttl_ms,
            now_ms,
            current.is_none(),
        );

        Ok(new_i64)
    }

    /// Atomically increment an f64 value by `delta`. Returns the new value.
    ///
    /// - If key doesn't exist: initializes to 0.0, adds delta, returns delta.
    /// - If value is not a MessagePack float or integer: returns `TypeMismatch`.
    /// - f64 does not overflow in the traditional sense (it goes to infinity),
    ///   but NaN/Infinity results are rejected as `Overflow`.
    pub fn incr_float(
        &mut self,
        tenant_id: u32,
        collection: &str,
        key: &[u8],
        delta: f64,
        now_ms: u64,
    ) -> Result<f64, AtomicError> {
        let tkey = table_key(tenant_id, collection);
        let table = self.ensure_table(tkey, tenant_id, collection);

        let current = table.get(key, now_ms).map(|v| v.to_vec());
        let old_f64 = match &current {
            None => 0.0f64,
            Some(bytes) => decode_msgpack_f64(bytes)?,
        };

        let new_f64 = old_f64 + delta;
        if new_f64.is_nan() || new_f64.is_infinite() {
            return Err(AtomicError::Overflow);
        }

        let new_bytes = rmp_serde::to_vec(&new_f64).expect("f64 always serializes");
        // incr_float always preserves existing TTL (ttl_ms = 0).
        self.atomic_put(
            tenant_id,
            collection,
            tkey,
            key,
            &new_bytes,
            0,
            now_ms,
            current.is_none(),
        );

        Ok(new_f64)
    }

    /// Atomic compare-and-swap.
    ///
    /// If current value equals `expected`, sets to `new_value` and returns success.
    /// If current value differs, returns the actual current value.
    /// If key doesn't exist and `expected` is empty, creates the key (create-if-not-exists).
    pub fn cas(
        &mut self,
        tenant_id: u32,
        collection: &str,
        key: &[u8],
        expected: &[u8],
        new_value: &[u8],
        now_ms: u64,
    ) -> CasResult {
        let tkey = table_key(tenant_id, collection);
        let table = self.ensure_table(tkey, tenant_id, collection);

        let current = table.get(key, now_ms).map(|v| v.to_vec());

        let matches = match &current {
            None => expected.is_empty(),
            Some(v) => v.as_slice() == expected,
        };

        if matches {
            // CAS preserves existing TTL (ttl_ms = 0).
            self.atomic_put(
                tenant_id,
                collection,
                tkey,
                key,
                new_value,
                0,
                now_ms,
                current.is_none(),
            );
            CasResult {
                success: true,
                current_value: current,
            }
        } else {
            CasResult {
                success: false,
                current_value: current,
            }
        }
    }

    /// Atomic get-and-set: sets new value, returns old value.
    ///
    /// If key didn't exist, returns `None`.
    /// Preserves existing TTL.
    pub fn getset(
        &mut self,
        tenant_id: u32,
        collection: &str,
        key: &[u8],
        new_value: &[u8],
        now_ms: u64,
    ) -> Option<Vec<u8>> {
        let tkey = table_key(tenant_id, collection);
        let table = self.ensure_table(tkey, tenant_id, collection);
        let old = table.get(key, now_ms).map(|v| v.to_vec());

        // GetSet preserves existing TTL (ttl_ms = 0).
        self.atomic_put(
            tenant_id,
            collection,
            tkey,
            key,
            new_value,
            0,
            now_ms,
            old.is_none(),
        );
        old
    }

    /// Ensure a hash table exists for (tenant, collection), creating if needed.
    /// Returns a mutable reference to the table.
    fn ensure_table(&mut self, tkey: u64, tenant_id: u32, collection: &str) -> &mut KvHashTable {
        if !self.tables.contains_key(&tkey) {
            self.hash_to_tenant.entry(tkey).or_insert(tenant_id);
            self.hash_to_collection
                .entry(tkey)
                .or_insert_with(|| collection.to_string());
            self.tables.entry(tkey).or_insert_with(|| {
                KvHashTable::new(
                    self.default_capacity,
                    self.load_factor_threshold,
                    self.rehash_batch_size,
                    self.inline_threshold,
                )
            });
        }
        self.tables.get_mut(&tkey).expect("just ensured")
    }

    /// Internal helper: put a value into the hash table, handling TTL and expiry.
    ///
    /// If `ttl_ms == 0`, preserves the existing TTL on an existing key.
    /// If `ttl_ms > 0`, sets/resets TTL. If key is new and `ttl_ms == 0`, no TTL.
    #[allow(clippy::too_many_arguments)]
    fn atomic_put(
        &mut self,
        tenant_id: u32,
        collection: &str,
        tkey: u64,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
        now_ms: u64,
        is_new_key: bool,
    ) {
        // Cache metadata lookup to avoid double HashMap access.
        let old_meta = if is_new_key {
            None
        } else {
            self.tables.get(&tkey).and_then(|t| t.get_entry_meta(key))
        };

        // Determine the target expire_at.
        let expire_at = if ttl_ms > 0 {
            // Explicit TTL: set/reset.
            now_ms + ttl_ms
        } else if let Some(ref meta) = old_meta {
            // Existing key, preserve TTL.
            meta.expire_at_ms
        } else {
            // New key with no TTL request: persistent.
            NO_EXPIRY
        };

        // Cancel old expiry before mutation.
        if let Some(ref meta) = old_meta {
            if meta.has_ttl {
                let composite = expiry_key(tenant_id, collection, key);
                self.expiry.cancel(&composite, meta.expire_at_ms);
            }
        }

        // Write the value.
        let table = self.tables.get_mut(&tkey).expect("table ensured");
        table.put(key, value, expire_at);

        // Schedule new expiry if needed.
        if expire_at != NO_EXPIRY {
            let composite = expiry_key(tenant_id, collection, key);
            self.expiry.insert(composite, expire_at);
        }

        // Secondary index maintenance.
        if self.indexes.get(&tkey).is_some_and(|idx| !idx.is_empty()) {
            let new_fields = super::engine_helpers::extract_all_field_values_from_msgpack(value);
            let new_refs: Vec<(&str, &[u8])> = new_fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_slice()))
                .collect();
            if let Some(idx_set) = self.indexes.get_mut(&tkey) {
                // For atomic ops on numeric values, indexes are updated but
                // old_fields tracking is omitted (atomic values are typically
                // not indexed by secondary indexes — they're raw numerics).
                idx_set.on_put(key, &new_refs, None);
            }
        }
    }
}

/// Decode a MessagePack-encoded value as i64.
fn decode_msgpack_i64(bytes: &[u8]) -> Result<i64, AtomicError> {
    // Try i64 first, then u64 (MessagePack encodes small positive as u64).
    if let Ok(v) = rmp_serde::from_slice::<i64>(bytes) {
        return Ok(v);
    }
    if let Ok(v) = rmp_serde::from_slice::<u64>(bytes) {
        return i64::try_from(v).map_err(|_| AtomicError::Overflow);
    }
    // Try f64 → i64 truncation for values stored as float.
    if let Ok(v) = rmp_serde::from_slice::<f64>(bytes)
        && v.fract() == 0.0
        && v >= i64::MIN as f64
        && v <= i64::MAX as f64
    {
        return Ok(v as i64);
    }
    Err(AtomicError::TypeMismatch {
        detail: "value is not an integer".into(),
    })
}

/// Decode a MessagePack-encoded value as f64.
fn decode_msgpack_f64(bytes: &[u8]) -> Result<f64, AtomicError> {
    if let Ok(v) = rmp_serde::from_slice::<f64>(bytes) {
        return Ok(v);
    }
    // Accept integer values promoted to float.
    if let Ok(v) = rmp_serde::from_slice::<i64>(bytes) {
        return Ok(v as f64);
    }
    if let Ok(v) = rmp_serde::from_slice::<u64>(bytes) {
        return Ok(v as f64);
    }
    Err(AtomicError::TypeMismatch {
        detail: "value is not numeric".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_engine() -> KvEngine {
        KvEngine::new(1000, 16, 0.75, 4, 64, 1000, 1024)
    }

    #[test]
    fn incr_new_key() {
        let mut engine = make_engine();
        let result = engine.incr(1, "counters", b"hits", 10, 0, 1000);
        assert_eq!(result.unwrap(), 10);
    }

    #[test]
    fn incr_existing_key() {
        let mut engine = make_engine();
        engine.incr(1, "counters", b"hits", 10, 0, 1000).unwrap();
        let result = engine.incr(1, "counters", b"hits", 5, 0, 1000);
        assert_eq!(result.unwrap(), 15);
    }

    #[test]
    fn incr_negative_delta() {
        let mut engine = make_engine();
        engine.incr(1, "counters", b"gold", 100, 0, 1000).unwrap();
        let result = engine.incr(1, "counters", b"gold", -30, 0, 1000);
        assert_eq!(result.unwrap(), 70);
    }

    #[test]
    fn incr_overflow() {
        let mut engine = make_engine();
        // Set to MAX.
        let bytes = rmp_serde::to_vec(&i64::MAX).unwrap();
        engine.put(1, "counters", b"max", &bytes, 0, 1000);
        let result = engine.incr(1, "counters", b"max", 1, 0, 1000);
        assert!(matches!(result, Err(AtomicError::Overflow)));
    }

    #[test]
    fn incr_type_mismatch() {
        let mut engine = make_engine();
        let bytes = rmp_serde::to_vec("hello").unwrap();
        engine.put(1, "counters", b"str", &bytes, 0, 1000);
        let result = engine.incr(1, "counters", b"str", 1, 0, 1000);
        assert!(matches!(result, Err(AtomicError::TypeMismatch { .. })));
    }

    #[test]
    fn incr_with_ttl_new_key() {
        let mut engine = make_engine();
        engine
            .incr(1, "counters", b"daily", 1, 86_400_000, 1000)
            .unwrap();
        let ttl = engine.get_ttl_ms(1, "counters", b"daily", 1000);
        assert!(ttl.is_some());
        assert!(ttl.unwrap() > 0);
    }

    #[test]
    fn incr_preserves_ttl_when_zero() {
        let mut engine = make_engine();
        // Set key with TTL.
        let bytes = rmp_serde::to_vec(&50i64).unwrap();
        engine.put(1, "counters", b"temp", &bytes, 5000, 1000);
        // Incr with ttl_ms=0 should preserve existing TTL.
        engine.incr(1, "counters", b"temp", 10, 0, 1000).unwrap();
        let ttl = engine.get_ttl_ms(1, "counters", b"temp", 1000);
        assert!(ttl.is_some());
        assert!(ttl.unwrap() > 0);
    }

    #[test]
    fn incr_float_new_key() {
        let mut engine = make_engine();
        let result = engine.incr_float(1, "scores", b"dmg", 3.125, 1000);
        assert!((result.unwrap() - 3.125).abs() < f64::EPSILON);
    }

    #[test]
    fn incr_float_existing() {
        let mut engine = make_engine();
        engine.incr_float(1, "scores", b"dmg", 3.0, 1000).unwrap();
        let result = engine.incr_float(1, "scores", b"dmg", 1.5, 1000);
        assert!((result.unwrap() - 4.5).abs() < f64::EPSILON);
    }

    #[test]
    fn incr_float_infinity_rejected() {
        let mut engine = make_engine();
        let bytes = rmp_serde::to_vec(&f64::MAX).unwrap();
        engine.put(1, "scores", b"big", &bytes, 0, 1000);
        let result = engine.incr_float(1, "scores", b"big", f64::MAX, 1000);
        assert!(matches!(result, Err(AtomicError::Overflow)));
    }

    #[test]
    fn cas_create_if_not_exists() {
        let mut engine = make_engine();
        let result = engine.cas(1, "state", b"player1", b"", b"idle", 1000);
        assert!(result.success);
        assert!(result.current_value.is_none());
        // Verify key was created.
        let val = engine.get(1, "state", b"player1", 1000);
        assert_eq!(val.as_deref(), Some(b"idle".as_slice()));
    }

    #[test]
    fn cas_success() {
        let mut engine = make_engine();
        engine.put(1, "state", b"p1", b"idle", 0, 1000);
        let result = engine.cas(1, "state", b"p1", b"idle", b"in_match", 1000);
        assert!(result.success);
        assert_eq!(result.current_value.as_deref(), Some(b"idle".as_slice()));
        let val = engine.get(1, "state", b"p1", 1000);
        assert_eq!(val.as_deref(), Some(b"in_match".as_slice()));
    }

    #[test]
    fn cas_failure() {
        let mut engine = make_engine();
        engine.put(1, "state", b"p1", b"fighting", 0, 1000);
        let result = engine.cas(1, "state", b"p1", b"idle", b"in_match", 1000);
        assert!(!result.success);
        assert_eq!(
            result.current_value.as_deref(),
            Some(b"fighting".as_slice())
        );
        // Value unchanged.
        let val = engine.get(1, "state", b"p1", 1000);
        assert_eq!(val.as_deref(), Some(b"fighting".as_slice()));
    }

    #[test]
    fn getset_new_key() {
        let mut engine = make_engine();
        let old = engine.getset(1, "session", b"tok", b"new-token", 1000);
        assert!(old.is_none());
        let val = engine.get(1, "session", b"tok", 1000);
        assert_eq!(val.as_deref(), Some(b"new-token".as_slice()));
    }

    #[test]
    fn getset_existing_key() {
        let mut engine = make_engine();
        engine.put(1, "session", b"tok", b"old-token", 0, 1000);
        let old = engine.getset(1, "session", b"tok", b"new-token", 1000);
        assert_eq!(old.as_deref(), Some(b"old-token".as_slice()));
        let val = engine.get(1, "session", b"tok", 1000);
        assert_eq!(val.as_deref(), Some(b"new-token".as_slice()));
    }
}
