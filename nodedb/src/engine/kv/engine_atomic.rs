// SPDX-License-Identifier: BUSL-1.1

//! Atomic KV operations: INCR, INCR_FLOAT, CAS, GETSET.
//!
//! All operations are atomic within a single TPC core (which owns the key's
//! hash slot). No cross-core coordination is needed because each key maps
//! to exactly one core.

use super::engine::KvEngine;
use super::engine_atomic_compute as compute;
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
    /// The computed new value failed to re-encode as MessagePack.
    Encode { detail: String },
    /// The [`IncrAdmission`] gate refused the computed post-image, so nothing
    /// was written. Boxed to keep the error small on the success path.
    Rejected(Box<crate::Error>),
}

/// A gate consulted with the computed post-image before an increment commits.
///
/// INCR computes the value it stores from the stored one, so the row a
/// row-level-security write policy has to decide does not exist until the
/// arithmetic has run — and the arithmetic runs here, inside the engine, in the
/// same pass that persists the result. Passing the decision in is what keeps
/// that arithmetic in one place: pre-computing the increment at the call site
/// just to check it would leave two copies of it to drift apart.
pub type IncrAdmission<'a> = &'a dyn Fn(&[u8]) -> crate::Result<()>;

/// An admission that accepts every image.
///
/// For replaying a write that was already decided: a WAL redo re-applies a
/// write whose policy verdict was reached when it was first accepted, and
/// re-deciding it against the *current* session's policies would make recovery
/// depend on who happens to be connected.
pub fn admit_any(_image: &[u8]) -> crate::Result<()> {
    Ok(())
}

/// Shared key-identity context for a single-key atomic KV operation
/// (INCR / INCR_FLOAT / CAS / GETSET).
#[derive(Clone, Copy)]
pub struct AtomicKeyCtx<'a> {
    /// Owning database.
    pub database_id: u64,
    /// Owning tenant.
    pub tenant_id: u64,
    /// Collection name.
    pub collection: &'a str,
    /// Key bytes.
    pub key: &'a [u8],
    /// Current time in milliseconds, used for TTL/expiry evaluation.
    pub now_ms: u64,
    /// Global cross-engine surrogate assigned to this write.
    pub surrogate: nodedb_types::Surrogate,
}

impl KvEngine {
    /// Atomically increment an i64 value by `delta`. Returns the new value.
    ///
    /// - If key doesn't exist: initializes to 0, adds delta, returns delta.
    /// - If value is not a MessagePack integer: returns `TypeMismatch`.
    /// - On i64 overflow: returns `Overflow` (never wraps silently).
    /// - TTL behavior: if `ttl_ms > 0` and key is new, sets TTL.
    ///   If key exists and `ttl_ms > 0`, resets TTL. If `ttl_ms == 0`, preserves.
    /// - If `admit` refuses the computed value: returns `Rejected` and writes
    ///   nothing.
    pub fn incr(
        &mut self,
        ctx: AtomicKeyCtx<'_>,
        delta: i64,
        ttl_ms: u64,
        admit: IncrAdmission<'_>,
    ) -> Result<i64, AtomicError> {
        self.incr_resolved(ctx, delta, ttl_ms, None, admit)
    }

    /// Atomically increment an i64 value by `delta`, installing an
    /// already-resolved absolute `expire_at_ms` instant instead of deriving
    /// one as `now_ms + ttl_ms`.
    ///
    /// Only meaningful when `ttl_ms > 0` — WAL redo replay uses this so a
    /// TTL'd `INCR`'s expiry recovers with the exact instant the original
    /// write computed, rather than recomputing `now_ms + ttl_ms` at recovery
    /// time (which would push expiry forward by the crash-to-restart delay).
    /// When `ttl_ms == 0`, `expire_at_ms` is ignored and the existing TTL is
    /// preserved, exactly as [`incr`] preserves it.
    ///
    /// [`incr`]: KvEngine::incr
    pub fn incr_with_absolute_expiry(
        &mut self,
        ctx: AtomicKeyCtx<'_>,
        delta: i64,
        ttl_ms: u64,
        expire_at_ms: u64,
        admit: IncrAdmission<'_>,
    ) -> Result<i64, AtomicError> {
        self.incr_resolved(ctx, delta, ttl_ms, Some(expire_at_ms), admit)
    }

    /// Shared INCR body: computes the new value, then installs it via
    /// `atomic_put` with an optional resolved-expiry override. `expire_override`
    /// is only consulted when `ttl_ms > 0` — see `atomic_put`'s doc comment.
    fn incr_resolved(
        &mut self,
        ctx: AtomicKeyCtx<'_>,
        delta: i64,
        ttl_ms: u64,
        expire_override: Option<u64>,
        admit: IncrAdmission<'_>,
    ) -> Result<i64, AtomicError> {
        let tkey = table_key(ctx.database_id, ctx.tenant_id, ctx.collection);
        let table = self.ensure_table(tkey, ctx.tenant_id, ctx.collection);

        let current = table.get(ctx.key, ctx.now_ms).map(|v| v.to_vec());
        let (new_i64, new_bytes) = compute::incr(current.as_deref(), delta)?;
        // Decided before `atomic_put`, so a refused image is never durable and
        // never reaches the expiry wheel or the secondary indexes.
        admit(&new_bytes).map_err(|error| AtomicError::Rejected(Box::new(error)))?;
        self.atomic_put(
            ctx,
            tkey,
            &new_bytes,
            ttl_ms,
            current.is_none(),
            expire_override,
        );

        Ok(new_i64)
    }

    /// Atomically increment an f64 value by `delta`. Returns the new value.
    ///
    /// - If key doesn't exist: initializes to 0.0, adds delta, returns delta.
    /// - If value is not a MessagePack float or integer: returns `TypeMismatch`.
    /// - f64 does not overflow in the traditional sense (it goes to infinity),
    ///   but NaN/Infinity results are rejected as `Overflow`.
    /// - If `admit` refuses the computed value: returns `Rejected` and writes
    ///   nothing.
    pub fn incr_float(
        &mut self,
        ctx: AtomicKeyCtx<'_>,
        delta: f64,
        admit: IncrAdmission<'_>,
    ) -> Result<f64, AtomicError> {
        let tkey = table_key(ctx.database_id, ctx.tenant_id, ctx.collection);
        let table = self.ensure_table(tkey, ctx.tenant_id, ctx.collection);

        let current = table.get(ctx.key, ctx.now_ms).map(|v| v.to_vec());
        let (new_f64, new_bytes) = compute::incr_float(current.as_deref(), delta)?;
        // Decided before the value is installed — see `incr_resolved`.
        admit(&new_bytes).map_err(|error| AtomicError::Rejected(Box::new(error)))?;
        // incr_float always preserves existing TTL (ttl_ms = 0).
        self.atomic_put(ctx, tkey, &new_bytes, 0, current.is_none(), None);

        Ok(new_f64)
    }

    /// Atomic compare-and-swap.
    ///
    /// If current value equals `expected`, sets to `new_value` and returns success.
    /// If current value differs, returns the actual current value.
    /// If key doesn't exist and `expected` is empty, creates the key (create-if-not-exists).
    pub fn cas(&mut self, ctx: AtomicKeyCtx<'_>, expected: &[u8], new_value: &[u8]) -> CasResult {
        let tkey = table_key(ctx.database_id, ctx.tenant_id, ctx.collection);
        let table = self.ensure_table(tkey, ctx.tenant_id, ctx.collection);

        let current = table.get(ctx.key, ctx.now_ms).map(|v| v.to_vec());

        let (matches, write_bytes) = compute::cas(current.as_deref(), expected, new_value);

        if matches {
            self.atomic_put(ctx, tkey, &write_bytes, 0, current.is_none(), None);
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
    pub fn getset(&mut self, ctx: AtomicKeyCtx<'_>, new_value: &[u8]) -> Option<Vec<u8>> {
        let tkey = table_key(ctx.database_id, ctx.tenant_id, ctx.collection);
        let table = self.ensure_table(tkey, ctx.tenant_id, ctx.collection);
        let old = table.get(ctx.key, ctx.now_ms).map(|v| v.to_vec());
        let write_bytes = compute::getset(old.as_deref(), new_value);

        // GetSet preserves existing TTL (ttl_ms = 0).
        self.atomic_put(ctx, tkey, &write_bytes, 0, old.is_none(), None);
        old
    }

    /// Ensure a hash table exists for (tenant, collection), creating if needed.
    /// Returns a mutable reference to the table.
    fn ensure_table(&mut self, tkey: u64, tenant_id: u64, collection: &str) -> &mut KvHashTable {
        self.hash_to_tenant.entry(tkey).or_insert(tenant_id);
        self.hash_to_collection
            .entry(tkey)
            .or_insert_with(|| collection.to_string());
        let default_capacity = self.default_capacity;
        let load_factor_threshold = self.load_factor_threshold;
        let rehash_batch_size = self.rehash_batch_size;
        let inline_threshold = self.inline_threshold;
        self.tables.entry(tkey).or_insert_with(|| {
            KvHashTable::new(
                default_capacity,
                load_factor_threshold,
                rehash_batch_size,
                inline_threshold,
            )
        })
    }

    /// Internal helper: put a value into the hash table, handling TTL and expiry.
    ///
    /// If `ttl_ms == 0`, preserves the existing TTL on an existing key —
    /// `expire_override` is ignored entirely in this case, so a caller that
    /// passes `Some(..)` alongside `ttl_ms == 0` cannot accidentally install
    /// an absolute instant into the preserve branch.
    /// If `ttl_ms > 0`, installs `expire_override` verbatim when given
    /// (WAL redo replay uses this so a TTL survives crash-restart with the
    /// exact instant the original write resolved), otherwise derives
    /// `now_ms + ttl_ms` the way a live write does.
    fn atomic_put(
        &mut self,
        ctx: AtomicKeyCtx<'_>,
        tkey: u64,
        value: &[u8],
        ttl_ms: u64,
        is_new_key: bool,
        expire_override: Option<u64>,
    ) {
        let AtomicKeyCtx {
            database_id,
            tenant_id,
            collection,
            key,
            now_ms,
            surrogate,
        } = ctx;
        // Cache metadata lookup to avoid double HashMap access.
        let old_meta = if is_new_key {
            None
        } else {
            self.tables.get(&tkey).and_then(|t| t.get_entry_meta(key))
        };

        // Determine the target expire_at.
        let expire_at = if ttl_ms > 0 {
            // Explicit TTL: install the caller-resolved absolute instant if
            // given (replay), otherwise derive it live.
            expire_override.unwrap_or(now_ms + ttl_ms)
        } else if let Some(ref meta) = old_meta {
            // Existing key, preserve TTL.
            meta.expire_at_ms
        } else {
            // New key with no TTL request: persistent.
            NO_EXPIRY
        };

        // Cancel old expiry before mutation.
        if let Some(ref meta) = old_meta
            && meta.has_ttl
        {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.cancel(&composite, meta.expire_at_ms);
        }

        let has_secondary_indexes = self.indexes.get(&tkey).is_some_and(|idx| !idx.is_empty());
        // Sorted indexes are maintained here too. Every atomic KV write —
        // `UPDATE ... SET`, `INCR`, `CAS`, `GETSET`, `TRANSFER`, and upsert's
        // conflict branch — reaches the store through this one body, so an
        // index refreshed only by `KvEngine::put` would keep answering `TOPK`
        // and `RANK` from the pre-update score of every row any of them
        // touched, with nothing to signal the divergence.
        let has_sorted_indexes = self.sorted_indexes.has_indexes(tkey);

        // Extract old field values BEFORE overwriting — needed so on_put can
        // remove stale index entries when a field changes. The sorted index
        // re-keys a primary key in place and needs no before-image.
        let old_fields = if !is_new_key && has_secondary_indexes {
            self.tables
                .get(&tkey)
                .and_then(|t| t.get(key, now_ms))
                .map(|old_val| {
                    super::engine_helpers::extract_all_field_values_from_msgpack(old_val)
                })
        } else {
            None
        };

        // Write the value, bumping this table's write epoch — the single
        // chokepoint the aggregate result cache reads to detect this write.
        let table = self.table_for_write_or_create(tkey, tenant_id, collection);
        table.put(key, value, expire_at, surrogate);

        // Schedule new expiry if needed.
        if expire_at != NO_EXPIRY {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.insert(composite, expire_at);
        }

        // Index maintenance — the same pair `KvEngine::put` performs, over the
        // one field extraction both kinds read.
        if has_secondary_indexes || has_sorted_indexes {
            let new_fields = super::engine_helpers::extract_all_field_values_from_msgpack(value);

            if has_secondary_indexes {
                let old_refs: Option<Vec<(&str, &[u8])>> = old_fields.as_ref().map(|fields| {
                    fields
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_slice()))
                        .collect()
                });
                let new_refs: Vec<(&str, &[u8])> = new_fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_slice()))
                    .collect();
                if let Some(idx_set) = self.indexes.get_mut(&tkey) {
                    idx_set.on_put(key, &new_refs, old_refs.as_deref());
                }
            }

            if has_sorted_indexes {
                self.sorted_indexes.on_put(tkey, key, &new_fields);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::super::engine_write::KvPutParams;
    use super::*;

    fn make_engine() -> KvEngine {
        KvEngine::new(1000, 16, 0.75, 4, 64, 1000, 1024)
    }

    /// Build a key context for tests (database 0, tenant 1, now_ms 1000).
    fn ctx<'a>(collection: &'a str, key: &'a [u8]) -> AtomicKeyCtx<'a> {
        AtomicKeyCtx {
            database_id: 0,
            tenant_id: 1,
            collection,
            key,
            now_ms: 1000,
            surrogate: Surrogate::ZERO,
        }
    }

    #[test]
    fn incr_new_key() {
        let mut engine = make_engine();
        let result = engine.incr(ctx("counters", b"hits"), 10, 0, &admit_any);
        assert_eq!(result.unwrap(), 10);
    }

    #[test]
    fn incr_existing_key() {
        let mut engine = make_engine();
        engine
            .incr(ctx("counters", b"hits"), 10, 0, &admit_any)
            .unwrap();
        let result = engine.incr(ctx("counters", b"hits"), 5, 0, &admit_any);
        assert_eq!(result.unwrap(), 15);
    }

    #[test]
    fn incr_negative_delta() {
        let mut engine = make_engine();
        engine
            .incr(ctx("counters", b"gold"), 100, 0, &admit_any)
            .unwrap();
        let result = engine.incr(ctx("counters", b"gold"), -30, 0, &admit_any);
        assert_eq!(result.unwrap(), 70);
    }

    /// The increment is computed inside the engine, so the gate is the only
    /// place the resulting row can be decided — and a refusal must leave the
    /// stored value exactly as it was.
    #[test]
    fn a_refused_increment_writes_nothing() {
        let mut engine = make_engine();
        engine
            .incr(ctx("counters", b"hits"), 7, 0, &admit_any)
            .unwrap();

        let deny = |_: &[u8]| {
            Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(1),
                resource: "test".into(),
            })
        };
        let result = engine.incr(ctx("counters", b"hits"), 5, 0, &deny);
        assert!(matches!(result, Err(AtomicError::Rejected(_))));

        let stored = engine
            .get(0, 1, "counters", b"hits", 1000)
            .expect("the refused increment must leave the prior row in place");
        let value: i64 = zerompk::from_msgpack(&stored).unwrap();
        assert_eq!(value, 7, "a refused increment must not be applied");
    }

    #[test]
    fn incr_overflow() {
        let mut engine = make_engine();
        // Set to MAX.
        let bytes = zerompk::to_msgpack_vec(&i64::MAX).unwrap();
        engine.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "counters",
            key: b"max",
            value: &bytes,
            ttl_ms: 0,
            now_ms: 1000,
            surrogate: Surrogate::ZERO,
        });
        let result = engine.incr(ctx("counters", b"max"), 1, 0, &admit_any);
        assert!(matches!(result, Err(AtomicError::Overflow)));
    }

    #[test]
    fn incr_type_mismatch() {
        let mut engine = make_engine();
        let bytes = zerompk::to_msgpack_vec(&"hello").unwrap();
        engine.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "counters",
            key: b"str",
            value: &bytes,
            ttl_ms: 0,
            now_ms: 1000,
            surrogate: Surrogate::ZERO,
        });
        let result = engine.incr(ctx("counters", b"str"), 1, 0, &admit_any);
        assert!(matches!(result, Err(AtomicError::TypeMismatch { .. })));
    }

    #[test]
    fn incr_with_ttl_new_key() {
        let mut engine = make_engine();
        engine
            .incr(ctx("counters", b"daily"), 1, 86_400_000, &admit_any)
            .unwrap();
        let ttl = engine.get_ttl_ms(0, 1, "counters", b"daily", 1000);
        assert!(ttl.is_some());
        assert!(ttl.unwrap() > 0);
    }

    #[test]
    fn incr_preserves_ttl_when_zero() {
        let mut engine = make_engine();
        // Set key with TTL.
        let bytes = zerompk::to_msgpack_vec(&50i64).unwrap();
        engine.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "counters",
            key: b"temp",
            value: &bytes,
            ttl_ms: 5000,
            now_ms: 1000,
            surrogate: Surrogate::ZERO,
        });
        // Incr with ttl_ms=0 should preserve existing TTL.
        engine
            .incr(ctx("counters", b"temp"), 10, 0, &admit_any)
            .unwrap();
        let ttl = engine.get_ttl_ms(0, 1, "counters", b"temp", 1000);
        assert!(ttl.is_some());
        assert!(ttl.unwrap() > 0);
    }

    #[test]
    fn incr_with_absolute_expiry_installs_recorded_instant_not_now_plus_ttl() {
        let mut engine = make_engine();
        // now_ms in `ctx()` is 1000; a live derivation would install
        // 1000 + 5000 = 6000. Passing an explicit absolute instant must
        // override that derivation entirely.
        engine
            .incr_with_absolute_expiry(ctx("counters", b"daily"), 1, 5_000, 1_000_000, &admit_any)
            .unwrap();
        let ttl = engine.get_ttl_ms(0, 1, "counters", b"daily", 1000);
        assert_eq!(
            ttl,
            Some(1_000_000 - 1000),
            "must install the caller-supplied absolute instant verbatim, not now_ms + ttl_ms"
        );
    }

    #[test]
    fn incr_with_absolute_expiry_and_zero_ttl_still_preserves_existing_expiry() {
        let mut engine = make_engine();
        let bytes = zerompk::to_msgpack_vec(&50i64).unwrap();
        engine.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "counters",
            key: b"temp",
            value: &bytes,
            ttl_ms: 5000,
            now_ms: 1000,
            surrogate: Surrogate::ZERO,
        });
        let ttl_before = engine.get_ttl_ms(0, 1, "counters", b"temp", 1000);

        // ttl_ms == 0 must ignore the supplied absolute instant and preserve
        // the existing expiry exactly as `incr` does.
        engine
            .incr_with_absolute_expiry(ctx("counters", b"temp"), 10, 0, 999_999_999, &admit_any)
            .unwrap();
        let ttl_after = engine.get_ttl_ms(0, 1, "counters", b"temp", 1000);
        assert_eq!(
            ttl_before, ttl_after,
            "ttl_ms == 0 must preserve the existing expiry, ignoring expire_override"
        );
    }

    #[test]
    fn incr_float_new_key() {
        let mut engine = make_engine();
        let result = engine.incr_float(ctx("scores", b"dmg"), 3.125, &admit_any);
        assert!((result.unwrap() - 3.125).abs() < f64::EPSILON);
    }

    #[test]
    fn incr_float_existing() {
        let mut engine = make_engine();
        engine
            .incr_float(ctx("scores", b"dmg"), 3.0, &admit_any)
            .unwrap();
        let result = engine.incr_float(ctx("scores", b"dmg"), 1.5, &admit_any);
        assert!((result.unwrap() - 4.5).abs() < f64::EPSILON);
    }

    #[test]
    fn incr_float_infinity_rejected() {
        let mut engine = make_engine();
        let bytes = zerompk::to_msgpack_vec(&f64::MAX).unwrap();
        engine.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "scores",
            key: b"big",
            value: &bytes,
            ttl_ms: 0,
            now_ms: 1000,
            surrogate: Surrogate::ZERO,
        });
        let result = engine.incr_float(ctx("scores", b"big"), f64::MAX, &admit_any);
        assert!(matches!(result, Err(AtomicError::Overflow)));
    }

    #[test]
    fn cas_create_if_not_exists() {
        let mut engine = make_engine();
        let result = engine.cas(ctx("state", b"player1"), b"", b"idle");
        assert!(result.success);
        assert!(result.current_value.is_none());
        // Verify key was created.
        let val = engine.get(0, 1, "state", b"player1", 1000);
        assert_eq!(val.as_deref(), Some(b"idle".as_slice()));
    }

    #[test]
    fn cas_success() {
        let mut engine = make_engine();
        engine.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "state",
            key: b"p1",
            value: b"idle",
            ttl_ms: 0,
            now_ms: 1000,
            surrogate: Surrogate::ZERO,
        });
        let result = engine.cas(ctx("state", b"p1"), b"idle", b"in_match");
        assert!(result.success);
        assert_eq!(result.current_value.as_deref(), Some(b"idle".as_slice()));
        let val = engine.get(0, 1, "state", b"p1", 1000);
        assert_eq!(val.as_deref(), Some(b"in_match".as_slice()));
    }

    #[test]
    fn cas_failure() {
        let mut engine = make_engine();
        engine.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "state",
            key: b"p1",
            value: b"fighting",
            ttl_ms: 0,
            now_ms: 1000,
            surrogate: Surrogate::ZERO,
        });
        let result = engine.cas(ctx("state", b"p1"), b"idle", b"in_match");
        assert!(!result.success);
        assert_eq!(
            result.current_value.as_deref(),
            Some(b"fighting".as_slice())
        );
        // Value unchanged.
        let val = engine.get(0, 1, "state", b"p1", 1000);
        assert_eq!(val.as_deref(), Some(b"fighting".as_slice()));
    }

    #[test]
    fn getset_new_key() {
        let mut engine = make_engine();
        let old = engine.getset(ctx("session", b"tok"), b"new-token");
        assert!(old.is_none());
        let val = engine.get(0, 1, "session", b"tok", 1000);
        assert_eq!(val.as_deref(), Some(b"new-token".as_slice()));
    }

    #[test]
    fn getset_existing_key() {
        let mut engine = make_engine();
        engine.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "session",
            key: b"tok",
            value: b"old-token",
            ttl_ms: 0,
            now_ms: 1000,
            surrogate: Surrogate::ZERO,
        });
        let old = engine.getset(ctx("session", b"tok"), b"new-token");
        assert_eq!(old.as_deref(), Some(b"old-token".as_slice()));
        let val = engine.get(0, 1, "session", b"tok", 1000);
        assert_eq!(val.as_deref(), Some(b"new-token".as_slice()));
    }
}
