// SPDX-License-Identifier: BUSL-1.1

//! Atomic KV operations: INCR, INCR_FLOAT, CAS, GETSET.
//!
//! All operations are atomic within a single TPC core (which owns the key's
//! hash slot). No cross-core coordination is needed because each key maps
//! to exactly one core.

use nodedb_physical::kv_atomic::{AtomicComputeError, compute};
use nodedb_physical::physical_plan::KvCounterShape;

use super::engine::KvEngine;
use super::engine_helpers::{expiry_key, table_key};
use super::entry::NO_EXPIRY;
use super::hash_table::KvHashTable;

/// Result of a compare-and-swap operation.
pub struct CasResult {
    /// The bytes the swap stored. `None` when the compare failed and nothing
    /// was written.
    pub written: Option<Vec<u8>>,
    /// The value that was present at the time of the CAS.
    /// `None` if the key did not exist.
    pub current_value: Option<Vec<u8>>,
}

impl CasResult {
    /// Whether the swap succeeded (current == expected).
    pub fn success(&self) -> bool {
        self.written.is_some()
    }
}

/// Result of a get-and-set operation.
pub struct GetSetResult {
    /// The value that was present before the write. `None` if the key did
    /// not exist.
    pub old: Option<Vec<u8>>,
    /// The bytes the write stored.
    pub written: Vec<u8>,
}

/// The value a counter atomic computed, and the bytes it stored.
///
/// `written` is the whole stored body: the re-encoded row for a typed row,
/// the decimal text for a raw body. A write event carries these bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct Incremented<T> {
    /// The new counter value.
    pub value: T,
    /// The bytes the increment stored.
    pub written: Vec<u8>,
}

/// One `INCR` step: the delta, the TTL request, and the row an absent key
/// becomes.
#[derive(Clone, Copy)]
pub struct IncrStep<'a> {
    /// The signed increment.
    pub delta: i64,
    /// TTL in milliseconds. `0` preserves the existing TTL.
    pub ttl_ms: u64,
    /// The row an absent key becomes.
    pub shape: &'a KvCounterShape,
}

/// Errors specific to atomic KV operations.
#[derive(Debug)]
pub enum AtomicError {
    /// A typed row has no column of the type the atomic reads.
    TypeMismatch { detail: String },
    /// A counter atomic read a stored value it cannot parse as a number, or
    /// computed a result out of range.
    Counter(crate::bridge::envelope::CounterFault),
    /// The computed new value failed to re-encode as MessagePack.
    Encode { detail: String },
    /// The [`AtomicAdmission`] gate refused the computed post-image, so nothing
    /// was written. Boxed to keep the error small on the success path.
    Rejected(Box<crate::Error>),
}

impl From<AtomicComputeError> for AtomicError {
    fn from(error: AtomicComputeError) -> Self {
        match error {
            AtomicComputeError::TypeMismatch { detail } => Self::TypeMismatch { detail },
            AtomicComputeError::Counter(fault) => Self::Counter(fault),
            AtomicComputeError::Encode { detail } => Self::Encode { detail },
        }
    }
}

/// A gate consulted with the computed post-image before an atomic commits.
///
/// Every atomic computes the value it stores from the stored one: INCR runs
/// arithmetic, and CAS and GETSET swap one column of a typed row. The row a
/// row-level-security write policy has to decide does not exist until that
/// computation has run, and it runs here, inside the engine, in the same pass
/// that persists the result. Passing the decision in keeps the computation in
/// one place.
pub type AtomicAdmission<'a> = &'a dyn Fn(&[u8]) -> crate::Result<()>;

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
    /// Atomically increment an i64 value by `delta`. Returns the new value
    /// and the bytes stored.
    ///
    /// - If key doesn't exist: initializes to 0, adds delta, and stores the
    ///   row `shape` names: decimal text, or a typed row.
    /// - A raw body is read as decimal text and written back as decimal
    ///   text. A typed row moves its first integer column in key order.
    /// - A raw body that is not a decimal i64: returns
    ///   `Counter(NotAnInteger)`. A typed row without an integer column:
    ///   returns `TypeMismatch`.
    /// - On i64 overflow: returns `Counter(IntegerOverflow)`. It never wraps.
    /// - TTL behavior: if `ttl_ms > 0` and key is new, sets TTL.
    ///   If key exists and `ttl_ms > 0`, resets TTL. If `ttl_ms == 0`, preserves.
    /// - If `admit` refuses the computed value: returns `Rejected` and writes
    ///   nothing.
    pub fn incr(
        &mut self,
        ctx: AtomicKeyCtx<'_>,
        delta: i64,
        ttl_ms: u64,
        shape: &KvCounterShape,
        admit: AtomicAdmission<'_>,
    ) -> Result<Incremented<i64>, AtomicError> {
        self.incr_resolved(
            ctx,
            IncrStep {
                delta,
                ttl_ms,
                shape,
            },
            None,
            admit,
        )
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
        step: IncrStep<'_>,
        expire_at_ms: u64,
        admit: AtomicAdmission<'_>,
    ) -> Result<Incremented<i64>, AtomicError> {
        self.incr_resolved(ctx, step, Some(expire_at_ms), admit)
    }

    /// Shared INCR body: computes the new value, then installs it via
    /// `atomic_put` with an optional resolved-expiry override. `expire_override`
    /// is only consulted when `ttl_ms > 0` — see `atomic_put`'s doc comment.
    fn incr_resolved(
        &mut self,
        ctx: AtomicKeyCtx<'_>,
        step: IncrStep<'_>,
        expire_override: Option<u64>,
        admit: AtomicAdmission<'_>,
    ) -> Result<Incremented<i64>, AtomicError> {
        let IncrStep {
            delta,
            ttl_ms,
            shape,
        } = step;
        let tkey = table_key(ctx.database_id, ctx.tenant_id, ctx.collection);
        let table = self.ensure_table(tkey, ctx.tenant_id, ctx.collection);

        let current = table.get(ctx.key, ctx.now_ms).map(|v| v.to_vec());
        let (value, written) = compute::incr(current.as_deref(), delta, shape)?;
        // Decided before `atomic_put`, so a refused image is never durable and
        // never reaches the expiry wheel or the secondary indexes.
        admit(&written).map_err(|error| AtomicError::Rejected(Box::new(error)))?;
        self.atomic_put(
            ctx,
            tkey,
            &written,
            ttl_ms,
            current.is_none(),
            expire_override,
        );

        Ok(Incremented { value, written })
    }

    /// Atomically increment an f64 value by `delta`. Returns the new value
    /// and the bytes stored.
    ///
    /// - `delta` is the client's decimal text.
    /// - If key doesn't exist: initializes to 0, adds delta, and stores the
    ///   row `shape` names: decimal text, or a typed row.
    /// - A raw body is read as decimal text and written back as decimal
    ///   text. A typed row moves its first numeric column in key order.
    /// - A raw body that is not a decimal float: returns
    ///   `Counter(NotAFloat)`. A typed row without a numeric column: returns
    ///   `TypeMismatch`.
    /// - A NaN or infinite result: returns `Counter(NonFinite)`.
    /// - If `admit` refuses the computed value: returns `Rejected` and writes
    ///   nothing.
    pub fn incr_float(
        &mut self,
        ctx: AtomicKeyCtx<'_>,
        delta: &str,
        shape: &KvCounterShape,
        admit: AtomicAdmission<'_>,
    ) -> Result<Incremented<f64>, AtomicError> {
        let tkey = table_key(ctx.database_id, ctx.tenant_id, ctx.collection);
        let table = self.ensure_table(tkey, ctx.tenant_id, ctx.collection);

        let current = table.get(ctx.key, ctx.now_ms).map(|v| v.to_vec());
        let (value, written) = compute::incr_float(current.as_deref(), delta, shape)?;
        // Decided before the value is installed — see `incr_resolved`.
        admit(&written).map_err(|error| AtomicError::Rejected(Box::new(error)))?;
        // incr_float always preserves existing TTL (ttl_ms = 0).
        self.atomic_put(ctx, tkey, &written, 0, current.is_none(), None);

        Ok(Incremented { value, written })
    }

    /// Atomic compare-and-swap.
    ///
    /// If current value equals `expected`, sets to `new_value` and returns success.
    /// If current value differs, returns the actual current value.
    /// If key doesn't exist and `expected` is empty, creates the key (create-if-not-exists).
    /// If `admit` refuses the bytes the swap would store: returns `Rejected`
    /// and writes nothing.
    pub fn cas(
        &mut self,
        ctx: AtomicKeyCtx<'_>,
        expected: &[u8],
        new_value: &[u8],
        admit: AtomicAdmission<'_>,
    ) -> Result<CasResult, AtomicError> {
        let tkey = table_key(ctx.database_id, ctx.tenant_id, ctx.collection);
        let table = self.ensure_table(tkey, ctx.tenant_id, ctx.collection);

        let current = table.get(ctx.key, ctx.now_ms).map(|v| v.to_vec());

        let (matches, write_bytes) = compute::cas(current.as_deref(), expected, new_value)?;
        if !matches {
            return Ok(CasResult {
                written: None,
                current_value: current,
            });
        }
        // Decided before the value is installed — see `incr_resolved`.
        admit(&write_bytes).map_err(|error| AtomicError::Rejected(Box::new(error)))?;
        self.atomic_put(ctx, tkey, &write_bytes, 0, current.is_none(), None);
        Ok(CasResult {
            written: Some(write_bytes),
            current_value: current,
        })
    }

    /// Atomic get-and-set: sets new value, returns old value.
    ///
    /// If key didn't exist, `old` is `None`.
    /// Preserves existing TTL.
    /// If `admit` refuses the bytes the write would store: returns `Rejected`
    /// and writes nothing.
    pub fn getset(
        &mut self,
        ctx: AtomicKeyCtx<'_>,
        new_value: &[u8],
        admit: AtomicAdmission<'_>,
    ) -> Result<GetSetResult, AtomicError> {
        let tkey = table_key(ctx.database_id, ctx.tenant_id, ctx.collection);
        let table = self.ensure_table(tkey, ctx.tenant_id, ctx.collection);
        let old = table.get(ctx.key, ctx.now_ms).map(|v| v.to_vec());
        let write_bytes = compute::getset(old.as_deref(), new_value)?;
        // Decided before the value is installed — see `incr_resolved`.
        admit(&write_bytes).map_err(|error| AtomicError::Rejected(Box::new(error)))?;

        // GetSet preserves existing TTL (ttl_ms = 0).
        self.atomic_put(ctx, tkey, &write_bytes, 0, old.is_none(), None);
        Ok(GetSetResult {
            old,
            written: write_bytes,
        })
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
    use crate::bridge::envelope::CounterFault;

    static RAW: KvCounterShape = KvCounterShape::Raw;

    /// A raw-shaped `INCR` step.
    fn step(delta: i64, ttl_ms: u64) -> IncrStep<'static> {
        IncrStep {
            delta,
            ttl_ms,
            shape: &RAW,
        }
    }

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
        let result = engine
            .incr(ctx("counters", b"hits"), 10, 0, &RAW, &admit_any)
            .expect("incr");
        assert_eq!(result.value, 10);
        assert_eq!(result.written, b"10".to_vec());
        assert_eq!(
            engine.get(0, 1, "counters", b"hits", 1000).as_deref(),
            Some(b"10".as_slice()),
            "the engine stores exactly the bytes it returns"
        );
    }

    #[test]
    fn incr_existing_key() {
        let mut engine = make_engine();
        engine
            .incr(ctx("counters", b"hits"), 10, 0, &RAW, &admit_any)
            .unwrap();
        let result = engine.incr(ctx("counters", b"hits"), 5, 0, &RAW, &admit_any);
        assert_eq!(result.expect("incr").value, 15);
    }

    #[test]
    fn incr_negative_delta() {
        let mut engine = make_engine();
        engine
            .incr(ctx("counters", b"gold"), 100, 0, &RAW, &admit_any)
            .unwrap();
        let result = engine.incr(ctx("counters", b"gold"), -30, 0, &RAW, &admit_any);
        assert_eq!(result.expect("incr").value, 70);
    }

    /// The increment is computed inside the engine, so the gate is the only
    /// place the resulting row can be decided — and a refusal must leave the
    /// stored value exactly as it was.
    #[test]
    fn a_refused_increment_writes_nothing() {
        let mut engine = make_engine();
        engine
            .incr(ctx("counters", b"hits"), 7, 0, &RAW, &admit_any)
            .unwrap();

        let deny = |_: &[u8]| {
            Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(1),
                resource: "test".into(),
            })
        };
        let result = engine.incr(ctx("counters", b"hits"), 5, 0, &RAW, &deny);
        assert!(matches!(result, Err(AtomicError::Rejected(_))));

        let stored = engine
            .get(0, 1, "counters", b"hits", 1000)
            .expect("the refused increment must leave the prior row in place");
        assert_eq!(
            stored,
            b"7".to_vec(),
            "a refused increment must not be applied"
        );
    }

    #[test]
    fn incr_overflow() {
        let mut engine = make_engine();
        // Set to MAX.
        let bytes = i64::MAX.to_string().into_bytes();
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
        let result = engine.incr(ctx("counters", b"max"), 1, 0, &RAW, &admit_any);
        assert!(matches!(
            result,
            Err(AtomicError::Counter(CounterFault::IntegerOverflow))
        ));
    }

    #[test]
    fn incr_on_raw_text_that_is_not_an_integer_is_refused() {
        let mut engine = make_engine();
        let bytes = b"hello".to_vec();
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
        let result = engine.incr(ctx("counters", b"str"), 1, 0, &RAW, &admit_any);
        assert!(matches!(
            result,
            Err(AtomicError::Counter(CounterFault::NotAnInteger))
        ));
    }

    #[test]
    fn incr_with_ttl_new_key() {
        let mut engine = make_engine();
        engine
            .incr(ctx("counters", b"daily"), 1, 86_400_000, &RAW, &admit_any)
            .unwrap();
        let ttl = engine.get_ttl_ms(0, 1, "counters", b"daily", 1000);
        assert!(ttl.is_some());
        assert!(ttl.unwrap() > 0);
    }

    #[test]
    fn incr_preserves_ttl_when_zero() {
        let mut engine = make_engine();
        // Set key with TTL.
        let bytes = b"50".to_vec();
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
            .incr(ctx("counters", b"temp"), 10, 0, &RAW, &admit_any)
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
            .incr_with_absolute_expiry(
                ctx("counters", b"daily"),
                step(1, 5_000),
                1_000_000,
                &admit_any,
            )
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
        let bytes = b"50".to_vec();
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
            .incr_with_absolute_expiry(
                ctx("counters", b"temp"),
                step(10, 0),
                999_999_999,
                &admit_any,
            )
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
        let result = engine
            .incr_float(ctx("scores", b"dmg"), "3.125", &RAW, &admit_any)
            .expect("incr_float");
        assert!((result.value - 3.125).abs() < f64::EPSILON);
        assert_eq!(result.written, b"3.125".to_vec());
    }

    #[test]
    fn incr_float_existing() {
        let mut engine = make_engine();
        engine
            .incr_float(ctx("scores", b"dmg"), "3.0", &RAW, &admit_any)
            .unwrap();
        let result = engine
            .incr_float(ctx("scores", b"dmg"), "1.5", &RAW, &admit_any)
            .expect("incr_float");
        assert!((result.value - 4.5).abs() < f64::EPSILON);
        assert_eq!(result.written, b"4.5".to_vec());
    }

    #[test]
    fn incr_float_infinity_rejected() {
        let mut engine = make_engine();
        let bytes_text = f64::MAX.to_string();
        let bytes = bytes_text.clone().into_bytes();
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
        let result = engine.incr_float(ctx("scores", b"big"), &bytes_text, &RAW, &admit_any);
        assert!(matches!(
            result,
            Err(AtomicError::Counter(CounterFault::NonFinite))
        ));
    }

    #[test]
    fn cas_create_if_not_exists() {
        let mut engine = make_engine();
        let result = engine
            .cas(ctx("state", b"player1"), b"", b"idle", &admit_any)
            .expect("cas");
        assert!(result.success());
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
        let result = engine
            .cas(ctx("state", b"p1"), b"idle", b"in_match", &admit_any)
            .expect("cas");
        assert!(result.success());
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
        let result = engine
            .cas(ctx("state", b"p1"), b"idle", b"in_match", &admit_any)
            .expect("cas");
        assert!(!result.success());
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
        let old = engine
            .getset(ctx("session", b"tok"), b"new-token", &admit_any)
            .expect("getset")
            .old;
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
        let old = engine
            .getset(ctx("session", b"tok"), b"new-token", &admit_any)
            .expect("getset")
            .old;
        assert_eq!(old.as_deref(), Some(b"old-token".as_slice()));
        let val = engine.get(0, 1, "session", b"tok", 1000);
        assert_eq!(val.as_deref(), Some(b"new-token".as_slice()));
    }
}
