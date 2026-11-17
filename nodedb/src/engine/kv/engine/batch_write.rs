//! Batch PUT operations for the KV engine.

use super::KvEngine;
use crate::engine::kv::batch_put::KvBatchPutParams;
use crate::engine::kv::engine_write::{KvPutParams, UnboundKvWrite};
use crate::engine::kv::write_refusal::KvWriteRefusal;

impl KvEngine {
    /// BATCH PUT: insert/update multiple pairs. Returns count of new keys.
    ///
    /// `surrogates` carries each entry's bound cross-engine identity, same
    /// order and length as `entries` -- assigned by the CP-side
    /// `SurrogateAssigner` from `(collection, key)`, same mechanism as a
    /// single-key `put`. A batch with a missing or `Surrogate::ZERO`
    /// surrogate, or with an entry over the row limit, is refused whole with
    /// [`KvWriteRefusal`] before any entry is written.
    pub fn batch_put(&mut self, params: KvBatchPutParams<'_>) -> Result<usize, KvWriteRefusal> {
        check_batch(&params)?;
        let KvBatchPutParams {
            database_id,
            tenant_id,
            collection,
            entries,
            ttl_ms,
            now_ms,
            surrogates,
        } = params;
        // One apply scope: every row of the batch stamps one ordinal.
        let guard = self.open_write_scope(None);
        let mut written = Ok(0);
        for ((key, value), &surrogate) in entries.iter().zip(surrogates) {
            let put = self.put(KvPutParams {
                database_id,
                tenant_id,
                collection,
                key: key.as_slice(),
                value: value.as_slice(),
                ttl_ms,
                now_ms,
                surrogate,
            });
            if !count_new_row(&mut written, put) {
                break;
            }
        }
        self.close_write_scope(guard);
        written
    }

    /// BATCH PUT installing an already-resolved absolute expiry instant on
    /// every entry. Mirrors [`KvEngine::put_with_absolute_expiry`]: WAL redo
    /// replay uses this so a TTL'd batch recovers with the exact expiry the
    /// original write computed, rather than recomputing `now_ms + ttl_ms` at
    /// recovery time (which would push expiry forward by the crash-to-restart
    /// delay). `params.ttl_ms` is carried through `put_with_absolute_expiry`
    /// only for `KvPutParams`'s shape; the installed expiry is `expire_at_ms`
    /// verbatim, same for every entry in the batch.
    pub fn batch_put_with_absolute_expiry(
        &mut self,
        params: KvBatchPutParams<'_>,
        expire_at_ms: u64,
    ) -> Result<usize, KvWriteRefusal> {
        check_batch(&params)?;
        let KvBatchPutParams {
            database_id,
            tenant_id,
            collection,
            entries,
            ttl_ms,
            now_ms,
            surrogates,
        } = params;
        // One apply scope: every row of the batch stamps one ordinal.
        let guard = self.open_write_scope(None);
        let mut written = Ok(0);
        for ((key, value), &surrogate) in entries.iter().zip(surrogates) {
            let put = self.put_with_absolute_expiry(
                KvPutParams {
                    database_id,
                    tenant_id,
                    collection,
                    key: key.as_slice(),
                    value: value.as_slice(),
                    ttl_ms,
                    now_ms,
                    surrogate,
                },
                expire_at_ms,
            );
            if !count_new_row(&mut written, put) {
                break;
            }
        }
        self.close_write_scope(guard);
        written
    }
}

/// Fold one row's put into the batch's running result: count a new row, or
/// keep the refusal. Returns whether the batch continues.
fn count_new_row(
    written: &mut Result<usize, KvWriteRefusal>,
    put: Result<Option<Vec<u8>>, KvWriteRefusal>,
) -> bool {
    match put {
        Err(refusal) => {
            *written = Err(refusal);
            false
        }
        Ok(old) => match written {
            Ok(count) => {
                if old.is_none() {
                    *count += 1;
                }
                true
            }
            Err(_) => false,
        },
    }
}

/// The refusal of a batch that does not carry one bound surrogate per entry,
/// or that holds an entry over the row limit.
fn check_batch(params: &KvBatchPutParams<'_>) -> Result<(), KvWriteRefusal> {
    if params.surrogates.len() != params.entries.len() {
        return Err(UnboundKvWrite {
            collection: params.collection.to_owned(),
        }
        .into());
    }
    params
        .entries
        .iter()
        .zip(params.surrogates)
        .try_for_each(|((key, value), &surrogate)| {
            KvWriteRefusal::check(params.collection, key, value, surrogate)
        })
}
