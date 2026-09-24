// SPDX-License-Identifier: BUSL-1.1

//! Bounded retry for collection-purge reclaim operations.

use tracing::info;

/// Bounded retry wrapper for collection-purge reclaim ops.
///
/// Runs the op up to `MAX_ATTEMPTS` times; returns the first `Ok(T)`
/// it sees. No sleep between attempts — the Data Plane is single-threaded
/// per core and a `sleep` here would stall every other request on the
/// shard; an immediate retry still recovers the vast majority of
/// transient fs-level errors (momentary lock, inflight fsync race).
///
/// **Fail-closed:** after exhausting all attempts this returns the last
/// error rather than swallowing it. The purge path must not warn-and-
/// continue: a partially-purged collection whose catalog row is then
/// removed leaves addressable storage rows that a re-CREATE of the same
/// name would resurrect. The caller propagates the error so the DROP
/// fails and the collection remains fully intact for the next attempt.
const L1_RECLAIM_MAX_ATTEMPTS: u32 = 3;

pub(in crate::data::executor) fn retry_reclaim<T, E, F>(
    op_name: &str,
    tenant_id: u64,
    collection: &str,
    mut op: F,
) -> crate::Result<T>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    let mut last_err: Option<String> = None;
    for attempt in 1..=L1_RECLAIM_MAX_ATTEMPTS {
        match op() {
            Ok(v) => {
                if attempt > 1 {
                    info!(
                        tenant_id,
                        collection,
                        op = op_name,
                        attempt,
                        "collection-purge reclaim recovered after transient failure"
                    );
                }
                return Ok(v);
            }
            Err(e) => {
                last_err = Some(e.to_string());
            }
        }
    }
    Err(crate::Error::Storage {
        engine: "collection-purge".into(),
        detail: format!(
            "reclaim op '{op_name}' for tenant {tenant_id} collection '{collection}' \
             failed after {L1_RECLAIM_MAX_ATTEMPTS} attempts: {}",
            last_err.as_deref().unwrap_or("(no detail)")
        ),
    })
}
