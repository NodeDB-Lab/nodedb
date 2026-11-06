// SPDX-License-Identifier: BUSL-1.1

//! `restore_tenant`: validates a backup envelope, maps every backed-up
//! database to its destination, merges the sections of each database into
//! one `TenantDataSnapshot`, and re-issues every section as durable,
//! replicated writes into its destination database. It then captures the
//! destination and checks every collection's row count and digest against
//! the backup's. A DRY RUN validates the envelope and verifies nothing.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_types::backup_envelope::{
    DEFAULT_MAX_TOTAL_BYTES, Envelope, EnvelopeError, VerificationPhase,
    parse_encrypted as parse_envelope_encrypted,
};

use crate::Error;
use crate::control::backup::verify::destination::verify_destination;
use crate::control::backup::verify::expect::Expectation;
use crate::control::server::shared::ddl::neutral::collection::dispatch_register_from_stored;
use crate::control::state::SharedState;

use super::super::databases::{DatabaseMap, resolve_databases};
use super::super::sections::apply_metadata_sections;
use super::super::target::RestoreMark;
use super::super::validate::{ValidatedEnvelope, validate_envelope};
use super::rebind;
use super::stats::{CollectionRows, RestoreStats};

/// Restore a tenant from a fully-buffered backup envelope.
pub async fn restore_tenant(
    state: &Arc<SharedState>,
    tenant_id: u64,
    envelope_bytes: &[u8],
    dry_run: bool,
    force: bool,
) -> Result<RestoreStats, Error> {
    let env = open_envelope(state, tenant_id, envelope_bytes)?;

    // Every group the restore reads or writes has a reachable majority, or
    // the restore fails here, before it proposes anything.
    if !dry_run {
        super::super::quorum::require_quorum(state)?;
    }

    // A retry of the same envelope carries the same id, so the guard below
    // skips the writes an earlier, failed attempt of it re-issued.
    let restore_id = restore_id_of(envelope_bytes);
    // The cut the RESTORE takes effect at, closed before the guard reads the
    // newest write: every write at or below it applied, so the guard sees
    // each one. A dry run writes nothing and takes no cut.
    let restore_cut = if dry_run {
        i64::MAX
    } else {
        super::super::cut::take_restore_cut(state).await?
    };
    if !dry_run {
        refuse_stale_envelope(state, tenant_id, &env, restore_id, force).await?;
    }

    let mut stats = RestoreStats {
        tenant_id,
        dry_run,
        sections: env.sections.len() as u16,
        source_vshard_count: env.meta.source_vshard_count,
        ..Default::default()
    };

    // Every refusal the envelope's content can raise runs here, before the
    // first proposal. A refused envelope changes nothing on this cluster.
    let ValidatedEnvelope {
        databases: database_blobs,
        merged,
        expectation,
    } = validate_envelope(state, tenant_id, &env)?;

    // Map every backed-up database to its destination, creating each one the
    // destination lacks. Every other section names its database by source id.
    let databases = resolve_databases(
        state,
        tenant_id,
        &database_blobs,
        dry_run,
        RestoreMark {
            id: restore_id,
            cut: restore_cut,
            event_source: crate::event::EventSource::Restore,
        },
    )
    .await?;
    stats.databases = database_blobs.len();
    stats.databases_created = databases.created();

    // Before the first write into an existing database. A database this restore
    // created has a fresh id, so no tombstone names it. A dry run has no target
    // for a database this cluster lacks.
    for (source, snap) in &merged {
        if let Some(target) = databases.get(*source) {
            rebind::record_tombstoned_restores(state, tenant_id, target, snap, dry_run)?;
        }
    }

    if !dry_run {
        restore_metadata(state, tenant_id, &env, &databases).await?;
        stats.arrays =
            super::super::array_reissue::restore_array_rows(state, tenant_id, &env, &databases)
                .await?;
    }

    for snap in merged.values() {
        stats.count_sections(snap);
        if dry_run {
            stats.columnar_engines += snap.columnar_engines.len();
        }
    }

    if dry_run {
        stats.collection_rows = envelope_collection_rows(&expectation, |source| {
            databases.get(source).map(|target| target.dest.as_u64())
        });
        return Ok(stats);
    }

    // Each database re-issues its rows into its destination database.
    for (source, snap) in merged {
        let target = databases.target(source)?;
        super::database::reissue_database(state, tenant_id, target, snap, &mut stats).await?;
    }

    verify_restored(state, tenant_id, expectation, &databases, &mut stats).await?;
    Ok(stats)
}

/// Decrypt and parse the envelope, and check that it holds `tenant_id`.
fn open_envelope(
    state: &SharedState,
    tenant_id: u64,
    envelope_bytes: &[u8],
) -> Result<Envelope, Error> {
    let Some(kek) = &state.backup_kek else {
        return Err(Error::Internal {
            detail: "restore: envelope is encrypted but no backup KEK is configured; \
                     set [backup_encryption] in the server config"
                .into(),
        });
    };
    let env = parse_envelope_encrypted(envelope_bytes, DEFAULT_MAX_TOTAL_BYTES, kek)?;
    if env.meta.tenant_id != tenant_id {
        return Err(EnvelopeError::TenantMismatch {
            expected: tenant_id,
            actual: env.meta.tenant_id,
        }
        .into());
    }
    Ok(env)
}

/// Refuse an envelope older than the tenant's newest committed write, unless
/// `force` overrides it. An envelope with no watermark passes.
async fn refuse_stale_envelope(
    state: &Arc<SharedState>,
    tenant_id: u64,
    env: &Envelope,
    restore_id: u64,
    force: bool,
) -> Result<(), Error> {
    if env.meta.snapshot_watermark == 0 {
        return Ok(());
    }
    let Some(mark) =
        super::super::guard::newest_committed_write(state, tenant_id, restore_id).await?
    else {
        return Ok(());
    };
    let current_high_water = mark.hlc;
    if env.meta.snapshot_watermark >= current_high_water {
        return Ok(());
    }
    if !force {
        return Err(Error::Internal {
            detail: format!(
                "restore refused: envelope watermark {} is older than the \
                 destination cluster's last observed write-HLC {} for tenant \
                 {} (newest write: {} on collection '{}') — newer writes would \
                 be silently overwritten",
                env.meta.snapshot_watermark,
                current_high_water,
                tenant_id,
                mark.site,
                mark.collection.as_deref().unwrap_or("<none>"),
            ),
        });
    }
    tracing::warn!(
        tenant_id,
        envelope_watermark = env.meta.snapshot_watermark,
        current_high_water,
        newest_write_site = mark.site.as_str(),
        newest_write_collection = mark.collection.as_deref().unwrap_or(""),
        "restore staleness protection explicitly overridden via FORCE: \
         envelope watermark is older than the destination cluster's last \
         observed write-HLC for this tenant — newer writes will be overwritten"
    );
    Ok(())
}

/// Apply the metadata sections and register every restored collection with
/// this node's Data Plane.
///
/// Every restored collection's declaration reaches this node's Data Plane
/// before any of its rows do. The catalog row alone leaves `doc_configs` empty
/// for the collection, and the re-issue then ingests a timeseries
/// collection's rows into an inferred shape: the declared time key becomes an
/// integer field and the row is stamped with the restore-time clock. This is
/// the same registration a committed DDL and the boot rehydration dispatch,
/// and it replaces any registration already present, so a cluster applier's
/// own register hook and a later boot seed are both idempotent with it. A
/// registration failure fails the restore.
async fn restore_metadata(
    state: &Arc<SharedState>,
    tenant_id: u64,
    env: &Envelope,
    databases: &DatabaseMap,
) -> Result<(), Error> {
    let restored_collections = apply_metadata_sections(state, tenant_id, env, databases).await?;
    for coll in &restored_collections {
        // A classified error keeps its class. Only a machinery failure
        // gains the restore context.
        dispatch_register_from_stored(state, coll)
            .await
            .map_err(|e| {
                if crate::error_classify::is_unclassified_failure(&e) {
                    Error::Internal {
                        detail: format!(
                            "restore: Data Plane registration of collection '{}' failed: {e}",
                            coll.name
                        ),
                    }
                } else {
                    e
                }
            })?;
    }
    Ok(())
}

/// Check that the destination holds every backed-up row, and record the
/// verified counts in `stats`. A mismatch fails the restore and leaves the
/// restored data in place.
async fn verify_restored(
    state: &Arc<SharedState>,
    tenant_id: u64,
    expectation: Expectation,
    databases: &DatabaseMap,
    stats: &mut RestoreStats,
) -> Result<(), Error> {
    let verified = verify_destination(
        state,
        tenant_id,
        expectation,
        |source| {
            databases
                .target(source)
                .map(|target| (target.dest, target.restore_cut))
        },
        VerificationPhase::Destination,
    )
    .await?;
    stats.verified_collections = verified.collections;
    stats.verified_rows = verified.rows;
    stats.collection_rows = verified
        .per_collection
        .into_iter()
        .map(|((database_id, collection), rows)| CollectionRows {
            database_id: Some(database_id),
            collection,
            rows,
        })
        .collect();
    Ok(())
}

/// The rows per collection the envelope holds. `dest_of` maps a source
/// database id to its destination, `None` for one this cluster lacks.
fn envelope_collection_rows(
    expectation: &Expectation,
    dest_of: impl Fn(u64) -> Option<u64>,
) -> Vec<CollectionRows> {
    let mut rows: BTreeMap<(Option<u64>, &str), u64> = BTreeMap::new();
    for (source, database) in &expectation.databases {
        let dest = dest_of(*source);
        for ((collection, _), tally) in &database.rows.tallies {
            *rows.entry((dest, collection.as_str())).or_default() += tally.count;
        }
    }
    rows.into_iter()
        .map(|((database_id, collection), rows)| CollectionRows {
            database_id,
            collection: collection.to_string(),
            rows,
        })
        .collect()
}

/// The id of a RESTORE of `envelope_bytes`: the first eight bytes of the
/// envelope's SHA-256, never `0`. Every retry of one envelope gets the same id.
fn restore_id_of(envelope_bytes: &[u8]) -> u64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(envelope_bytes);
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest[..8]);
    u64::from_le_bytes(head) | 1
}

#[cfg(test)]
mod tests {
    use super::restore_id_of;

    #[test]
    fn one_envelope_has_one_nonzero_restore_id() {
        let a = restore_id_of(b"envelope a");
        assert_eq!(a, restore_id_of(b"envelope a"));
        assert_ne!(a, restore_id_of(b"envelope b"));
        assert_ne!(restore_id_of(b""), 0);
    }
}
