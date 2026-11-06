// SPDX-License-Identifier: BUSL-1.1

//! The source's side of a bootstrap: one capture per link, served in chunks.
//!
//! The first chunk request starts the capture in the background and answers
//! nothing, so no request outlives the link's response deadline. Later requests
//! poll, then fetch. A capture stays on the replica that took it. A mirror that
//! moves to another replica gets a restart refusal and starts over.
//!
//! The DDL lease spans the catalog read and the capturing cut, so no schema
//! change sits between the catalog image and the rows. A capture that outlasts
//! the lease's bound lets a DDL through. That DDL's entry lies above the image's
//! metadata index, so the mirror applies it from the tail.
//!
//! The capture sits in memory until the mirror fetched its last chunk, or until
//! [`CAPTURE_IDLE_TTL`] passes with no request.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::mirror::{
    CrossClusterSnapshotEnvelope, MAX_BOOTSTRAP_BYTES, MAX_BOOTSTRAP_CHUNK,
};
use nodedb_types::{DatabaseId, MirrorGroupPosition};

use crate::Error;
use crate::control::backup::bind_capture::capture_binds;
use crate::control::backup::cut_capture::collect::capture_database;
use crate::control::backup::database::database_tenants;
use crate::control::metadata_proposer::{
    acquire_ddl_prepare_lease_async, lock_ddl_preparation_async,
};
use crate::control::mirror::bootstrap_image::MirrorBootstrapImage;
use crate::control::read_cut::statement_cap;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

use super::catalog_image::catalog_image;

/// How long a capture no request touched stays in memory.
const CAPTURE_IDLE_TTL: Duration = Duration::from_secs(300);
/// How often a finished capture checks its idle bound.
const CAPTURE_TTL_POLL: Duration = Duration::from_secs(5);

/// A finished capture, ready to serve.
#[derive(Debug)]
struct ReadyCapture {
    source_cluster: String,
    database: u64,
    /// The cut's HLC wall time. It names this capture in every chunk.
    cut: u64,
    crc32c: u32,
    bytes: Vec<u8>,
}

impl ReadyCapture {
    fn chunk(&self, offset: u64) -> Option<CrossClusterSnapshotEnvelope> {
        let total = self.bytes.len() as u64;
        if offset > total {
            return None;
        }
        let start = usize::try_from(offset).ok()?;
        let end = start
            .saturating_add(MAX_BOOTSTRAP_CHUNK)
            .min(self.bytes.len());
        Some(CrossClusterSnapshotEnvelope {
            source_cluster_id: self.source_cluster.clone(),
            source_database_id: self.database.to_string(),
            snapshot_lsn: self.cut,
            total_bytes: total,
            total_crc32c: self.crc32c,
            offset,
            data: self.bytes.get(start..end)?.to_vec(),
            done: end == self.bytes.len(),
        })
    }
}

#[derive(Debug)]
enum Slot {
    Preparing,
    Ready {
        capture: Arc<ReadyCapture>,
        touched: Instant,
    },
    Failed(String),
}

/// What a chunk request gets.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum CaptureAnswer {
    /// No capture runs, and this request started one: run it.
    Start,
    /// The capture still runs.
    Wait,
    Chunk(Box<CrossClusterSnapshotEnvelope>),
    /// The capture failed. The next request at offset 0 takes a new one.
    Failed(String),
    /// This replica holds no capture at this offset.
    Restart,
}

/// Each link's capture on this replica.
#[derive(Debug, Default)]
pub(super) struct CaptureSlots {
    slots: Mutex<HashMap<u64, Slot>>,
}

impl CaptureSlots {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Slot>> {
        self.slots.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Answer one chunk request of `link_id`. The last chunk frees the capture.
    pub(super) fn answer(&self, link_id: u64, offset: u64) -> CaptureAnswer {
        let mut slots = self.lock();
        match slots.get_mut(&link_id) {
            None if offset == 0 => {
                slots.insert(link_id, Slot::Preparing);
                CaptureAnswer::Start
            }
            None => CaptureAnswer::Restart,
            Some(Slot::Preparing) => CaptureAnswer::Wait,
            Some(Slot::Failed(detail)) => {
                let detail = std::mem::take(detail);
                slots.remove(&link_id);
                CaptureAnswer::Failed(detail)
            }
            Some(Slot::Ready { capture, touched }) => {
                *touched = Instant::now();
                let Some(chunk) = capture.chunk(offset) else {
                    return CaptureAnswer::Restart;
                };
                if chunk.done {
                    slots.remove(&link_id);
                }
                CaptureAnswer::Chunk(Box::new(chunk))
            }
        }
    }

    fn finish(&self, link_id: u64, outcome: Result<ReadyCapture, String>) {
        let slot = match outcome {
            Ok(capture) => Slot::Ready {
                capture: Arc::new(capture),
                touched: Instant::now(),
            },
            Err(detail) => Slot::Failed(detail),
        };
        self.lock().insert(link_id, slot);
    }

    /// Whether the link still holds a capture, after freeing one idle past `ttl`.
    fn holds_after_expiry(&self, link_id: u64, ttl: Duration) -> bool {
        let mut slots = self.lock();
        match slots.get(&link_id) {
            Some(Slot::Ready { touched, .. }) if touched.elapsed() >= ttl => {
                slots.remove(&link_id);
                false
            }
            Some(Slot::Ready { .. }) => true,
            Some(Slot::Preparing | Slot::Failed(_)) | None => false,
        }
    }
}

/// Each group's place after the capture: its barrier, and the catalog's index.
fn image_positions(barriers: &BTreeMap<u64, u64>, metadata_index: u64) -> Vec<MirrorGroupPosition> {
    barriers
        .iter()
        .map(|(group_id, log_index)| MirrorGroupPosition {
            group_id: *group_id,
            log_index: *log_index,
        })
        .chain(std::iter::once(MirrorGroupPosition {
            group_id: METADATA_GROUP_ID,
            log_index: metadata_index,
        }))
        .collect()
}

/// Take `database` at one cut, under the DDL lease.
async fn capture_image(
    state: &Arc<SharedState>,
    database: DatabaseId,
) -> Result<MirrorBootstrapImage, Error> {
    let handle = state.metadata_raft_handle()?;
    let _local_ddl_guard = lock_ddl_preparation_async(state).await;
    let lease = acquire_ddl_prepare_lease_async(state, handle.as_ref()).await?;
    let captured = capture_under_lease(state, database).await;
    lease.release().await;
    captured
}

async fn capture_under_lease(
    state: &Arc<SharedState>,
    database: DatabaseId,
) -> Result<MirrorBootstrapImage, Error> {
    let catalog = state.credentials.catalog();
    // The index comes first. Whatever the catalog holds past it applies again
    // from the tail, and every catalog apply repeats safely.
    let metadata_index = state.applied_index_watcher(METADATA_GROUP_ID).current();
    let image = catalog_image(catalog, database)?;
    let collections = catalog.load_all_collections(database)?;
    let arrays: Vec<_> = catalog
        .load_all_arrays()?
        .into_iter()
        .filter(|array| array.array_id.database_id == database)
        .collect();
    let tenants = database_tenants(state, database)?;

    // The cap comes before the cut: every version at or below it sits below
    // every group's barrier.
    let now_ms = nodedb_types::ordinal_to_ms(nodedb_types::wall_now_ns_i64());
    let cap = statement_cap(state, now_ms).await?;
    let mut capture = capture_database(state, database, &tenants).await?;

    let mut encoded = Vec::with_capacity(tenants.len());
    for tenant in &tenants {
        let mut snap = capture.tenants.remove(tenant).unwrap_or_default();
        // The captured rows' encoding: the mirror re-issues them as they stand.
        snap.storage_conversions = capture.conversions.remove(tenant).unwrap_or_default();
        let names: Vec<String> = collections
            .iter()
            .filter(|collection| collection.tenant_id == *tenant)
            .map(|collection| collection.name.clone())
            .chain(
                arrays
                    .iter()
                    .filter(|array| array.array_id.tenant_id.as_u64() == *tenant)
                    .map(|array| array.array_id.name.clone()),
            )
            .collect();
        // The Data Plane capture carries no surrogate binds. Each lives in the
        // catalog of the nodes that hold its home. A mirror mints none, so an
        // array's cell binds travel with its rows too: a key with no bind stops
        // the bootstrap.
        snap.surrogate_pk
            .extend(capture_binds(state, *tenant, database, &names).await?);
        let body = zerompk::to_msgpack_vec(&snap).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("mirror bootstrap: encode the capture of tenant {tenant}: {e}"),
        })?;
        encoded.push((*tenant, body));
    }
    Ok(MirrorBootstrapImage {
        database: database.as_u64(),
        cut_hlc_ns: capture.cut,
        cap_ordinal: cap.cap,
        positions: image_positions(&capture.positions, metadata_index),
        catalog: image,
        tenants: encoded,
    })
}

/// The capture as bytes, refused past the transfer's bound.
fn ready_capture(
    source_cluster: String,
    image: &MirrorBootstrapImage,
) -> Result<ReadyCapture, Error> {
    let bytes = image.encode()?;
    if bytes.len() as u64 > MAX_BOOTSTRAP_BYTES {
        return Err(Error::BadRequest {
            detail: format!(
                "the capture of database {} holds {} bytes, past the bound of \
                 {MAX_BOOTSTRAP_BYTES}",
                image.database,
                bytes.len()
            ),
        });
    }
    Ok(ReadyCapture {
        source_cluster,
        database: image.database,
        cut: image.cut_hlc_ns,
        crc32c: crc32c::crc32c(&bytes),
        bytes,
    })
}

/// Take the link's capture in the background. It frees itself once idle.
pub(super) fn start_capture(
    state: &Arc<SharedState>,
    slots: Arc<CaptureSlots>,
    source_cluster: String,
    database: DatabaseId,
    link_id: u64,
) {
    let shared = Arc::clone(state);
    spawn_loop(
        &state.loop_registry,
        &state.shutdown,
        "mirror_bootstrap_capture",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let outcome = tokio::select! {
                _ = shutdown.wait_cancelled() => return,
                captured = capture_image(&shared, database) => captured,
            };
            let outcome = outcome
                .and_then(|image| ready_capture(source_cluster, &image))
                .map_err(|error| error.to_string());
            if let Err(detail) = &outcome {
                tracing::warn!(link_id, %detail, "mirror source: a capture failed");
            }
            slots.finish(link_id, outcome);
            while slots.holds_after_expiry(link_id, CAPTURE_IDLE_TTL) {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => return,
                    _ = tokio::time::sleep(CAPTURE_TTL_POLL) => {}
                }
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINK: u64 = 9;

    fn capture(bytes: Vec<u8>) -> ReadyCapture {
        ReadyCapture {
            source_cluster: "prod".to_string(),
            database: 1700,
            cut: 21,
            crc32c: crc32c::crc32c(&bytes),
            bytes,
        }
    }

    #[test]
    fn the_first_request_starts_a_capture_and_the_next_one_waits() {
        let slots = CaptureSlots::default();
        assert_eq!(slots.answer(LINK, 0), CaptureAnswer::Start);
        assert_eq!(slots.answer(LINK, 0), CaptureAnswer::Wait);
    }

    #[test]
    fn a_later_offset_with_no_capture_restarts() {
        let slots = CaptureSlots::default();
        assert_eq!(slots.answer(LINK, 4), CaptureAnswer::Restart);
    }

    #[test]
    fn a_failure_answers_once_and_the_retry_starts_again() {
        let slots = CaptureSlots::default();
        slots.answer(LINK, 0);
        slots.finish(LINK, Err("no quorum".to_string()));
        assert_eq!(
            slots.answer(LINK, 0),
            CaptureAnswer::Failed("no quorum".to_string())
        );
        assert_eq!(slots.answer(LINK, 0), CaptureAnswer::Start);
    }

    #[test]
    fn the_chunks_cover_the_capture_and_the_last_one_frees_it() {
        let slots = CaptureSlots::default();
        slots.answer(LINK, 0);
        let bytes: Vec<u8> = (0..MAX_BOOTSTRAP_CHUNK + 10).map(|at| at as u8).collect();
        slots.finish(LINK, Ok(capture(bytes.clone())));

        let mut received = Vec::new();
        loop {
            let CaptureAnswer::Chunk(chunk) = slots.answer(LINK, received.len() as u64) else {
                panic!("a ready capture answers with a chunk");
            };
            assert!(chunk.data.len() <= MAX_BOOTSTRAP_CHUNK);
            assert_eq!(chunk.total_bytes, bytes.len() as u64);
            assert_eq!(chunk.snapshot_lsn, 21);
            received.extend_from_slice(&chunk.data);
            if chunk.done {
                break;
            }
        }
        assert_eq!(received, bytes);
        assert_eq!(slots.answer(LINK, 4), CaptureAnswer::Restart);
    }

    #[test]
    fn an_offset_past_the_capture_restarts() {
        let slots = CaptureSlots::default();
        slots.answer(LINK, 0);
        slots.finish(LINK, Ok(capture(vec![1, 2])));
        assert_eq!(slots.answer(LINK, 3), CaptureAnswer::Restart);
    }

    #[test]
    fn an_idle_capture_frees_itself() {
        let slots = CaptureSlots::default();
        slots.answer(LINK, 0);
        slots.finish(LINK, Ok(capture(vec![1, 2])));
        assert!(slots.holds_after_expiry(LINK, Duration::from_secs(60)));
        assert!(!slots.holds_after_expiry(LINK, Duration::ZERO));
        assert_eq!(slots.answer(LINK, 1), CaptureAnswer::Restart);
    }

    #[test]
    fn every_data_group_and_the_metadata_group_get_a_place() {
        let barriers = BTreeMap::from([(1, 40), (2, 50)]);
        assert_eq!(
            image_positions(&barriers, 60),
            vec![
                MirrorGroupPosition {
                    group_id: 1,
                    log_index: 40,
                },
                MirrorGroupPosition {
                    group_id: 2,
                    log_index: 50,
                },
                MirrorGroupPosition {
                    group_id: METADATA_GROUP_ID,
                    log_index: 60,
                },
            ]
        );
    }
}
