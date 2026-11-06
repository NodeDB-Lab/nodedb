// SPDX-License-Identifier: BUSL-1.1

//! The source's answers to an authenticated mirror client.
//!
//! The listener proved who the client is. This service decides what it can
//! read. A hello checks the client's allowlist entry before it looks at the
//! catalog, so a client learns nothing of a database it cannot follow. A fetch
//! and an ack name a link, and the link's pin names the database and its owner:
//! no request can name a database of its own choosing after the hello.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::mirror::tail_wire::{MAX_TAIL_BYTES, MAX_TAIL_WAIT_MS};
use nodedb_cluster::mirror::{
    CrossClusterSnapshotEnvelope, MIRROR_PROTOCOL_VERSION, MirrorAck, MirrorBootstrapRequest,
    MirrorClient, MirrorHello, MirrorHelloAck, MirrorRefusal, MirrorSourceService, MirrorTailBatch,
    MirrorTailEntry, MirrorTailRequest,
};
use nodedb_cluster::{ClusterError, METADATA_GROUP_ID, WaitOutcome};
use nodedb_types::{DatabaseId, MirrorPin, MirrorPinState};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::auth_fence::cluster::routed_groups;
use crate::control::state::SharedState;

use super::bootstrap::{CaptureAnswer, CaptureSlots, start_capture};
use super::filter::{data_entry_leaves, metadata_payloads};

/// How often an ack moves the replicated pin. An ack between two of these moves
/// nothing, so a busy link never churns the metadata log.
const PIN_WRITE_INTERVAL: Duration = Duration::from_secs(10);

/// Refuse a hello the catalog never needs to see.
fn check_hello(
    client: &MirrorClient,
    hello: &MirrorHello,
    cluster_name: &str,
    cluster_id: u64,
) -> Result<(), MirrorRefusal> {
    if hello.protocol_version != MIRROR_PROTOCOL_VERSION {
        return Err(MirrorRefusal::BadVersion {
            supported: MIRROR_PROTOCOL_VERSION,
            requested: hello.protocol_version,
        });
    }
    if hello.source_cluster != cluster_name {
        return Err(MirrorRefusal::WrongCluster {
            declared: hello.source_cluster.clone(),
            actual: cluster_name.to_string(),
        });
    }
    if hello.mirror_cluster_id == cluster_id {
        return Err(MirrorRefusal::SameCluster);
    }
    if !client.allows(&hello.source_database) {
        return Err(MirrorRefusal::DatabaseNotAllowed {
            database: hello.source_database.clone(),
        });
    }
    Ok(())
}

/// Refuse a request of `link_id` from anyone but its owner, or of a pin that no
/// longer holds.
fn check_link(
    client: &MirrorClient,
    link_id: u64,
    pin: Option<&MirrorPin>,
) -> Result<u64, MirrorRefusal> {
    let pin = pin.ok_or(MirrorRefusal::LinkUnknown { link_id })?;
    if pin.client != client.name {
        return Err(MirrorRefusal::LinkNotOwned { link_id });
    }
    match pin.state {
        MirrorPinState::Live => Ok(pin.database),
        MirrorPinState::Expired => Err(MirrorRefusal::PinExpired { link_id }),
        MirrorPinState::SourceDropped => Err(MirrorRefusal::SourceDropped { link_id }),
    }
}

/// A hello on an existing link must come from its owner, for its database. A
/// hello on a stopped link answers why it stopped.
fn check_rehello(
    client: &MirrorClient,
    link_id: u64,
    database: u64,
    pin: &MirrorPin,
) -> Result<(), MirrorRefusal> {
    if pin.database != database {
        return Err(MirrorRefusal::LinkNotOwned { link_id });
    }
    check_link(client, link_id, Some(pin)).map(drop)
}

/// Refuse a database whose rows still live in its clone source. A clone's lineage
/// names this cluster's LSNs and surrogates, which mean nothing on a mirror.
fn check_materialized(
    name: &str,
    has_parent: bool,
    mut collections: impl Iterator<Item = bool>,
) -> Result<(), MirrorRefusal> {
    if has_parent || collections.any(|cloned| cloned) {
        return Err(MirrorRefusal::UnmaterializedClone {
            database: name.to_string(),
        });
    }
    Ok(())
}

/// Refuse a group a mirror never tails. The sequencer log carries every
/// database's transactions, and a slice installs from its data group alone.
fn check_group(group: u64) -> Result<(), MirrorRefusal> {
    if group == SEQUENCER_GROUP_ID {
        return Err(MirrorRefusal::GroupNotTailed { group });
    }
    Ok(())
}

/// The batch a filter leaves of `entries`: what leaves, and the index past
/// everything scanned. The scan always moves forward, even when nothing leaves.
fn filtered_batch(
    group: u64,
    database: u64,
    from_index: u64,
    entries: Vec<nodedb_raft::message::LogEntry>,
) -> (Vec<MirrorTailEntry>, u64) {
    let next_index = entries
        .last()
        .map_or(from_index, |entry| entry.index.saturating_add(1));
    let mut leaving = Vec::new();
    for entry in entries {
        if group == METADATA_GROUP_ID {
            for data in metadata_payloads(&entry.data, database) {
                leaving.push(MirrorTailEntry {
                    index: entry.index,
                    data,
                });
            }
        } else if data_entry_leaves(&entry.data, database) {
            leaving.push(MirrorTailEntry {
                index: entry.index,
                data: entry.data,
            });
        }
    }
    (leaving, next_index)
}

/// This cluster's answers on the mirror listener.
pub struct SourceService {
    state: Arc<SharedState>,
    cluster_name: String,
    /// When this node last wrote each link's pin.
    last_pin_write: std::sync::Mutex<std::collections::HashMap<u64, Instant>>,
    /// Each link's bootstrap capture on this replica.
    captures: Arc<CaptureSlots>,
}

impl SourceService {
    pub fn new(state: Arc<SharedState>, cluster_name: String) -> Self {
        Self {
            state,
            cluster_name,
            last_pin_write: std::sync::Mutex::default(),
            captures: Arc::default(),
        }
    }

    /// Refuse a database that is an unmaterialized clone, or holds a collection
    /// that is one.
    fn refuse_clone(&self, name: &str, database: DatabaseId) -> Result<(), MirrorRefusal> {
        let catalog = self.state.credentials.catalog();
        let has_parent = catalog
            .get_database(database)
            .map_err(unavailable)?
            .is_some_and(|descriptor| descriptor.parent_clone.is_some());
        let collections = catalog
            .load_all_collections(database)
            .map_err(unavailable)?;
        check_materialized(
            name,
            has_parent,
            collections.iter().map(|coll| coll.cloned_from.is_some()),
        )
    }

    /// Every group a mirror tails: the data groups and the metadata group.
    fn tailed_groups(&self) -> Vec<u64> {
        let mut groups: BTreeSet<u64> = routed_groups(&self.state)
            .into_iter()
            .filter(|group| *group != SEQUENCER_GROUP_ID)
            .collect();
        groups.insert(METADATA_GROUP_ID);
        groups.into_iter().collect()
    }

    /// Whether this ack's write is due, and note it when it is.
    fn pin_write_due(&self, link_id: u64) -> bool {
        let mut writes = self
            .last_pin_write
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let now = Instant::now();
        let due = writes
            .get(&link_id)
            .is_none_or(|last| now.duration_since(*last) >= PIN_WRITE_INTERVAL);
        if due {
            writes.insert(link_id, now);
        }
        due
    }

    async fn propose_pin(&self, pin: MirrorPin) -> Result<(), MirrorRefusal> {
        propose_catalog_entry_async(&self.state, &CatalogEntry::PutMirrorPin(Box::new(pin)))
            .await
            .map(drop)
            .map_err(unavailable)
    }

    /// Wait until this replica applied `from_index`, or `wait` passes.
    async fn await_entry(&self, group: u64, from_index: u64, wait: Duration) {
        if wait.is_zero() {
            return;
        }
        let watcher = self.state.applied_index_watcher(group);
        let outcome = watcher
            .wait_for_async(from_index, Instant::now() + wait)
            .await;
        match outcome {
            WaitOutcome::Reached | WaitOutcome::TimedOut | WaitOutcome::GroupGone => {}
        }
    }
}

fn unavailable(error: crate::Error) -> MirrorRefusal {
    MirrorRefusal::Unavailable {
        detail: error.to_string(),
    }
}

fn now_ms() -> u64 {
    nodedb_types::wall_now_ns() / 1_000_000
}

#[async_trait::async_trait]
impl MirrorSourceService for SourceService {
    async fn hello(
        &self,
        client: &MirrorClient,
        hello: MirrorHello,
    ) -> Result<MirrorHelloAck, MirrorRefusal> {
        let cluster_id =
            self.state
                .mirror
                .cluster_id
                .get()
                .copied()
                .ok_or(MirrorRefusal::Unavailable {
                    detail: "this node has not joined its cluster yet".to_string(),
                })?;
        check_hello(client, &hello, &self.cluster_name, cluster_id)?;
        let catalog = self.state.credentials.catalog();
        let database = catalog
            .get_database_id_by_name(&hello.source_database)
            .map_err(unavailable)?
            .ok_or_else(|| MirrorRefusal::DatabaseUnknown {
                database: hello.source_database.clone(),
            })?;
        if database == DatabaseId::DEFAULT {
            return Err(MirrorRefusal::DefaultDatabase);
        }
        if self.state.mirror.fence.read_only_name(database).is_some() {
            return Err(MirrorRefusal::SourceIsMirror {
                database: hello.source_database,
            });
        }
        self.refuse_clone(&hello.source_database, database)?;
        match self.state.mirror.pins.get(hello.link_id) {
            Some(pin) => check_rehello(client, hello.link_id, database.as_u64(), &pin)?,
            None => {
                self.propose_pin(MirrorPin {
                    link_id: hello.link_id,
                    client: client.name.clone(),
                    database: database.as_u64(),
                    positions: Vec::new(),
                    acked_ms: now_ms(),
                    state: MirrorPinState::Live,
                })
                .await?;
                super::cut_loop::install_pin_ceilings(&self.state);
            }
        }
        Ok(MirrorHelloAck {
            source_cluster: self.cluster_name.clone(),
            source_cluster_id: cluster_id,
            source_database_id: database.as_u64(),
            timeline: catalog.load_metadata_timeline().map_err(unavailable)?,
            groups: self.tailed_groups(),
        })
    }

    async fn tail(
        &self,
        client: &MirrorClient,
        request: MirrorTailRequest,
    ) -> Result<MirrorTailBatch, MirrorRefusal> {
        check_group(request.group)?;
        let pin = self.state.mirror.pins.get(request.link_id);
        let database = check_link(client, request.link_id, pin.as_ref())?;
        let multi_raft = self
            .state
            .multi_raft
            .get()
            .ok_or(MirrorRefusal::Unavailable {
                detail: "this node's Raft loop has not started".to_string(),
            })?;
        let wait = Duration::from_millis(request.wait_ms.min(MAX_TAIL_WAIT_MS));
        self.await_entry(request.group, request.from_index, wait)
            .await;
        let max_bytes = usize::try_from(request.max_bytes.min(MAX_TAIL_BYTES))
            .unwrap_or(usize::MAX)
            .max(1);
        let read = multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .read_committed_within(request.group, request.from_index, max_bytes);
        let read = match read {
            Ok(read) => read,
            Err(ClusterError::GroupNotFound { .. }) => {
                return Err(MirrorRefusal::NotHosted {
                    group: request.group,
                });
            }
            Err(error) => {
                return Err(MirrorRefusal::Unavailable {
                    detail: error.to_string(),
                });
            }
        };
        if request.from_index < read.first_available {
            return Err(MirrorRefusal::LogTruncated {
                group: request.group,
                first_available: read.first_available,
            });
        }
        let (entries, next_index) =
            filtered_batch(request.group, database, request.from_index, read.entries);
        Ok(MirrorTailBatch {
            entries,
            next_index,
            commit_index: read.commit_index,
            source_hlc_ns: self.state.hlc_clock.now().wall_ns,
            first_available: read.first_available,
        })
    }

    async fn ack(&self, client: &MirrorClient, ack: MirrorAck) -> Result<(), MirrorRefusal> {
        let pin = self.state.mirror.pins.get(ack.link_id);
        check_link(client, ack.link_id, pin.as_ref())?;
        let Some(pin) = pin else {
            return Err(MirrorRefusal::LinkUnknown {
                link_id: ack.link_id,
            });
        };
        if !self.pin_write_due(ack.link_id) {
            return Ok(());
        }
        self.propose_pin(MirrorPin {
            positions: ack.positions,
            acked_ms: now_ms(),
            ..pin
        })
        .await
    }

    /// The pin names the database, so the request cannot name one of its own.
    async fn bootstrap(
        &self,
        client: &MirrorClient,
        request: MirrorBootstrapRequest,
    ) -> Result<Option<CrossClusterSnapshotEnvelope>, MirrorRefusal> {
        let pin = self.state.mirror.pins.get(request.link_id);
        let database = check_link(client, request.link_id, pin.as_ref())?;
        match self.captures.answer(request.link_id, request.offset) {
            CaptureAnswer::Start => {
                start_capture(
                    &self.state,
                    Arc::clone(&self.captures),
                    self.cluster_name.clone(),
                    DatabaseId::new(database),
                    request.link_id,
                );
                Ok(None)
            }
            CaptureAnswer::Wait => Ok(None),
            CaptureAnswer::Chunk(chunk) => Ok(Some(*chunk)),
            CaptureAnswer::Failed(detail) => Err(MirrorRefusal::BootstrapFailed {
                link_id: request.link_id,
                detail,
            }),
            CaptureAnswer::Restart => Err(MirrorRefusal::BootstrapRestart {
                link_id: request.link_id,
                offset: request.offset,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_raft::message::LogEntry;

    use super::*;
    use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};

    const DB: u64 = 1600;

    fn client(databases: &[&str]) -> MirrorClient {
        MirrorClient {
            name: "dr".to_string(),
            spki: [0; 32],
            databases: databases.iter().map(ToString::to_string).collect(),
        }
    }

    fn hello(database: &str) -> MirrorHello {
        MirrorHello {
            source_cluster: "prod".to_string(),
            source_database: database.to_string(),
            mirror_cluster_id: 2,
            link_id: 7,
            protocol_version: MIRROR_PROTOCOL_VERSION,
        }
    }

    fn pin(owner: &str, state: MirrorPinState) -> MirrorPin {
        MirrorPin {
            link_id: 7,
            client: owner.to_string(),
            database: DB,
            positions: Vec::new(),
            acked_ms: 0,
            state,
        }
    }

    #[test]
    fn a_hello_for_an_allowed_database_passes() {
        assert!(check_hello(&client(&["orders"]), &hello("orders"), "prod", 1).is_ok());
    }

    #[test]
    fn a_hello_for_a_database_off_the_allowlist_is_refused() {
        assert!(matches!(
            check_hello(&client(&["orders"]), &hello("payroll"), "prod", 1),
            Err(MirrorRefusal::DatabaseNotAllowed { .. })
        ));
    }

    #[test]
    fn a_hello_naming_another_cluster_is_refused() {
        assert!(matches!(
            check_hello(&client(&["orders"]), &hello("orders"), "staging", 1),
            Err(MirrorRefusal::WrongCluster { .. })
        ));
    }

    #[test]
    fn a_hello_from_this_cluster_is_refused() {
        assert!(matches!(
            check_hello(&client(&["orders"]), &hello("orders"), "prod", 2),
            Err(MirrorRefusal::SameCluster)
        ));
    }

    #[test]
    fn a_hello_of_another_version_is_refused() {
        let mut stale = hello("orders");
        stale.protocol_version = MIRROR_PROTOCOL_VERSION + 1;
        assert!(matches!(
            check_hello(&client(&["orders"]), &stale, "prod", 1),
            Err(MirrorRefusal::BadVersion { .. })
        ));
    }

    #[test]
    fn a_link_of_another_client_is_refused() {
        let held = pin("someone-else", MirrorPinState::Live);
        assert!(matches!(
            check_link(&client(&["orders"]), 7, Some(&held)),
            Err(MirrorRefusal::LinkNotOwned { link_id: 7 })
        ));
    }

    #[test]
    fn a_link_with_no_pin_is_refused() {
        assert!(matches!(
            check_link(&client(&["orders"]), 7, None),
            Err(MirrorRefusal::LinkUnknown { link_id: 7 })
        ));
    }

    #[test]
    fn an_expired_or_dropped_link_is_refused_and_a_live_one_names_its_database() {
        let me = client(&["orders"]);
        assert!(matches!(
            check_link(&me, 7, Some(&pin("dr", MirrorPinState::Expired))),
            Err(MirrorRefusal::PinExpired { link_id: 7 })
        ));
        assert!(matches!(
            check_link(&me, 7, Some(&pin("dr", MirrorPinState::SourceDropped))),
            Err(MirrorRefusal::SourceDropped { link_id: 7 })
        ));
        assert_eq!(
            check_link(&me, 7, Some(&pin("dr", MirrorPinState::Live))),
            Ok(DB)
        );
    }

    /// A link cannot move to another database by saying hello again.
    #[test]
    fn a_hello_cannot_move_a_link_to_another_database() {
        let held = pin("dr", MirrorPinState::Live);
        let me = client(&["orders"]);
        assert!(check_rehello(&me, 7, DB, &held).is_ok());
        assert!(matches!(
            check_rehello(&me, 7, DB + 1, &held),
            Err(MirrorRefusal::LinkNotOwned { link_id: 7 })
        ));
    }

    /// A clone's lineage holds on the database or on any one collection.
    #[test]
    fn an_unmaterialized_clone_is_refused() {
        assert!(check_materialized("orders", false, [false, false].into_iter()).is_ok());
        assert!(matches!(
            check_materialized("orders", true, [false].into_iter()),
            Err(MirrorRefusal::UnmaterializedClone { .. })
        ));
        assert!(matches!(
            check_materialized("orders", false, [false, true].into_iter()),
            Err(MirrorRefusal::UnmaterializedClone { .. })
        ));
    }

    #[test]
    fn the_sequencer_log_is_never_tailed() {
        assert!(matches!(
            check_group(SEQUENCER_GROUP_ID),
            Err(MirrorRefusal::GroupNotTailed { .. })
        ));
        assert!(check_group(METADATA_GROUP_ID).is_ok());
        assert!(check_group(1).is_ok());
    }

    fn log_entry(index: u64, database: u64) -> LogEntry {
        LogEntry {
            term: 1,
            index,
            data: ReplicatedEntry::new(
                1,
                database,
                2,
                ReplicatedWrite::CalvinReadTimeout {
                    epoch: 1,
                    position: 0,
                    tenant_id: 1,
                },
            )
            .encode()
            .expect("encode"),
        }
    }

    fn surrogate_bind(index: u64, database: u64) -> LogEntry {
        LogEntry {
            term: 1,
            index,
            data: ReplicatedEntry::new(
                1,
                database,
                2,
                ReplicatedWrite::SurrogateBind {
                    collection: "c".into(),
                    binds: vec![(vec![1], 9)],
                },
            )
            .encode()
            .expect("encode"),
        }
    }

    /// Another database's entries never leave, and the scan still moves past
    /// them, so a fetch over a busy foreign range makes progress.
    #[test]
    fn a_batch_holds_only_the_database_and_moves_past_the_rest() {
        let entries = vec![
            surrogate_bind(5, DB + 1),
            surrogate_bind(6, DB),
            log_entry(7, DB),
            surrogate_bind(8, DB + 1),
        ];
        let (leaving, next_index) = filtered_batch(1, DB, 5, entries);
        assert_eq!(leaving.iter().map(|e| e.index).collect::<Vec<_>>(), vec![6]);
        assert_eq!(next_index, 9);
    }

    #[test]
    fn an_empty_scan_stays_at_its_start() {
        let (leaving, next_index) = filtered_batch(1, DB, 5, Vec::new());
        assert!(leaving.is_empty());
        assert_eq!(next_index, 5);
    }
}
