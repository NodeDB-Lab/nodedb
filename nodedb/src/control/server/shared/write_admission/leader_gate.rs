// SPDX-License-Identifier: BUSL-1.1

//! The write gate a data-group leader runs on every proposal to its group.
//!
//! A replicated write applies on every replica in log order, with its
//! ordering decided once: on the leader, before the entry enters the log.
//! [`LeaderWriteGate`] admits the entry there with the same gate a local
//! write passes:
//!
//! - Uncontended: the leader proposes the entry, and its keys stay held until
//!   the leader's apply loop starts the entry (see `holds`).
//! - Contended, and the write routes (see `admission_keys`): the propose
//!   refuses with `RouteToSequencer`, and the proposer submits the write to
//!   the Calvin sequencer. An append never routes.
//! - A client entry whose `RETURNING` list can fail routes the same way,
//!   held key or not (see `returning_route`). A list that cannot fail
//!   proposes as sent.
//! - Contended otherwise: the gate waits holding no key, until the keys are
//!   free or the proposer's deadline passes. A forwarded entry carries what
//!   remains of that deadline.
//!
//! Followers apply the committed entry with no gate: the leader ordered it.
//!
//! The gate also stamps the entry with the system-time ordinals its write
//! lands (`ReplicatedEntry::system_ordinal`), reserved from this leader's
//! clock once the entry holds its keys. A later write to the same keys is
//! admitted only after this one lands, so it reserves a later run. Every
//! replica folds each applied stamp into its clock, so a new leader stamps
//! above every entry it applied. The clock also folds the group's ordinal
//! floor, which a leader gathers from a quorum before its first write, so a
//! new leader stamps above every read cut an earlier leader closed, whatever
//! its own clock reads. Every replica installs the stamp the entry
//! carries, so a version has one system time on every replica.
//!
//! An entry a mirror's applier proposed differs in two ways. It takes no key:
//! the source's log already ordered it, and a mirror has no other writer. It
//! keeps the ordinal its source stamped, which every replica installs, so the
//! leader folds that ordinal into its clock and stamps nothing.

use std::sync::{Arc, Weak};

use nodedb_cluster::{Admitted, CalvinError, ClusterError, DataProposeGate, ProposeHold};

use crate::control::read_cut::OrdinalReservation;
use crate::control::state::SharedState;
use crate::control::wal_replication::decode::decode_parsed_entry;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::VShardId;

use super::admission_keys::collection_admission_keys;
use super::entry_scope::{EntryScope, entry_scope};
use super::entry_storage::{entry_adds_storage, entry_writes_database};
use super::gate::{
    WriteAdmission, WriteAdmissionGuard, WriteTarget, admit_request, admit_routed, lock_manager_of,
};
use super::mirror_gate::{await_entry_metadata_floor, check_mirror_proposal};
use super::returning_route::routes_for_returning;
use super::route::calvin_route_keeps;
use super::storage_quota::check_storage_quota;
use super::tickets::AdmissionTicket;

/// The host side of [`DataProposeGate`]. Weak: `SharedState` outlives the
/// Raft loop that holds the gate only while the node runs.
pub struct LeaderWriteGate {
    state: Weak<SharedState>,
}

impl LeaderWriteGate {
    pub fn new(state: &Arc<SharedState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }
}

#[async_trait::async_trait]
impl DataProposeGate for LeaderWriteGate {
    async fn admit(
        &self,
        vshard_id: u32,
        entry: &[u8],
        deadline: tokio::time::Instant,
        ordinal_floor: i64,
    ) -> nodedb_cluster::Result<Admitted> {
        // A node that is shutting down cannot stamp. It refuses every entry
        // that takes a stamp, and proposes the rest as sent.
        let Some(state) = self.state.upgrade() else {
            return admit_without_state(entry);
        };
        let vshard = VShardId::new(vshard_id);
        // Bytes that are not a replicated entry apply as nothing on any replica.
        let Some(mut entry) = ReplicatedEntry::from_bytes(entry) else {
            return Ok(Admitted::default());
        };
        // Before the reroute to the sequencer and before any key is held.
        await_entry_metadata_floor(&state, entry.metadata_floor, deadline)
            .await
            .map_err(gate_error)?;
        check_mirror_proposal(&state, &entry, || entry_writes_database(&entry))
            .map_err(gate_error)?;
        admit_redo_stream(&state, &entry, deadline).map_err(gate_error)?;
        check_storage_quota(
            &state,
            crate::types::DatabaseId::new(entry.database_id),
            crate::types::TenantId::new(entry.tenant_id),
            crate::event::EventSource::from(entry.event_source),
            || entry_adds_storage(&entry),
        )
        .map_err(gate_error)?;
        if entry.mirror.is_some() {
            fold_carried_ordinal(&state, &entry, ordinal_floor);
            return Ok(Admitted::default());
        }
        let hold: Option<Box<dyn ProposeHold>> =
            match admit_entry(&state, vshard, &entry).map_err(gate_error)? {
                WriteAdmission::ExemptRead | WriteAdmission::FastPath { guard: None } => None,
                // No scheduler runs for the vShard here: the log alone orders
                // the write. Its landing fences the vShard's next scheduler.
                WriteAdmission::FastPathBlocking { ticket, .. }
                | WriteAdmission::Unscheduled { ticket } => {
                    Some(Box::new(UnscheduledLanding { ticket }))
                }
                WriteAdmission::FastPath { guard: Some(guard) } => Some(Box::new(LandedHold {
                    state: Arc::clone(&state),
                    vshard,
                    guard,
                })),
                WriteAdmission::RouteToCalvin => {
                    return Err(ClusterError::Calvin(CalvinError::RouteToSequencer));
                }
                WriteAdmission::Wait(wait) => {
                    let guard = wait.acquire(deadline).await.map_err(gate_error)?;
                    Some(Box::new(LandedHold {
                        state: Arc::clone(&state),
                        vshard,
                        guard,
                    }))
                }
            };
        // Stamped once the entry holds its keys. A refusal past this point
        // drops `hold`, which frees them.
        let (entry, reservation) =
            stamp_system_ordinal(&state, &mut entry, ordinal_floor).map_err(gate_error)?;
        // The reservation rides the hold: it stays open until this node
        // starts the entry's apply, where the apply's own reservation takes
        // over (`read_cut::reservations`). A propose that fails drops it.
        let hold = match reservation {
            Some(reservation) => Some(Box::new(ReservedHold {
                keys: hold,
                state: Arc::clone(&state),
                vshard,
                reservation,
            }) as Box<dyn ProposeHold>),
            None => hold,
        };
        Ok(Admitted { hold, entry })
    }
}

/// Fold a mirror entry's carried ordinal into this leader's clock. The entry
/// proposes as sent, so its ordinal stays its source's. The clock lands above
/// it, so a write after PROMOTE stamps above every version the mirror holds.
fn fold_carried_ordinal(state: &SharedState, entry: &ReplicatedEntry, ordinal_floor: i64) {
    let clock = state.wal.ordinal_clock();
    clock.update_from_remote(ordinal_floor);
    clock.update_from_remote(entry.system_ordinal);
}

/// Reserve the run of system-time ordinals `entry`'s write stamps from this
/// leader's clock, write its first ordinal into the entry, and return the
/// re-encoded entry. `None` for a write that lands no system-time version:
/// its bytes propose as sent.
///
/// The leader's stamp replaces any stamp the bytes carry, so a re-proposal
/// to a new leader stamps from the new leader's clock.
///
/// The run is drawn under the reservation registry's lock, and the returned
/// reservation stays open until the write applied here.
fn stamp_system_ordinal(
    state: &SharedState,
    entry: &mut ReplicatedEntry,
    ordinal_floor: i64,
) -> crate::Result<(Option<Vec<u8>>, Option<OrdinalReservation>)> {
    let Some(span) = entry.write.system_ordinal_span() else {
        return Ok((None, None));
    };
    let clock = state.wal.ordinal_clock();
    // The group's floor names every read cut an earlier leader closed. The
    // clock folds it first, so the run reserved below lies above each one.
    clock.update_from_remote(ordinal_floor);
    // A TTL reap ends its version at its expiry instant: the run, and every
    // later write of the key, lies above it.
    clock.update_from_remote(entry.write.ordinal_floor());
    let (ordinal, reservation) = state.read_cut.reservations.reserve(|| clock.reserve(span));
    entry.system_ordinal = ordinal;
    Ok((Some(entry.encode()?), Some(reservation)))
}

/// The gate's answer once this node's state is gone and it cannot stamp.
///
/// An entry that takes this leader's stamp is refused as `NotLeader` with
/// no hint. The proposer maps that to `Error::NoLeader`
/// (`start_raft::propose_error`) and retries against the group's next
/// leader. Proposing it unstamped lets each replica stamp it from its
/// own clock. Every other entry takes no stamp, so it proposes as sent:
/// bytes that are not an entry, a mirror entry, and a write that lands no
/// system-time version.
fn admit_without_state(entry: &[u8]) -> nodedb_cluster::Result<Admitted> {
    match ReplicatedEntry::from_bytes(entry) {
        Some(entry) if takes_leader_stamp(&entry) => {
            Err(ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                leader_hint: None,
                term: 0,
            }))
        }
        Some(_) | None => Ok(Admitted::default()),
    }
}

/// Whether the gate stamps `entry` with this leader's ordinals. A mirror
/// entry keeps its source's ordinal (`fold_carried_ordinal`). Any other
/// entry takes a stamp when its write lands a system-time version
/// ([`ReplicatedWrite::system_ordinal_span`]), as `stamp_system_ordinal`
/// decides.
fn takes_leader_stamp(entry: &ReplicatedEntry) -> bool {
    entry.mirror.is_none() && entry.write.system_ordinal_span().is_some()
}

/// An admitted entry's keys, if it holds any, and its ordinal reservation.
/// On landing the keys go to their hold, and the reservation stays open
/// until this node starts the entry's apply (`AdmissionHolds`).
struct ReservedHold {
    keys: Option<Box<dyn ProposeHold>>,
    state: Arc<SharedState>,
    vshard: VShardId,
    reservation: OrdinalReservation,
}

impl ProposeHold for ReservedHold {
    fn landed(self: Box<Self>, group_id: u64, log_index: u64) {
        let ReservedHold {
            keys,
            state,
            vshard,
            reservation,
        } = *self;
        if let Some(keys) = keys {
            keys.landed(group_id, log_index);
        }
        state
            .calvin
            .admission_holds
            .hold_reservation(group_id, log_index, vshard, reservation);
    }
}

/// An admitted entry's keys, held until the entry lands in the log.
struct LandedHold {
    state: Arc<SharedState>,
    vshard: VShardId,
    guard: WriteAdmissionGuard,
}

impl ProposeHold for LandedHold {
    fn landed(self: Box<Self>, group_id: u64, log_index: u64) {
        let LandedHold {
            state,
            vshard,
            mut guard,
        } = *self;
        // The ticket records the index before the keys move to the holds. A
        // scheduler that starts on a new table waits for that index.
        guard.landed(group_id, log_index);
        state
            .calvin
            .admission_holds
            .hold(group_id, log_index, vshard, guard);
    }
}

/// An admitted entry of a vShard with no scheduler. Its landing index fences
/// the vShard's next scheduler.
struct UnscheduledLanding {
    ticket: AdmissionTicket,
}

impl ProposeHold for UnscheduledLanding {
    fn landed(self: Box<Self>, group_id: u64, log_index: u64) {
        self.ticket.landed(group_id, log_index);
    }
}

/// Admit the first chunk of a chunked redo stream against this node's open
/// stream bytes. Every replica takes the chunks the log holds, so the leader
/// is the one place a stream is refused for memory. Any other entry passes.
fn admit_redo_stream(
    state: &SharedState,
    entry: &ReplicatedEntry,
    deadline: tokio::time::Instant,
) -> crate::Result<()> {
    let ReplicatedWrite::RedoChunk {
        stream,
        index: 0,
        len,
        ..
    } = &entry.write
    else {
        return Ok(());
    };
    state
        .redo_chunks
        .admit_stream(
            stream.in_mirror(entry.mirror_database()),
            *len,
            deadline.into_std(),
        )
        .map_err(crate::Error::from)
}

/// Admit `entry`, proposed to `vshard`'s group.
pub(crate) fn admit_entry(
    state: &SharedState,
    vshard: VShardId,
    entry: &ReplicatedEntry,
) -> crate::Result<WriteAdmission> {
    let database_id = crate::types::DatabaseId::new(entry.database_id);
    match entry_scope(&entry.write, &entry.incarnations, database_id) {
        EntryScope::Exempt => Ok(WriteAdmission::ExemptRead),
        EntryScope::Collections(collections) => Ok(admit_collections(state, vshard, &collections)),
        EntryScope::Plan => {
            let Some((_, (tenant_id, _, plan, _))) = decode_parsed_entry(entry)? else {
                return Ok(WriteAdmission::ExemptRead);
            };
            // A client write whose `RETURNING` list can fail never commits
            // before its list is evaluated, whatever path proposed it.
            if routes_for_returning(state, entry, tenant_id, &plan) {
                return Ok(WriteAdmission::RouteToCalvin);
            }
            // An entry the scheduler cannot apply as proposed waits for its
            // keys rather than route.
            let may_route = calvin_route_keeps(
                &plan,
                crate::event::EventSource::from(entry.event_source),
                entry.restore_id,
            );
            admit_routed(
                state,
                &WriteTarget {
                    tenant_id,
                    database_id,
                    vshard_id: vshard,
                    plan: &plan,
                },
                may_route,
            )
        }
    }
}

/// Admit a write that locks each of `collections` whole.
fn admit_collections(
    state: &SharedState,
    vshard: VShardId,
    collections: &[String],
) -> WriteAdmission {
    // The ticket opens before the table lookup (see `tickets`).
    let ticket = state.calvin.admission_tickets.open(vshard.as_u32());
    match lock_manager_of(state, vshard) {
        Some(lock_manager) => admit_request(
            state,
            vshard,
            lock_manager,
            collection_admission_keys(collections),
        )
        .carrying(ticket),
        None => WriteAdmission::Unscheduled { ticket },
    }
}

/// The cluster error a gate failure answers the proposer with. A deadline
/// keeps its class across the hop. Every other error crosses typed.
fn gate_error(error: crate::Error) -> ClusterError {
    match error {
        crate::Error::DeadlineExceeded { .. } => {
            ClusterError::Calvin(CalvinError::AdmissionTimedOut)
        }
        other => {
            let detail = format!("data-group write gate: {other}");
            ClusterError::ShardExecution {
                error: Box::new(
                    crate::control::cluster::execution_error_wire::execution_error_to_typed(other),
                ),
                detail,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nodedb_types::{DatabaseId, MirrorEntryStamp};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    const MIRROR: u64 = 1400;

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        state
            .mirror
            .fence
            .mark_read_only_for_test(DatabaseId::new(MIRROR), "mirror_db");
        (state, dir)
    }

    /// A topic message counts as a database write, and it locks no key.
    fn write(system_ordinal: i64) -> ReplicatedEntry {
        let mut entry = ReplicatedEntry::new(
            1,
            MIRROR,
            2,
            ReplicatedWrite::TopicPublish {
                topic: "t".into(),
                payload: "p".into(),
                event_time: 1,
                origin: None,
            },
        )
        .with_event_source(crate::event::EventSource::RaftFollower);
        entry.system_ordinal = system_ordinal;
        entry
    }

    fn deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(5)
    }

    #[tokio::test]
    async fn an_unstamped_applier_source_is_refused() {
        let (state, _dir) = state();
        let gate = LeaderWriteGate::new(&state);
        let bytes = write(0).encode().expect("encode");
        let refused = gate.admit(2, &bytes, deadline(), 0).await;
        assert!(refused.is_err());
    }

    /// A KV delete lands a system-time version, so the gate stamps it.
    fn user_write() -> ReplicatedEntry {
        ReplicatedEntry::new(
            1,
            0,
            2,
            ReplicatedWrite::KvDelete {
                collection: "c".into(),
                keys: vec![b"k".to_vec()],
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            },
        )
    }

    /// A gate whose node state is gone, as at shutdown.
    fn stateless_gate() -> LeaderWriteGate {
        LeaderWriteGate { state: Weak::new() }
    }

    /// With its state gone the gate cannot stamp, so it refuses a write that
    /// takes a stamp as a retryable `NotLeader`, and proposes nothing.
    #[tokio::test]
    async fn a_gate_without_state_refuses_a_write_that_takes_a_stamp() {
        let bytes = user_write().encode().expect("encode");
        let refused = stateless_gate().admit(2, &bytes, deadline(), 0).await;
        assert!(
            matches!(
                refused,
                Err(ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                    leader_hint: None,
                    ..
                }))
            ),
            "a stamp-taking write is refused"
        );
    }

    /// A write that lands no system-time version, and a mirror entry, take no
    /// stamp: they pass a gate without state as sent.
    #[tokio::test]
    async fn a_gate_without_state_passes_an_entry_that_takes_no_stamp() {
        let gate = stateless_gate();
        let topic = write(0).encode().expect("encode");
        let admitted = gate
            .admit(2, &topic, deadline(), 0)
            .await
            .expect("a topic message takes no stamp");
        assert!(admitted.entry.is_none(), "the bytes propose as sent");

        let mut mirrored = user_write();
        mirrored.system_ordinal = 41;
        mirrored.mirror = Some(MirrorEntryStamp {
            source_group: 1,
            source_index: 9,
            cut: 1,
            epoch: 1,
        });
        let bytes = mirrored.encode().expect("encode");
        let admitted = gate
            .admit(2, &bytes, deadline(), 0)
            .await
            .expect("a mirror entry keeps its source's stamp");
        assert!(admitted.entry.is_none(), "the bytes propose as sent");
    }

    /// A stamped entry takes no key and proposes as sent, so its ordinal stays
    /// its source's. The clock lands above that ordinal.
    #[tokio::test]
    async fn a_stamped_entry_keeps_its_ordinal_and_folds_the_clock() {
        let (state, _dir) = state();
        let gate = LeaderWriteGate::new(&state);
        let carried = i64::MAX / 2;
        let mut entry = write(carried);
        entry.mirror = Some(MirrorEntryStamp {
            source_group: 1,
            source_index: 9,
            cut: 1,
            epoch: 1,
        });
        let bytes = entry.encode().expect("encode");
        let admitted = gate
            .admit(2, &bytes, deadline(), 0)
            .await
            .expect("a stamped entry passes");
        assert!(admitted.entry.is_none(), "the bytes propose as sent");
        assert!(admitted.hold.is_none(), "the entry holds no key");
        assert!(state.wal.ordinal_clock().next_ordinal() > carried);
    }
}
