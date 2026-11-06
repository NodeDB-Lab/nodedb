// SPDX-License-Identifier: BUSL-1.1

//! The reserver a server runs: the metadata log in a cluster, the catalog alone
//! on a single node.
//!
//! A cluster's entry names the row it read and the row it writes. Apply writes it
//! only over that row, in log order, so each node and each mirror reaches the
//! same counter. A proposer reads its identity on the row to learn it won.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, Weak};

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_sequence_entry;
use crate::control::security::audit::log_commit::hand_off_worker;
use crate::control::security::catalog::sequence_types::{
    SequenceReservation, SequenceState, StoredSequence,
};
use crate::control::state::SharedState;

use super::reserve::{ReservedRange, SequenceRangeReserver, next_range};
use super::types::SequenceError;

/// Proposals one reservation takes while other nodes win the same row.
const MAX_PROPOSALS: usize = 16;

/// [`SequenceRangeReserver`] over one server's state.
pub struct SequenceRangeLog {
    shared: Weak<SharedState>,
    next_request: AtomicU64,
    /// Serializes a single node's read and write of the row.
    local: Mutex<()>,
}

fn unavailable(name: &str, detail: impl std::fmt::Display) -> SequenceError {
    SequenceError::Unavailable {
        name: name.to_string(),
        detail: detail.to_string(),
    }
}

/// Run `work` to its end on this runtime, off this Tokio worker.
fn block_on_propose<T>(
    name: &str,
    work: impl std::future::Future<Output = Result<T, SequenceError>>,
) -> Result<T, SequenceError> {
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|_| unavailable(name, "a metadata propose needs a Tokio runtime"))?;
    match handle.runtime_flavor() {
        tokio::runtime::RuntimeFlavor::MultiThread => hand_off_worker(|| handle.block_on(work)),
        flavor => Err(unavailable(
            name,
            format!("a metadata propose needs a multi-thread runtime, not {flavor:?}"),
        )),
    }
}

impl SequenceRangeLog {
    pub fn new(shared: Weak<SharedState>) -> Self {
        Self {
            shared,
            next_request: AtomicU64::new(nodedb_types::wall_now_ns_i64().unsigned_abs()),
            local: Mutex::new(()),
        }
    }

    fn state(&self, name: &str) -> Result<std::sync::Arc<SharedState>, SequenceError> {
        self.shared
            .upgrade()
            .ok_or_else(|| unavailable(name, "this server is shutting down"))
    }

    fn held_row(
        state: &SharedState,
        def: &StoredSequence,
    ) -> Result<Option<SequenceState>, SequenceError> {
        state
            .credentials
            .catalog()
            .get_sequence_state(def.database_id, def.tenant_id, &def.name)
            .map_err(|error| unavailable(&def.name, error))
    }

    /// Reserve on a node with no cluster: the row's fsync'd write is the record.
    fn reserve_locally(
        &self,
        state: &SharedState,
        def: &StoredSequence,
        period_key: &str,
        count: u64,
    ) -> Result<ReservedRange, SequenceError> {
        let _serial = self.local.lock().unwrap_or_else(|p| p.into_inner());
        let held = Self::held_row(state, def)?;
        let range = next_range(def, held.as_ref(), period_key, count)?;
        let row = reserved_row(def, &range, None);
        state
            .credentials
            .catalog()
            .put_sequence_state(&row)
            .map_err(|error| unavailable(&def.name, error))?;
        Ok(range)
    }

    /// Propose until this node's entry is the row, or another node's wins too often.
    async fn reserve_in_log(
        &self,
        state: &SharedState,
        def: &StoredSequence,
        period_key: &str,
        count: u64,
    ) -> Result<ReservedRange, SequenceError> {
        let reservation = SequenceReservation {
            node_id: state.node_id,
            request_id: self.next_request.fetch_add(1, Ordering::Relaxed),
        };
        for _ in 0..MAX_PROPOSALS {
            let held = Self::held_row(state, def)?;
            let range = next_range(def, held.as_ref(), period_key, count)?;
            let entry = CatalogEntry::ReserveSequenceRange {
                expected: held.map(Box::new),
                next: Box::new(reserved_row(def, &range, Some(reservation))),
            };
            propose_sequence_entry(state, &entry)
                .await
                .map_err(|error| unavailable(&def.name, error))?;
            let landed = Self::held_row(state, def)?;
            if landed.is_some_and(|row| row.reservation == Some(reservation)) {
                return Ok(range);
            }
        }
        Err(unavailable(
            &def.name,
            "other nodes took every range this node proposed; retry",
        ))
    }
}

/// The row a reservation of `range` writes.
fn reserved_row(
    def: &StoredSequence,
    range: &ReservedRange,
    reservation: Option<SequenceReservation>,
) -> SequenceState {
    SequenceState {
        database_id: def.database_id,
        tenant_id: def.tenant_id,
        name: def.name.clone(),
        current_value: range.last,
        is_called: true,
        epoch: def.epoch,
        period_key: range.period_key.clone(),
        reservation,
    }
}

impl SequenceRangeReserver for SequenceRangeLog {
    fn reserve(
        &self,
        def: &StoredSequence,
        period_key: &str,
        count: u64,
    ) -> Result<ReservedRange, SequenceError> {
        let state = self.state(&def.name)?;
        if state
            .mirror
            .fence
            .is_read_only(DatabaseId::new(def.database_id))
        {
            return Err(SequenceError::ReadOnlyMirror {
                name: def.name.clone(),
            });
        }
        if state.cluster_topology.is_none() {
            return self.reserve_locally(&state, def, period_key, count);
        }
        block_on_propose(
            &def.name,
            self.reserve_in_log(&state, def, period_key, count),
        )
    }

    fn publish(&self, row: &SequenceState) -> Result<(), SequenceError> {
        let state = self.state(&row.name)?;
        if state
            .mirror
            .fence
            .is_read_only(DatabaseId::new(row.database_id))
        {
            return Err(SequenceError::ReadOnlyMirror {
                name: row.name.clone(),
            });
        }
        if state.cluster_topology.is_none() {
            let _serial = self.local.lock().unwrap_or_else(|p| p.into_inner());
            return state
                .credentials
                .catalog()
                .put_sequence_state(row)
                .map_err(|error| unavailable(&row.name, error));
        }
        let entry = CatalogEntry::PutSequenceState(Box::new(row.clone()));
        block_on_propose(&row.name, async {
            propose_sequence_entry(&state, &entry)
                .await
                .map(drop)
                .map_err(|error| unavailable(&row.name, error))
        })
    }
}

/// Install the reserver on `shared`'s registry.
pub fn install_range_reserver(shared: &std::sync::Arc<SharedState>) {
    shared
        .sequence_registry
        .install_reserver(std::sync::Arc::new(SequenceRangeLog::new(
            std::sync::Arc::downgrade(shared),
        )));
}
