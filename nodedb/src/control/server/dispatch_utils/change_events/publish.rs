// SPDX-License-Identifier: BUSL-1.1

//! CDC change-event publishing for dispatched writes: turning the metadata
//! [`super::extract`] derived from a plan into `ChangeEvent`s on the
//! change stream, at the position of the write in its partition's feed.

use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::change_stream::stream::Settled;
use crate::control::change_stream::{ChangeEvent, ChangeOperation, ChangePartition};
use crate::control::state::SharedState;
use crate::event::cdc::position::MarkerChanges;
use crate::types::{DatabaseId, Lsn, TenantId};

use super::extract::{WriteChangeMeta, extract_write_metadata};

const NANOS_PER_MS: u64 = 1_000_000;

/// The change-event timestamp of a write that committed at `commit_hlc`
/// (HLC wall time, nanoseconds). Every replica reads the same commit HLC
/// from the committed entry, so every node stamps the same value.
fn commit_timestamp_ms(commit_hlc: u64) -> u64 {
    commit_hlc / NANOS_PER_MS
}

/// Check if a timeseries collection has CDC enabled.
///
/// Returns `false` (CDC off) by default for timeseries to prevent
/// high-cardinality metric streams from flooding the ChangeStream bus.
/// Users opt in via `CREATE TIMESERIES name WITH (cdc = 'true')`.
fn is_timeseries_cdc_enabled(
    shared: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> bool {
    let catalog = shared.credentials.catalog();
    if let Ok(Some(coll)) = catalog.get_collection(database_id, tenant_id.as_u64(), collection)
        && coll.collection_type.is_timeseries()
    {
        if let Some(config) = coll.get_timeseries_config()
            && let Some(cdc_val) = config.get("cdc")
        {
            return cdc_val.as_str() == Some("true") || cdc_val.as_bool() == Some(true);
        }
        // Default: CDC off for timeseries.
        return false;
    }
    // Not timeseries or catalog unavailable — allow publishing.
    true
}

/// The change event of one row change, or `None` when its collection
/// publishes none: a timeseries collection publishes only with `cdc`
/// enabled.
///
/// The plan names the collection database-qualified. The event names it
/// bare, as the catalog and every subscriber filter do; its database travels
/// beside it.
fn change_event(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    change_meta: WriteChangeMeta,
    lsn: nodedb_types::Lsn,
    commit_hlc: u64,
) -> Option<ChangeEvent> {
    let (qualified, document_id, op) = change_meta;
    let collection = match nodedb_types::CollectionKey::from_qualified_str(database_id, &qualified)
    {
        Ok(key) => key.name().to_owned(),
        Err(error) => {
            tracing::error!(
                %error,
                collection = %qualified,
                "a row change names a collection outside its database; it publishes no event"
            );
            return None;
        }
    };
    if !is_timeseries_cdc_enabled(shared, database_id, tenant_id, &collection) {
        return None;
    }
    Some(ChangeEvent {
        lsn,
        tenant_id,
        collection,
        document_id,
        operation: op,
        timestamp_ms: commit_timestamp_ms(commit_hlc),
        after: None,
    })
}

/// The Control-Plane change events one write plan yields.
///
/// Extraction is split from publishing because the write funnel consumes the
/// plan: it moves into the `Request` before the write applies. The events
/// stage only once the write applied.
pub(crate) struct WriteChangeSet {
    /// One tuple per logical row change — see `extract_write_metadata`.
    metas: Vec<WriteChangeMeta>,
}

/// Derive a plan's change events. Pure: it matches over the plan and clones out
/// collection / document identity, touching no shared state, so it is safe to
/// call at the one point where the plan is still owned.
pub(crate) fn extract_write_change_set(plan: &PhysicalPlan, tenant_id: TenantId) -> WriteChangeSet {
    WriteChangeSet {
        metas: extract_write_metadata(plan, tenant_id),
    }
}

fn change_events(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    change_set: WriteChangeSet,
    lsn: nodedb_types::Lsn,
    commit_hlc: u64,
) -> Vec<ChangeEvent> {
    change_set
        .metas
        .into_iter()
        .filter_map(|meta| change_event(shared, tenant_id, database_id, meta, lsn, commit_hlc))
        .collect()
}

impl WriteChangeSet {
    /// The change set of a redo's named row changes: one page of a chunked
    /// redo's row-change section, or the rows a change marker's tail names.
    pub(crate) fn of_row_changes(changes: Vec<crate::wal::RedoRowChange>) -> Self {
        Self {
            metas: changes
                .into_iter()
                .filter_map(super::redo::named_row_change)
                .collect(),
        }
    }

    /// The row changes the set names, in its order. [`Self::of_row_changes`]
    /// reads them back to the same set.
    fn row_changes(&self) -> Vec<crate::wal::RedoRowChange> {
        self.metas
            .iter()
            .map(
                |(collection, identity, operation)| crate::wal::RedoRowChange {
                    collection: collection.clone(),
                    row: identity.as_str().to_owned(),
                    kind: match operation {
                        ChangeOperation::Insert => crate::wal::RedoRowKind::Insert,
                        ChangeOperation::Update => crate::wal::RedoRowKind::Update,
                        ChangeOperation::Delete => crate::wal::RedoRowKind::Delete,
                    },
                },
            )
            .collect()
    }

    /// Whether the write yields no change event.
    pub(crate) fn is_empty(&self) -> bool {
        self.metas.is_empty()
    }
}

/// The tail of the change marker of a write whose plan is `plan`: the rows
/// the write yields.
pub(crate) fn marker_changes_of_plan(plan: &PhysicalPlan, tenant_id: TenantId) -> MarkerChanges {
    MarkerChanges::Rows(extract_write_change_set(plan, tenant_id).row_changes())
}

/// A replicated write's change set before its entry's change marker is
/// appended.
pub(crate) struct EntryChanges {
    change_set: WriteChangeSet,
    /// The data group of the committed entry the write applies.
    group_id: u64,
    /// The entry's log index in its group.
    log_index: u64,
    /// Whether the write's own `TransactionRedo` record names the rows.
    rows_in_redo: bool,
}

impl EntryChanges {
    /// The change set of `plan`, the write that applies the data-group
    /// entry `(group_id, log_index)`.
    pub(crate) fn of_entry(
        plan: &PhysicalPlan,
        tenant_id: TenantId,
        group_id: u64,
        log_index: u64,
    ) -> Self {
        Self {
            change_set: extract_write_change_set(plan, tenant_id),
            group_id,
            log_index,
            // The change set of a committed redo is its record's
            // `row_changes` (see `redo::redo_change_meta`), and the WAL record
            // keeps them whole: inline verbatim, and a split record's origin
            // keeps every field.
            rows_in_redo: matches!(
                plan,
                PhysicalPlan::Meta(
                    nodedb_physical::physical_plan::MetaOp::ApplyTransactionRedo { .. }
                )
            ),
        }
    }

    /// The tail of the entry's change marker. A restart that delivers the
    /// entry again stages its events from it: from the rows it names, or
    /// from the rows the write's `TransactionRedo` record names.
    pub(crate) fn marker_changes(&self) -> MarkerChanges {
        if self.rows_in_redo {
            return MarkerChanges::OfRedo;
        }
        MarkerChanges::Rows(self.change_set.row_changes())
    }

    /// The change set staged at `lsn`, the LSN of the entry's change marker.
    pub(crate) fn at_marker(self, lsn: Lsn) -> PendingChanges {
        PendingChanges::staged(self.change_set, self.group_id, self.log_index, lsn)
    }
}

/// A replicated write's change set, staged under its data-group entry once
/// the write applied.
pub(crate) struct PendingChanges {
    change_set: WriteChangeSet,
    /// The data group of the committed entry the write applies.
    group_id: u64,
    /// The entry's log index in its group.
    log_index: u64,
    /// The LSN every event names: the entry's change marker's. A restart
    /// that stages the events again reads the same marker, so the events
    /// name the same LSN with or without a crash.
    lsn: Lsn,
    /// HLC wall time, in nanoseconds, at which the write committed.
    commit_hlc: u64,
}

impl PendingChanges {
    /// Staged under the data-group entry `(group_id, log_index)` at `lsn`,
    /// and published when the apply loop settles the entry in log order
    /// ([`publish_settled_changes`]).
    pub(crate) fn staged(
        change_set: WriteChangeSet,
        group_id: u64,
        log_index: u64,
        lsn: Lsn,
    ) -> Self {
        Self {
            change_set,
            group_id,
            log_index,
            lsn,
            commit_hlc: 0,
        }
    }

    /// Stamp the write's commit HLC, which dates its events. A replicated
    /// write takes the HLC its proposer stamped on the entry.
    pub(crate) fn committed_at(mut self, commit_hlc: u64) -> Self {
        self.commit_hlc = commit_hlc;
        self
    }

    /// Stage the write's events under its entry. Almost every write plan
    /// yields exactly one event; a handful of multi-row / multi-collection
    /// ops yield more than one, and reads / DDL / index maintenance yield
    /// none.
    pub(crate) fn publish(
        self,
        shared: &SharedState,
        tenant_id: TenantId,
        database_id: DatabaseId,
    ) {
        let events = change_events(
            shared,
            tenant_id,
            database_id,
            self.change_set,
            self.lsn,
            self.commit_hlc,
        );
        shared
            .change_stream
            .stage(self.group_id, self.log_index, database_id, events);
    }

    /// Stage the events of one page of a large entry. Past the change
    /// stream's spill bound they move to the change journal, so the entry
    /// holds at most one page in memory. Fails when the page did not spill
    /// while an earlier one waits in memory: the caller stages it again.
    pub(crate) async fn publish_page(
        self,
        shared: &Arc<SharedState>,
        tenant_id: TenantId,
        database_id: DatabaseId,
    ) -> crate::Result<()> {
        let events = change_events(
            shared,
            tenant_id,
            database_id,
            self.change_set,
            self.lsn,
            self.commit_hlc,
        );
        crate::control::change_stream::ChangeStream::stage_page(
            shared,
            self.group_id,
            self.log_index,
            database_id,
            events,
        )
        .await
    }
}

/// Publish the staged changes of `group_id`'s entries `first..=last`, which
/// the apply loop settled in log order, and forward them to the nodes that
/// do not replicate the group when this node leads it.
///
/// Every replica publishes the group's writes at their log positions, so
/// every node serves the same feed. A large entry publishes page by page,
/// read back from the change journal on the blocking pool, and each page
/// goes out to the other nodes before the next one enters the bounded ring.
/// A settle from memory runs inline. Returns what the ring took that it did
/// not hold yet.
pub(crate) async fn publish_settled_changes(
    shared: &Arc<SharedState>,
    group_id: u64,
    first: u64,
    last: u64,
) -> Settled {
    if !shared.change_stream.settles_from_journal(group_id, last) {
        return settle_and_forward(shared, group_id, first, last);
    }
    let settler = Arc::clone(shared);
    let task =
        tokio::task::spawn_blocking(move || settle_and_forward(&settler, group_id, first, last));
    match task.await {
        Ok(settled) => settled,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // Cancelled at runtime shutdown with the settle: no floor covers its
        // entries, and a restart delivers them again.
        Err(error) => {
            tracing::error!(group_id, %error, "the settle of spilled change events did not finish");
            Settled::default()
        }
    }
}

fn settle_and_forward(shared: &Arc<SharedState>, group_id: u64, first: u64, last: u64) -> Settled {
    let partition = ChangePartition(group_id);
    shared
        .change_stream
        .settle_group_with(group_id, first, last, || {
            shared.change_stream.forward(shared, partition)
        })
}
