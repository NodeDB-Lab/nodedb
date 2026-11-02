// SPDX-License-Identifier: BUSL-1.1

//! Load every sink's durable state before a consumer delivers an event.

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::watermark::WatermarkStore;
use crate::wal::WalManager;

use super::ledgers::SinkLedgers;

/// Restore the streaming views with their applied keys and the change-stream
/// buffers with their routed keys, and install the audit, CRDT and CDC
/// ledgers. A replayed event then reaches each sink once.
pub fn load_sink_state(
    shared: &SharedState,
    wal: &WalManager,
    watermarks: &WatermarkStore,
    num_cores: usize,
) -> crate::Result<()> {
    shared.mv_persistence.restore_all(&shared.mv_registry)?;
    let ledgers = SinkLedgers::open(wal, shared, watermarks, num_cores)?;
    ledgers.cdc.restore_into(&shared.cdc_router)?;
    // A snapshot install before the ledger opened raised its floors in
    // memory only.
    ledgers
        .cdc
        .persist_floors(&shared.cdc_router.availability().all())?;
    // A WAL catch-up can rebuild events of records applied before the
    // restart. Their replicated positions come back from the WAL markers.
    // A refused forward record keeps its position: the events rebuilt from
    // its write's group parts name its LSN.
    shared
        .cdc_router
        .positions()
        .recover(&wal.replay_with_refused()?);
    // The Control-Plane change feeds come back from their journal, so a
    // cursor inside retention resumes after the restart.
    let journal = crate::control::change_stream::ChangeJournal::open(
        watermarks.dir(),
        shared.change_stream.capacity(),
    )?;
    shared.change_stream.attach_journal(Arc::new(journal))?;
    if shared.sink_ledgers.set(Arc::new(ledgers)).is_err() {
        return Err(crate::Error::Internal {
            detail: "event plane sink ledgers were already installed".into(),
        });
    }
    Ok(())
}
