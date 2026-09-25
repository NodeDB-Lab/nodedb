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
    let ledgers = SinkLedgers::open(wal, watermarks, num_cores)?;
    ledgers.cdc.restore_into(&shared.cdc_router)?;
    if shared.sink_ledgers.set(Arc::new(ledgers)).is_err() {
        return Err(crate::Error::Internal {
            detail: "event plane sink ledgers were already installed".into(),
        });
    }
    Ok(())
}
