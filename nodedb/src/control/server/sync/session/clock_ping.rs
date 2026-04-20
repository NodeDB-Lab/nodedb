//! Vector clock sync + ping/pong.

use std::time::Instant;

use tracing::debug;

use super::super::wire::*;
use super::state::SyncSession;

impl SyncSession {
    /// Update the server's view of the client's clock and return the
    /// server's current clock.
    pub fn handle_vector_clock_sync(&mut self, msg: &VectorClockSyncMsg) -> SyncFrame {
        self.last_activity = Instant::now();
        for (collection, lsn) in &msg.clocks {
            self.server_clock
                .entry(collection.clone())
                .and_modify(|v| *v = (*v).max(*lsn))
                .or_insert(*lsn);
        }
        debug!(
            session = %self.session_id,
            collections = msg.clocks.len(),
            "vector clock sync"
        );
        let response = VectorClockSyncMsg {
            clocks: self.server_clock.clone(),
            sender_id: 0,
        };
        SyncFrame::encode_or_empty(SyncMessageType::VectorClockSync, &response)
    }

    pub fn handle_ping(&mut self, msg: &PingPongMsg) -> SyncFrame {
        self.last_activity = Instant::now();
        let pong = PingPongMsg {
            timestamp_ms: msg.timestamp_ms,
            is_pong: true,
        };
        SyncFrame::encode_or_empty(SyncMessageType::PingPong, &pong)
    }
}
