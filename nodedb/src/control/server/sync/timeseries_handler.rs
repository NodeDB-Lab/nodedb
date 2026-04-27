//! Timeseries push handler for sync sessions.
//!
//! Decodes Gorilla-encoded metric blocks from Lite, builds ILP-format
//! payloads with `__source` tag for Data Plane ingest.

use tracing::debug;

use super::session::SyncSession;
use super::wire::*;

/// Data extracted from a timeseries push for async Data Plane dispatch.
#[derive(Debug)]
pub struct TimeseriesIngestData {
    /// Collection to ingest into.
    pub collection: String,
    /// ILP-format payload with `__source` tag injected.
    pub ilp_payload: Vec<u8>,
    /// Source Lite instance ID.
    pub lite_id: String,
    /// Number of decoded samples.
    pub sample_count: u64,
}

impl SyncSession {
    /// Process a timeseries push: decode Gorilla blocks, prepare for Data Plane ingest.
    ///
    /// Returns `(ack_frame, ingest_payload)`. The caller (listener) must dispatch
    /// `ingest_payload` to the Data Plane via `dispatch_to_data_plane` with a
    /// `TimeseriesIngest` physical plan. The ACK is sent immediately (optimistic),
    /// similar to how `DeltaPush` works.
    pub fn handle_timeseries_push(
        &mut self,
        msg: &TimeseriesPushMsg,
    ) -> (Option<SyncFrame>, Option<TimeseriesIngestData>) {
        self.last_activity = std::time::Instant::now();

        if !self.authenticated {
            let ack = TimeseriesAckMsg {
                collection: msg.collection.clone(),
                accepted: 0,
                rejected: msg.sample_count,
                lsn: 0,
            };
            return (
                SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack),
                None,
            );
        }

        // Decode Gorilla blocks to verify integrity.
        let timestamps = nodedb_codec::GorillaDecoder::new(&msg.ts_block).decode_all();
        let values = nodedb_codec::GorillaDecoder::new(&msg.val_block).decode_all();

        let decoded_count = timestamps.len().min(values.len());
        if decoded_count == 0 {
            let ack = TimeseriesAckMsg {
                collection: msg.collection.clone(),
                accepted: 0,
                rejected: msg.sample_count,
                lsn: 0,
            };
            return (
                SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack),
                None,
            );
        }

        self.mutations_processed += decoded_count as u64;

        // Build ILP-format payload for Data Plane ingest.
        // Each decoded sample becomes an ILP line with `__source` tag.
        let mut ilp_lines = String::with_capacity(decoded_count * 80);
        for i in 0..decoded_count {
            let (ts, _) = timestamps[i];
            let (_, val) = values[i];
            // ILP format: measurement,__source=lite_id value=X timestamp_ns
            ilp_lines.push_str(&msg.collection);
            ilp_lines.push_str(",__source=");
            ilp_lines.push_str(&msg.lite_id);
            ilp_lines.push_str(" value=");
            ilp_lines.push_str(&val.to_string());
            ilp_lines.push(' ');
            // Convert ms to ns for ILP.
            ilp_lines.push_str(&(ts * 1_000_000).to_string());
            ilp_lines.push('\n');
        }

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            decoded = decoded_count,
            lite_id = %msg.lite_id,
            "timeseries push decoded, dispatching to Data Plane"
        );

        let ack = TimeseriesAckMsg {
            collection: msg.collection.clone(),
            accepted: decoded_count as u64,
            rejected: msg.sample_count.saturating_sub(decoded_count as u64),
            lsn: self.mutations_processed,
        };

        let ingest = TimeseriesIngestData {
            collection: msg.collection.clone(),
            ilp_payload: ilp_lines.into_bytes(),
            lite_id: msg.lite_id.clone(),
            sample_count: decoded_count as u64,
        };

        (
            SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack),
            Some(ingest),
        )
    }
}
