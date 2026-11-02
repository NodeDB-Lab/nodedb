// SPDX-License-Identifier: BUSL-1.1

//! Decode one WAL `VectorParams` record and re-register the index
//! configuration it carries.
//!
//! Split out of `wal_replay_vector` so that file stays focused on driving the
//! record scan. The tuple shape lives here with the encoder it mirrors
//! (`control::server::wal_dispatch::vector`).

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::vector::distance::DistanceMetric;
use crate::engine::vector::hnsw::HnswParams;

/// The `VectorParams` payload the WAL writer encodes: `(collection, m,
/// ef_construction, metric, index_type, pq_m, ivf_cells, ivf_nprobe,
/// field_name, dim)`.
type VectorParamsPayload = (
    String,
    usize,
    usize,
    String,
    String,
    usize,
    usize,
    usize,
    String,
    usize,
);

impl CoreLoop {
    /// Restore the vector-index configuration a `VectorParams` record carries.
    ///
    /// A record for a tombstoned collection is counted in `skipped` and
    /// otherwise ignored. A payload that does not decode, or that names an
    /// unknown metric, is a committed record this build cannot apply. Replay
    /// halts on it.
    pub(in crate::data::executor) fn restore_vector_params_record(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        record_lsn: u64,
        payload: &[u8],
        tombstones: &nodedb_wal::replay::filter::DatabaseTombstones<'_>,
        skipped: &mut usize,
    ) {
        let (collection, m, ef_construction, metric, _it, _pq, _ic, _ip, field_name, dim) =
            match zerompk::from_msgpack::<VectorParamsPayload>(payload) {
                Ok(decoded) => decoded,
                Err(e) => {
                    self.replay_record_unapplied(
                        "vector",
                        "params_decode",
                        record_lsn,
                        &format!("VectorParams payload does not decode: {e}"),
                    );
                    *skipped += 1;
                    return;
                }
            };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            *skipped += 1;
            return;
        }
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field_name);
        // An empty metric keeps the configured one, as the live
        // `execute_set_vector_params` does. With none configured it is cosine.
        let metric_enum = match metric.as_str() {
            "" => self
                .vector_params
                .get(&index_key)
                .map_or(DistanceMetric::Cosine, |existing| existing.metric),
            "l2" | "euclidean" => DistanceMetric::L2,
            "cosine" => DistanceMetric::Cosine,
            "inner_product" | "ip" | "dot" => DistanceMetric::InnerProduct,
            "manhattan" | "l1" => DistanceMetric::Manhattan,
            "chebyshev" | "linf" => DistanceMetric::Chebyshev,
            "hamming" => DistanceMetric::Hamming,
            "jaccard" => DistanceMetric::Jaccard,
            "pearson" => DistanceMetric::Pearson,
            unknown => {
                self.replay_record_unapplied(
                    "vector",
                    "params_metric",
                    record_lsn,
                    &format!("VectorParams for '{collection}' names unknown metric '{unknown}'"),
                );
                *skipped += 1;
                return;
            }
        };
        let params = HnswParams {
            m,
            m0: m * 2,
            ef_construction,
            metric: metric_enum,
            dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
        };
        if dim > 0 {
            self.declared_dims.insert(index_key.clone(), dim);
        }
        self.vector_params.insert(index_key, params);
        tracing::debug!(
            core = self.core_id,
            %collection,
            field = %field_name,
            dim,
            m,
            ef_construction,
            %metric,
            "WAL replay: restored vector params"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::data::executor::core_loop::CoreLoop;
    use crate::engine::vector::distance::DistanceMetric;

    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    fn restore(core: &mut CoreLoop, payload: &[u8]) -> usize {
        let tombstones = nodedb_wal::TombstoneSet::new();
        let mut skipped = 0;
        core.restore_vector_params_record(
            0,
            7,
            5,
            payload,
            &tombstones.for_database(0),
            &mut skipped,
        );
        skipped
    }

    fn payload(metric: &str) -> Vec<u8> {
        zerompk::to_msgpack_vec(&(
            "emb".to_string(),
            16usize,
            200usize,
            metric.to_string(),
            "hnsw".to_string(),
            8usize,
            256usize,
            16usize,
            "body".to_string(),
            3usize,
        ))
        .expect("encode")
    }

    /// The shape the WAL writer encodes restores the configuration and the
    /// declared dimension.
    #[test]
    fn the_written_shape_restores_params() {
        let mut h = make_core();
        assert_eq!(restore(&mut h.core, &payload("l2")), 0);
        let key = CoreLoop::vector_index_key(0, 7, "emb", "body");
        let params = h.core.vector_params.get(&key).expect("params restored");
        assert_eq!(params.metric, DistanceMetric::L2);
        assert_eq!(params.m, 16);
        assert_eq!(h.core.declared_dims.get(&key), Some(&3));
        assert!(!h.core.replay_halted());
    }

    /// A four-element tuple is not the written shape. Replay halts on it
    /// instead of restoring a partial configuration.
    #[test]
    fn a_short_tuple_halts_replay() {
        let mut h = make_core();
        let short =
            zerompk::to_msgpack_vec(&("emb".to_string(), 16usize, 200usize, "l2")).expect("encode");
        assert_eq!(restore(&mut h.core, &short), 1);
        assert!(h.core.vector_params.is_empty());
        assert!(h.core.replay_halted());
    }

    /// An unknown metric halts replay instead of building a cosine index.
    #[test]
    fn an_unknown_metric_halts_replay() {
        let mut h = make_core();
        assert_eq!(restore(&mut h.core, &payload("bogus")), 1);
        assert!(h.core.vector_params.is_empty());
        assert!(h.core.replay_halted());
    }
}
