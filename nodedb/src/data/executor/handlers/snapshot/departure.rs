// SPDX-License-Identifier: BUSL-1.1

//! Purge the data of vShards this node left.
//!
//! Once a vShard's new owners serve it, its copy here is stale. The purge
//! removes it the way a data-group install clears a group before it installs,
//! with nothing installed after:
//!
//! - every collection homed on a departed vShard loses its rows in every
//!   engine, and keeps its definition;
//! - every array store drops the cells routed to a departed vShard;
//! - every node a departed vShard homes loses its label versions and labels;
//! - every edge version with an endpoint home among the departed vShards and
//!   none among the vShards this node still hosts goes.
//!
//! Memory-only engines checkpoint before the purge answers, so a restart
//! loads no purged row from an older checkpoint. Every step is idempotent:
//! a re-run after a partial purge removes what is left.

use std::collections::HashSet;
use std::sync::Arc;

use nodedb_physical::physical_plan::DepartedCollection;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantDataSnapshot, TenantId};

/// What one core's departure purge removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) struct DeparturePurged {
    pub documents: usize,
    pub edge_versions: usize,
    pub label_versions: usize,
}

impl CoreLoop {
    /// Run the departure purge of `departed` on this core and answer
    /// `{"purged": N}`, N the documents, edge versions and label versions
    /// removed.
    pub(in crate::data::executor) fn execute_purge_departed_vshards(
        &mut self,
        task: &ExecutionTask,
        departed: &[u32],
        resident: &[u32],
        collections: &[DepartedCollection],
    ) -> Response {
        let purged = match self.purge_departed_vshards(departed, resident, collections) {
            Ok(purged) => purged,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "departure purge of vShards {departed:?} on core {}: {e}",
                            self.core_id
                        ),
                    },
                );
            }
        };
        tracing::info!(
            core = self.core_id,
            ?departed,
            documents = purged.documents,
            edge_versions = purged.edge_versions,
            label_versions = purged.label_versions,
            "departure purge: removed this core's data of vShards this node left"
        );
        let total = purged.documents + purged.edge_versions + purged.label_versions;
        let result = serde_json::json!({ "purged": total });
        match crate::data::executor::response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("departure purge result serialization failed: {e}"),
                },
            ),
        }
    }

    /// Remove this core's data of `departed`. Edges with an endpoint home in
    /// `resident` stay.
    pub(in crate::data::executor) fn purge_departed_vshards(
        &mut self,
        departed: &[u32],
        resident: &[u32],
        collections: &[DepartedCollection],
    ) -> crate::Result<DeparturePurged> {
        let mut purged = DeparturePurged::default();
        if departed.is_empty() {
            return Ok(purged);
        }
        let departed_set: HashSet<u32> = departed.iter().copied().collect();
        let resident_set: HashSet<u32> = resident.iter().copied().collect();

        for target in collections {
            let database_id = DatabaseId::new(target.database_id);
            // Only the home core unlinks the shared on-disk files.
            let reclaim_l1_files = self.owns_collection(database_id, &target.collection)?;
            let stats = self.clear_collection_all_engines(
                database_id,
                TenantId::new(target.tenant_id),
                &target.collection,
                true,
                reclaim_l1_files,
                false,
            )?;
            purged.documents += stats.docs_removed;
        }

        self.install_group_arrays(departed, &[])?;

        // A node's labels home on one vShard, so the labels of a departed
        // vShard's nodes go whole. Every other label stays, and survives the
        // CSR rebuild below.
        let kept_labels = self.held_node_labels(Some(&departed_set));
        purged.label_versions = self.edge_store.purge_homed_node_labels(&departed_set)?;
        self.historical_csr.clear();
        purged.edge_versions = self
            .edge_store
            .purge_departed(&departed_set, &resident_set)?;
        let csr_rebuilt = purged.edge_versions > 0;
        if csr_rebuilt {
            self.csr = crate::engine::graph::csr::rebuild::rebuild_sharded_from_store(
                &self.edge_store,
                Arc::clone(&self.governor),
            )?;
        }
        self.install_snapshot_node_labels(
            &kept_labels,
            csr_rebuilt,
            Some(&departed_set),
            &TenantDataSnapshot::default(),
        )?;

        self.persist_snapshot_install()?;
        Ok(purged)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_bridge::buffer::RingBuffer;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::engine::graph::edge_store::temporal::{EdgeValuePayload, versioned_edge_key};
    use crate::types::VShardId;

    fn open_core(dir: &std::path::Path) -> CoreLoop {
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core loop")
    }

    /// Two node keys whose key vShards differ.
    fn two_homes() -> (String, String) {
        let a = "n0".to_string();
        let home = VShardId::from_key(a.as_bytes());
        let b = (1..4096)
            .map(|i| format!("n{i}"))
            .find(|k| VShardId::from_key(k.as_bytes()) != home)
            .expect("a second home in 4096 keys");
        (a, b)
    }

    fn edge_keys(core: &CoreLoop) -> Vec<String> {
        let mut keys: Vec<String> = core
            .edge_store
            .export_edges()
            .expect("export")
            .into_iter()
            .map(|(_, _, key, _)| key)
            .collect();
        keys.sort();
        keys
    }

    /// The purge takes the edges only departed vShards hold, keeps an edge a
    /// resident vShard holds, and a second run finds nothing left.
    #[test]
    fn a_departure_purge_keeps_what_a_resident_vshard_holds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        let (a, b) = two_homes();
        let value = EdgeValuePayload::new(0, i64::MAX, Vec::new())
            .encode()
            .expect("payload");
        let tid = TenantId::new(1);
        for (src, dst) in [(&a, &a), (&a, &b), (&b, &b)] {
            let key = versioned_edge_key("g", src, "L", dst, 1).expect("key");
            core.edge_store
                .put_edge_raw(0, tid, &key, &value)
                .expect("put");
        }
        let departed = [VShardId::from_key(a.as_bytes()).as_u32()];
        let resident = [VShardId::from_key(b.as_bytes()).as_u32()];

        let purged = core
            .purge_departed_vshards(&departed, &resident, &[])
            .expect("purge");
        assert_eq!(purged.edge_versions, 1);
        let mut kept = vec![
            versioned_edge_key("g", &a, "L", &b, 1).expect("key"),
            versioned_edge_key("g", &b, "L", &b, 1).expect("key"),
        ];
        kept.sort();
        assert_eq!(edge_keys(&core), kept);

        let again = core
            .purge_departed_vshards(&departed, &resident, &[])
            .expect("a second purge");
        assert_eq!(again, DeparturePurged::default(), "the purge is idempotent");
    }
}
