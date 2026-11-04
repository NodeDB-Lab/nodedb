// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::QueryStorageBytes`: this core's stored bytes per database and
//! tenant.
//!
//! The source of each engine's stored bytes:
//!
//! - document, strict document and full-text: the sparse file's persisted
//!   counter, the key and value bytes of every stored row version;
//! - graph edges: the edge store file's persisted counter, counted the same way;
//! - key-value: each hash table's resident bytes, history included;
//! - vector, sparse vector and spatial: each index's resident bytes, live and
//!   history;
//! - CRDT: each collection document's estimated state bytes;
//! - columnar: the encoded bytes of the flushed segments, plus the row-version
//!   stamps;
//! - timeseries: the on-disk bytes of every partition not deleted;
//! - array: the encoded tile bytes of the open segments, per placement vShard.
//!
//! Memtables do not count. The WAL holds their rows until a flush writes them.
//!
//! A collection counts when its home vShard is in the led set and routes to
//! this core. An array tile counts the same way by its placement vShard.
//!
//! Edges live on their endpoint keys' vShards, not on the collection home. The
//! edge counter splits each collection by the edge's two endpoint homes. A core
//! stores one copy for the homes that route to it. That copy counts under the
//! lowest of those homes, when that home is led. Every core-local copy then
//! counts once, whichever node leads which home.
//!
//! Every source keeps its size as writes land, so a call reads counters and
//! never scans rows.

use std::collections::{HashMap, HashSet};

use nodedb_types::timeseries::PartitionState;
use nodedb_types::{CollectionKey, DatabaseId, TenantId};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::edge_store::home_bytes::EdgeHomes;
use crate::types::{VShardId, core_for_vshard};

/// One answer row: `(database_id, tenant_id, stored bytes)`.
pub type TenantBytesRow = (u64, u64, u64);

impl CoreLoop {
    /// `MetaOp::QueryStorageBytes`: answer this core's stored bytes per
    /// `(database, tenant)` against `led_vshards`.
    pub(in crate::data::executor) fn execute_query_storage_bytes(
        &self,
        task: &ExecutionTask,
        led_vshards: &[u32],
    ) -> Response {
        let rows = match self.led_storage_bytes(led_vshards) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };
        match response_codec::encode(&rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    /// This core's stored bytes per `(database, tenant)` of the collections
    /// `led_vshards` covers, in key order. A counter that does not read, or a
    /// stored name not in its database's stored form, fails the call.
    pub(in crate::data::executor) fn led_storage_bytes(
        &self,
        led_vshards: &[u32],
    ) -> crate::Result<Vec<TenantBytesRow>> {
        let mut tally = StorageTally {
            led: led_vshards.iter().copied().collect(),
            num_cores: self.redo_apply.num_cores,
            core_id: self.core_id,
            homes: HashMap::new(),
            totals: HashMap::new(),
        };
        if tally.led.is_empty() {
            return Ok(Vec::new());
        }

        for (database_id, tenant_id, collection, bytes) in self.sparse.storage_bytes_snapshot()? {
            tally.add_collection(database_id, tenant_id, &collection, bytes)?;
        }
        for row in self.edge_store.storage_bytes_snapshot()? {
            tally.add_edge_copy(row.database_id, row.tenant_id, row.homes, row.bytes);
        }
        for (database_id, tenant_id, collection, bytes) in self.kv_engine.collection_mem_usages() {
            tally.add_collection(
                DatabaseId::new(database_id),
                TenantId::new(tenant_id),
                collection,
                bytes,
            )?;
        }
        self.tally_indexes(&mut tally)?;
        self.tally_segments(&mut tally)?;
        for ((database_id, tenant_id), engine) in &self.crdt_engines {
            for (collection, bytes) in engine.collection_state_bytes() {
                tally.add_collection(*database_id, *tenant_id, collection, bytes)?;
            }
        }
        for (array, placements) in self.array_engine.placement_bytes_by_array() {
            for (placement, bytes) in placements {
                tally.add_placed(array.database_id, array.tenant_id, *placement, *bytes);
            }
        }
        Ok(tally.into_rows())
    }

    /// Vector, sparse-vector and spatial index bytes. A multi-vector index
    /// key `"{collection}:{field}"` counts under its collection.
    fn tally_indexes(&self, tally: &mut StorageTally) -> crate::Result<()> {
        for ((database_id, tenant_id, key), index) in &self.vector_collections {
            let collection = key.split_once(':').map_or(key.as_str(), |(head, _)| head);
            tally.add_collection(
                *database_id,
                *tenant_id,
                collection,
                index.resident_bytes() as u64,
            )?;
        }
        for ((database_id, tenant_id, collection, _), index) in &self.sparse_vector_indexes {
            tally.add_collection(
                *database_id,
                *tenant_id,
                collection,
                index.resident_bytes() as u64,
            )?;
        }
        for ((database_id, tenant_id, collection, _), rtree) in &self.spatial_indexes {
            tally.add_collection(
                *database_id,
                *tenant_id,
                collection,
                rtree.resident_bytes() as u64,
            )?;
        }
        Ok(())
    }

    /// Columnar segment and version-stamp bytes, and timeseries partition
    /// bytes.
    fn tally_segments(&self, tally: &mut StorageTally) -> crate::Result<()> {
        for ((database_id, tenant_id, collection), segments) in &self.columnar_flushed_segments {
            let bytes = segments.iter().fold(0u64, |sum, segment| {
                sum.saturating_add(segment.len() as u64)
            });
            tally.add_collection(*database_id, *tenant_id, collection, bytes)?;
        }
        for ((database_id, tenant_id, collection), engine) in &self.columnar_engines {
            tally.add_collection(
                *database_id,
                *tenant_id,
                collection,
                engine.version_memory_bytes() as u64,
            )?;
        }
        for ((database_id, tenant_id, collection), registry) in &self.ts_registries {
            let bytes = registry
                .iter()
                .filter(|(_, entry)| entry.meta.state != PartitionState::Deleted)
                .fold(0u64, |sum, (_, entry)| {
                    sum.saturating_add(entry.meta.size_bytes)
                });
            tally.add_collection(*database_id, *tenant_id, collection, bytes)?;
        }
        Ok(())
    }
}

/// The running sums of one call, and the home vShard of each collection seen.
struct StorageTally {
    /// Every vShard this node leads.
    led: HashSet<u32>,
    num_cores: usize,
    core_id: usize,
    /// Home vShard per database and stored collection name, computed once per
    /// call.
    homes: HashMap<DatabaseId, HashMap<String, u32>>,
    totals: HashMap<(u64, u64), u64>,
}

impl StorageTally {
    /// The home vShard of the stored collection name `stored` of `database_id`.
    fn home(&mut self, database_id: DatabaseId, stored: &str) -> crate::Result<u32> {
        let names = self.homes.entry(database_id).or_default();
        if let Some(home) = names.get(stored) {
            return Ok(*home);
        }
        let home = CollectionKey::from_qualified_str(database_id, stored)?
            .vshard()
            .as_u32();
        names.insert(stored.to_owned(), home);
        Ok(home)
    }

    /// Whether `vshard` is led by this node and routes to this core.
    fn counts_here(&self, vshard: u32) -> bool {
        self.led.contains(&vshard)
            && core_for_vshard(VShardId::new(vshard), self.num_cores) == self.core_id
    }

    fn add(&mut self, database_id: DatabaseId, tenant_id: TenantId, bytes: u64) {
        let total = self
            .totals
            .entry((database_id.as_u64(), tenant_id.as_u64()))
            .or_insert(0);
        *total = total.saturating_add(bytes);
    }

    /// Count a collection-homed engine's `bytes` of `stored`.
    fn add_collection(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        stored: &str,
        bytes: u64,
    ) -> crate::Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let home = self.home(database_id, stored)?;
        if self.counts_here(home) {
            self.add(database_id, tenant_id, bytes);
        }
        Ok(())
    }

    /// Count this core's copy of the edge rows homed on `homes`. The copy
    /// counts under the lowest home that routes to this core, when that home
    /// is led.
    fn add_edge_copy(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        homes: EdgeHomes,
        bytes: u64,
    ) {
        let on_core = homes
            .iter()
            .find(|&home| core_for_vshard(VShardId::new(home), self.num_cores) == self.core_id);
        if bytes > 0 && on_core.is_some_and(|home| self.led.contains(&home)) {
            self.add(database_id, tenant_id, bytes);
        }
    }

    /// Count array tile bytes stored under `placement`.
    fn add_placed(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        placement: u32,
        bytes: u64,
    ) {
        if bytes > 0 && self.counts_here(placement) {
            self.add(database_id, tenant_id, bytes);
        }
    }

    fn into_rows(self) -> Vec<TenantBytesRow> {
        let mut rows: Vec<TenantBytesRow> = self
            .totals
            .into_iter()
            .map(|((database_id, tenant_id), bytes)| (database_id, tenant_id, bytes))
            .collect();
        rows.sort_unstable();
        rows
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{
        make_core_with_dir, make_default_task, point_put, task_with_vshard,
    };
    use crate::data::executor::handlers::graph::EdgePutParams;
    use crate::engine::kv::KvPutParams;
    use crate::types::RecordHomes;

    const OTHER_DB: DatabaseId = DatabaseId::new(1024);

    fn home(database_id: DatabaseId, stored: &str) -> u32 {
        CollectionKey::from_qualified_str(database_id, stored)
            .unwrap()
            .vshard()
            .as_u32()
    }

    fn key_home(node: &str) -> u32 {
        VShardId::from_key(node.as_bytes()).as_u32()
    }

    /// Two node names whose key vShards differ.
    fn two_homes() -> (String, String) {
        let src = "n0".to_owned();
        let dst = (1..)
            .map(|i| format!("n{i}"))
            .find(|n| key_home(n) != key_home(&src))
            .unwrap();
        (src, dst)
    }

    fn put_edge(core: &mut CoreLoop, tid: u64, src: &str, dst: &str) {
        let edge = core.execute_edge_put(
            &task_with_vshard(RecordHomes::edge_owner(src)),
            EdgePutParams {
                tid,
                collection: "follows",
                src_id: src,
                label: "F",
                dst_id: dst,
                properties: &[],
                src_surrogate: Surrogate::new(7),
                dst_surrogate: Surrogate::new(8),
                valid_from_ms: None,
                valid_until_ms: None,
            },
        );
        assert_eq!(edge.status, Status::Ok);
    }

    fn edge_counter_bytes(core: &CoreLoop) -> u64 {
        core.edge_store
            .storage_bytes_snapshot()
            .unwrap()
            .iter()
            .map(|row| row.bytes)
            .sum()
    }

    fn every_vshard() -> Vec<u32> {
        (0..VShardId::COUNT).collect()
    }

    fn total(rows: &[TenantBytesRow], database_id: DatabaseId, tenant_id: u64) -> u64 {
        rows.iter()
            .find(|(d, t, _)| *d == database_id.as_u64() && *t == tenant_id)
            .map_or(0, |(_, _, bytes)| *bytes)
    }

    /// Writes into documents, edges, KV and columnar segments of two
    /// databases and two tenants. A led set without one collection's home
    /// drops exactly that collection's bytes.
    #[test]
    fn sums_per_tenant_and_skips_unled_collections() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());

        // Default database, tenant 1: a document and an edge.
        let put = point_put(&mut core, &make_default_task(), "users", "u1", 1, "alice");
        assert_eq!(put.status, Status::Ok);
        let edge = core.execute_edge_put(
            &task_with_vshard(RecordHomes::edge_owner("a")),
            EdgePutParams {
                tid: 1,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                properties: &[],
                src_surrogate: Surrogate::new(2),
                dst_surrogate: Surrogate::new(3),
                valid_from_ms: None,
                valid_until_ms: None,
            },
        );
        assert_eq!(edge.status, Status::Ok);

        // Another database, tenant 2: a KV row and a flushed columnar segment.
        core.kv_engine
            .put(KvPutParams {
                database_id: OTHER_DB.as_u64(),
                tenant_id: 2,
                collection: "1024/cache",
                key: b"k1",
                value: b"value-bytes",
                ttl_ms: 0,
                now_ms: 1,
                surrogate: Surrogate::new(4),
            })
            .unwrap();
        core.columnar_flushed_segments.insert(
            (OTHER_DB, TenantId::new(2), "1024/metrics".into()),
            vec![vec![0u8; 300]],
        );

        // Default database, tenant 1: a segment whose home the led set omits.
        core.columnar_flushed_segments.insert(
            (DatabaseId::DEFAULT, TenantId::new(1), "hidden".into()),
            vec![vec![0u8; 500]],
        );
        let hidden = home(DatabaseId::DEFAULT, "hidden");
        for (database_id, stored) in [
            (DatabaseId::DEFAULT, "users"),
            (OTHER_DB, "1024/cache"),
            (OTHER_DB, "1024/metrics"),
        ] {
            assert_ne!(home(database_id, stored), hidden, "{stored} shares a home");
        }
        for node in ["a", "b"] {
            assert_ne!(key_home(node), hidden, "edge endpoint {node} shares a home");
        }

        let all = core.led_storage_bytes(&every_vshard()).unwrap();
        let led: Vec<u32> = every_vshard()
            .into_iter()
            .filter(|v| *v != hidden)
            .collect();
        let partial = core.led_storage_bytes(&led).unwrap();

        let default_all = total(&all, DatabaseId::DEFAULT, 1);
        let default_led = total(&partial, DatabaseId::DEFAULT, 1);
        assert_eq!(default_all - default_led, 500, "{all:?} vs {partial:?}");
        assert!(default_led > 0, "documents and edges count: {partial:?}");

        let kv = core
            .kv_engine
            .collection_mem_usage(OTHER_DB.as_u64(), 2, "1024/cache");
        assert!(kv > 0);
        assert_eq!(total(&partial, OTHER_DB, 2), kv + 300);
        assert_eq!(total(&all, OTHER_DB, 2), kv + 300);
        assert_eq!(
            partial.len(),
            2,
            "one row per database and tenant: {partial:?}"
        );

        assert!(core.led_storage_bytes(&[]).unwrap().is_empty());
    }

    /// With several cores, a collection counts only on the core its home
    /// routes to.
    #[test]
    fn a_collection_counts_only_on_its_home_core() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        let num_cores = 4;
        core.set_num_cores(num_cores);
        let names = ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"];
        for name in names {
            core.columnar_flushed_segments.insert(
                (DatabaseId::DEFAULT, TenantId::new(1), name.into()),
                vec![vec![0u8; 10]],
            );
        }
        let here = names
            .iter()
            .filter(|name| {
                core_for_vshard(VShardId::new(home(DatabaseId::DEFAULT, name)), num_cores) == 0
            })
            .count() as u64;
        let rows = core.led_storage_bytes(&every_vshard()).unwrap();
        assert_eq!(total(&rows, DatabaseId::DEFAULT, 1), here * 10);
    }

    /// The handler answers the rows as one msgpack array.
    #[test]
    fn the_answer_decodes_as_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        core.columnar_flushed_segments.insert(
            (DatabaseId::DEFAULT, TenantId::new(3), "events".into()),
            vec![vec![0u8; 42]],
        );
        let response = core.execute_query_storage_bytes(&make_default_task(), &every_vshard());
        assert_eq!(response.status, Status::Ok);
        let rows: Vec<TenantBytesRow> = zerompk::from_msgpack(response.payload.as_bytes()).unwrap();
        assert_eq!(rows, vec![(0, 3, 42)]);
    }

    /// One core counts its copy of an edge once, under the lowest of the
    /// edge's homes that route to it, and only when that home is led.
    #[test]
    fn an_edge_copy_counts_once_under_its_led_home() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        let (src, dst) = two_homes();
        put_edge(&mut core, 5, &src, &dst);
        let counted = edge_counter_bytes(&core);
        assert!(counted > 0);
        let (low, high) = {
            let (a, b) = (key_home(&src), key_home(&dst));
            (a.min(b), a.max(b))
        };

        // One core holds both homes: the copy counts once, under the low home.
        let rows = core.led_storage_bytes(&every_vshard()).unwrap();
        assert_eq!(total(&rows, DatabaseId::DEFAULT, 5), counted);
        let without_low: Vec<u32> = every_vshard().into_iter().filter(|v| *v != low).collect();
        let rows = core.led_storage_bytes(&without_low).unwrap();
        assert_eq!(
            total(&rows, DatabaseId::DEFAULT, 5),
            0,
            "the low home is not led"
        );
        let rows = core.led_storage_bytes(&[low]).unwrap();
        assert_eq!(total(&rows, DatabaseId::DEFAULT, 5), counted);
        let rows = core.led_storage_bytes(&[high]).unwrap();
        assert_eq!(
            total(&rows, DatabaseId::DEFAULT, 5),
            0,
            "the high home does not count"
        );
    }

    /// With the homes on two cores, each core counts its own copy under the
    /// home that routes to it.
    #[test]
    fn an_edge_homed_on_two_cores_counts_on_each_by_its_home() {
        let num_cores = 2;
        let (src, dst) = (0..)
            .map(|i| (format!("s{i}"), format!("d{i}")))
            .find(|(s, d)| key_home(s) % 2 != key_home(d) % 2)
            .unwrap();
        let mut shares = Vec::new();
        for core_id in 0..num_cores {
            let dir = tempfile::tempdir().unwrap();
            let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
            core.set_num_cores(num_cores);
            core.core_id = core_id;
            put_edge(&mut core, 6, &src, &dst);
            let counted = edge_counter_bytes(&core);
            let here = [key_home(&src), key_home(&dst)]
                .into_iter()
                .find(|h| core_for_vshard(VShardId::new(*h), num_cores) == core_id)
                .unwrap();
            let rows = core.led_storage_bytes(&[here]).unwrap();
            assert_eq!(
                total(&rows, DatabaseId::DEFAULT, 6),
                counted,
                "core {core_id}"
            );
            let others: Vec<u32> = every_vshard().into_iter().filter(|v| *v != here).collect();
            let rows = core.led_storage_bytes(&others).unwrap();
            assert_eq!(total(&rows, DatabaseId::DEFAULT, 6), 0, "core {core_id}");
            shares.push(counted);
        }
        assert!(shares.iter().all(|bytes| *bytes > 0));
    }
}
