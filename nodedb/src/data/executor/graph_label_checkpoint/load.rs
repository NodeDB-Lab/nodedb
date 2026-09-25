// SPDX-License-Identifier: BUSL-1.1

//! The CSR graph node-label checkpoint load path: decode the published state
//! file whole and install every partition's labels before WAL replay applies
//! the records above it.

use tracing::{error, info, warn};

use super::format::{GRAPH_LABEL_CKPT_FORMAT_VERSION, GraphLabelCheckpointFile};
use super::paths::{graph_label_ckpt_dir, graph_label_ckpt_state_path};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Load the CSR node labels from disk on startup, BEFORE WAL replay.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/graph-label-ckpt/core-{core_id}/`) — a core owns only the
    /// partitions routed to its vShards.
    ///
    /// Must run after `CoreLoop::open`, which rebuilds the CSR's nodes and edges
    /// from the redb `EdgeStore`. It does not depend on that rebuild for
    /// correctness — `add_node_label` vivifies an unknown node exactly as the
    /// live handler and the WAL replay do, so a label on a never-edged node
    /// restores whether or not the rebuild produced the node — but running after
    /// it keeps the restore from interning nodes the rebuild would then re-add.
    ///
    /// Replay then applies the `GraphNodeLabelSet` / `GraphNodeLabelRemove`
    /// records the WAL still holds on top of this state, in LSN order. No floor
    /// gates them: both records are absolute bit operations keyed by
    /// `(node, label)`, so a record already folded into this state re-applies to
    /// the same bit.
    pub fn load_graph_label_checkpoint(&mut self) -> crate::Result<()> {
        let ckpt_dir = graph_label_ckpt_dir(&self.data_dir, self.core_id);
        let path = graph_label_ckpt_state_path(&ckpt_dir);
        if !path.exists() {
            return Ok(());
        }

        // A present-but-corrupt checkpoint is fail-stop, not skip-and-replay:
        // the WAL below this generation's durable LSN may already be gone, so
        // silently restoring nothing here would boot with labels this build
        // can never recover.
        let bytes = nodedb_wal::segment::read_checkpoint_framed(&path).map_err(|source| {
            CheckpointDecodeError::ReadFile {
                path: path.clone(),
                source,
            }
        })?;
        let file = zerompk::from_msgpack::<GraphLabelCheckpointFile>(&bytes).map_err(|source| {
            CheckpointDecodeError::MsgpackDecode {
                path: path.clone(),
                source,
            }
        })?;
        if file.format_version != GRAPH_LABEL_CKPT_FORMAT_VERSION {
            return Err(CheckpointDecodeError::FormatVersion {
                path: path.clone(),
                found: file.format_version,
                expected: GRAPH_LABEL_CKPT_FORMAT_VERSION,
            }
            .into());
        }

        let partitions = file.partitions.len();
        let mut restored = 0usize;
        let mut failed = 0usize;
        // Read out before the loop: `csr_partition_mut` holds a mutable borrow
        // of `self` for as long as the partition is being installed.
        let core_id = self.core_id;
        for partition in file.partitions {
            let csr = self.csr_partition_mut(partition.database_id, partition.tenant_id);
            for (node, labels) in &partition.nodes {
                for label in labels {
                    // `add_node_label` is the same call the live `SetNodeLabels`
                    // handler and the WAL replay make, vivifying the node when
                    // it has no edges. Its `Ok(false)` (the 64-distinct-label
                    // bitset limit) is discarded here for the same reason they
                    // discard it: the limit was already enforced when the label
                    // was first set, so an export can never carry more than 64
                    // distinct labels per partition to begin with.
                    if let Err(e) = csr.add_node_label(node, label) {
                        warn!(
                            core = core_id,
                            %node,
                            %label,
                            error = %e,
                            "graph node-label checkpoint restore: could not set label"
                        );
                        failed += 1;
                        continue;
                    }
                    restored += 1;
                }
            }
        }

        // The durable LSN is what a LATER failed flush clamps truncation to, so
        // it may only be claimed over a state that is fully back. A partial
        // restore keeps it at zero: the labels that did install are still
        // installed, but this core stops authorising any truncation until it has
        // written a checkpoint of its own that succeeded end to end.
        // A partial install is NOT decode corruption: the file itself decoded
        // whole, and `add_node_label`'s per-call failure never claims the
        // durable LSN, so WAL replay above it still covers every label this
        // loop could not set. This stays a non-fatal, logged skip — only the
        // decode swallows above are fail-stop.
        if failed > 0 {
            error!(
                core = core_id,
                failed,
                restored,
                durable_through_lsn = file.durable_through_lsn,
                "graph node-label checkpoint restored only in part; NOT claiming its \
                 durable LSN, so WAL truncation stays pinned until a flush succeeds"
            );
            return Ok(());
        }
        self.floors.graph_label_durable_lsn = Lsn::new(file.durable_through_lsn);

        info!(
            core = core_id,
            partitions,
            labels = restored,
            durable_through_lsn = file.durable_through_lsn,
            "graph node-label checkpoint restored"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_types::{DatabaseId, OrdinalClock, TenantId};
    use tempfile::TempDir;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::engine::graph::edge_store::EdgeRef;
    use crate::engine::graph::pattern::compiler;
    use crate::engine::graph::pattern::executor::{
        MatchExecCtx, PropertyLookup, VarLenCaps, execute,
    };

    const TID: u64 = 7;
    const COLL: &str = "social";

    /// A core rooted at `dir`, so two cores in one test share a data dir the way
    /// a restart does: the second opens the same redb edge store (and so rebuilds
    /// the same edges) and reads exactly what the first wrote.
    fn open_core_at(dir: &std::path::Path) -> CoreLoop {
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx); // no requests are dispatched in these tests
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            Arc::new(OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open")
    }

    /// Commit an edge to the redb `EdgeStore` — the durable path a real
    /// `EdgePut` takes. `CoreLoop::open` rebuilds the CSR from exactly this.
    fn commit_edge(core: &CoreLoop, src: &str, label: &str, dst: &str) {
        core.edge_store
            .put_edge_versioned(
                EdgeRef::new(
                    DatabaseId::DEFAULT,
                    TenantId::new(TID),
                    COLL,
                    src,
                    label,
                    dst,
                ),
                &[],
                1,
                0,
                i64::MAX,
            )
            .expect("commit edge to the durable edge store");
    }

    /// Run a real MATCH query against the core's own CSR partition and edge
    /// store, and return the `a` bindings. This is the same executor entry point
    /// the graph dispatch handler uses, so it exercises the label bitset through
    /// the code path that actually consumes it (`node_has_label`).
    fn match_sources(core: &CoreLoop, query: &str) -> Vec<String> {
        let csr = core
            .csr_partition(DatabaseId::DEFAULT.as_u64(), TID)
            .expect("partition must exist");
        let props = PropertyLookup {
            sparse: &core.sparse,
            csr,
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: TID,
            collection: Some(COLL),
        };
        let parsed = compiler::parse(query).expect("parse MATCH");
        let mut sources: Vec<String> = execute(
            &parsed,
            MatchExecCtx {
                csr,
                edge_store: &core.edge_store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .expect("execute MATCH")
        .rows
        .into_iter()
        .filter_map(|row| row.get("a").cloned())
        .collect();
        sources.sort();
        sources
    }

    /// The whole point of this checkpoint: after a restart whose WAL no longer
    /// carries the `GraphNodeLabelSet` records, a label-scoped MATCH must still
    /// match exactly the nodes that were labeled.
    ///
    /// Drives the real write path, then the real load path on a SECOND core over
    /// the same data dir. The second core rebuilds its edges from redb and its
    /// labels start EMPTY, exactly as they do after truncation — so only the
    /// restore can make these assertions hold.
    #[test]
    fn restored_labels_still_answer_a_label_scoped_match() {
        let dir = TempDir::new().expect("tempdir");

        let mut before = open_core_at(dir.path());
        commit_edge(&before, "alice", "KNOWS", "bob");
        commit_edge(&before, "carol", "KNOWS", "bob");
        {
            let csr = before.csr_partition_mut(DatabaseId::DEFAULT.as_u64(), TID);
            // Rebuilt into the CSR from the durable edge store on the next open;
            // seeded here only so this core's own view matches.
            csr.add_edge_in_collection("alice", "KNOWS", "bob", COLL)
                .expect("seed edge");
            csr.add_edge_in_collection("carol", "KNOWS", "bob", COLL)
                .expect("seed edge");
            csr.add_node_label("alice", "Person").expect("label alice");
            csr.add_node_label("carol", "Bot").expect("label carol");
        }
        before.advance_watermark(Lsn::new(900));
        before
            .floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(900));

        let reported = before
            .checkpoint_graph_labels()
            .expect("flush to a writable dir must succeed");
        assert_eq!(
            reported,
            Lsn::new(900),
            "the flush must report exactly the LSN it made durable — the manager \
             deletes WAL segments below whatever this returns"
        );

        // Released before the next core opens: a core owns its data dir's redb
        // exclusively, so a restart is modelled by dropping this one first.
        drop(before);

        // The restart WITHOUT the restore, first — proving the assertions below
        // are load-bearing and not merely true of any reopened core.
        let unrestored = open_core_at(dir.path());
        assert_eq!(
            match_sources(&unrestored, "MATCH (a)-[:KNOWS]->(b) RETURN a, b"),
            vec!["alice".to_string(), "carol".to_string()],
            "the edges must come back from the redb edge store with no checkpoint \
             at all — they are why this checkpoint persists labels ONLY"
        );
        assert!(
            match_sources(&unrestored, "MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b").is_empty(),
            "before the restore the label is GONE while the edge survives — this is \
             the silent loss the checkpoint exists to prevent"
        );
        drop(unrestored);

        let mut after = open_core_at(dir.path());
        after
            .load_graph_label_checkpoint()
            .expect("valid checkpoint must load");

        assert_eq!(
            match_sources(&after, "MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b"),
            vec!["alice".to_string()],
            "the restored label must scope the MATCH to exactly the labeled node"
        );
        assert_eq!(
            match_sources(&after, "MATCH (a:Bot)-[:KNOWS]->(b) RETURN a, b"),
            vec!["carol".to_string()],
            "every label must restore, not just the first"
        );
        assert_eq!(
            match_sources(&after, "MATCH (a)-[:KNOWS]->(b) RETURN a, b"),
            vec!["alice".to_string(), "carol".to_string()],
            "restoring labels must not duplicate the rebuilt edges — an unlabeled \
             MATCH must still return one row per edge"
        );
        assert_eq!(
            after.floors.graph_label_durable_lsn,
            Lsn::new(900),
            "the restored durable LSN is what a failed flush clamps to; losing it \
             would pin truncation at zero for the rest of the process"
        );
    }

    /// A label on a node with NO edges has no redb-backed trace whatsoever: the
    /// edge-store rebuild cannot even produce the node. The restore must vivify
    /// it, exactly as the live handler and the WAL replay do.
    #[test]
    fn restored_label_on_an_edgeless_node_survives() {
        let dir = TempDir::new().expect("tempdir");

        let mut before = open_core_at(dir.path());
        before
            .csr_partition_mut(DatabaseId::DEFAULT.as_u64(), TID)
            .add_node_label("ghost", "Person")
            .expect("label ghost");
        before.checkpoint_graph_labels().expect("flush");
        drop(before);

        let mut after = open_core_at(dir.path());
        after
            .load_graph_label_checkpoint()
            .expect("valid checkpoint must load");

        let csr = after
            .csr_partition(DatabaseId::DEFAULT.as_u64(), TID)
            .expect("the restore must create the partition");
        let id = csr.node_id("ghost").expect("the node must be vivified");
        assert!(
            csr.node_has_label(id.raw(csr.partition_tag()), "Person"),
            "a label whose node never had an edge is exactly the state no rebuild \
             can reproduce — it must come back from the checkpoint"
        );
    }

    /// Labels are per-`(database, tenant)` partition. A restore that collapsed
    /// them onto one partition would label another tenant's identically-named
    /// node — a cross-tenant leak, not just a wrong row.
    #[test]
    fn restored_labels_stay_in_their_own_partition() {
        let dir = TempDir::new().expect("tempdir");

        let mut before = open_core_at(dir.path());
        before
            .csr_partition_mut(DatabaseId::DEFAULT.as_u64(), 1)
            .add_node_label("alice", "Person")
            .expect("label tenant 1's alice");
        before
            .csr_partition_mut(DatabaseId::DEFAULT.as_u64(), 2)
            .add_node_label("alice", "Bot")
            .expect("label tenant 2's alice");
        before.checkpoint_graph_labels().expect("flush");
        drop(before);

        let mut after = open_core_at(dir.path());
        after
            .load_graph_label_checkpoint()
            .expect("valid checkpoint must load");

        for (tid, want, unwanted) in [(1u64, "Person", "Bot"), (2, "Bot", "Person")] {
            let csr = after
                .csr_partition(DatabaseId::DEFAULT.as_u64(), tid)
                .expect("partition must exist");
            let id = csr.node_id("alice").expect("alice must exist");
            let raw = id.raw(csr.partition_tag());
            assert!(
                csr.node_has_label(raw, want),
                "tenant {tid} must keep {want}"
            );
            assert!(
                !csr.node_has_label(raw, unwanted),
                "tenant {tid} must not inherit the other tenant's label"
            );
        }
    }

    /// The export is keyed by node NAME because local ids are assigned by CSR
    /// build order. Here the restoring core's node ids differ from the writer's
    /// (its edge-store rebuild interns a different node first), so an id-keyed
    /// restore would attach the label to the wrong node.
    #[test]
    fn restore_follows_node_names_not_local_ids() {
        let dir = TempDir::new().expect("tempdir");

        let mut before = open_core_at(dir.path());
        commit_edge(&before, "zed", "KNOWS", "amy");
        {
            let csr = before.csr_partition_mut(DatabaseId::DEFAULT.as_u64(), TID);
            // Intern `amy` FIRST here, so `amy` takes local id 0 in the writer.
            csr.add_edge_in_collection("amy", "KNOWS", "zed", COLL)
                .expect("seed edge");
            csr.add_edge_in_collection("zed", "KNOWS", "amy", COLL)
                .expect("seed edge");
            csr.add_node_label("zed", "Person").expect("label zed");
        }
        let writer_zed_id = {
            let csr = before
                .csr_partition(DatabaseId::DEFAULT.as_u64(), TID)
                .expect("partition");
            csr.node_id("zed").expect("zed").raw(csr.partition_tag())
        };
        before.checkpoint_graph_labels().expect("flush");
        drop(before);

        let mut after = open_core_at(dir.path());
        after
            .load_graph_label_checkpoint()
            .expect("valid checkpoint must load");

        let csr = after
            .csr_partition(DatabaseId::DEFAULT.as_u64(), TID)
            .expect("partition");
        let tag = csr.partition_tag();
        let zed = csr.node_id("zed").expect("zed").raw(tag);
        let amy = csr.node_id("amy").expect("amy").raw(tag);
        assert_ne!(
            writer_zed_id, zed,
            "this test only proves anything while the two cores disagree on zed's \
             local id — the edge-store rebuild interns `zed` first"
        );
        assert!(
            csr.node_has_label(zed, "Person"),
            "the label must follow the name"
        );
        assert!(
            !csr.node_has_label(amy, "Person"),
            "an id-keyed restore would have landed the label here instead"
        );
    }

    /// An absent checkpoint must leave the labels untouched and claim nothing, so
    /// a first boot falls back to a full WAL replay rather than a zeroed LSN that
    /// looks like a real durability claim.
    #[test]
    fn absent_checkpoint_restores_nothing() {
        let dir = TempDir::new().expect("tempdir");
        let mut core = open_core_at(dir.path());
        core.load_graph_label_checkpoint()
            .expect("an absent checkpoint is a legitimate no-op, not an error");
        assert_eq!(core.csr.partition_count(), 0);
        assert_eq!(core.floors.graph_label_durable_lsn, Lsn::ZERO);
    }

    /// A file from a future format must be refused, not misparsed: labels the
    /// user never set are not correctable by any later record. It must now
    /// fail-stop the boot rather than silently restore nothing, because the
    /// WAL below this generation's durable LSN may already be gone.
    #[test]
    fn unknown_version_is_fail_stop() {
        use super::super::format::GraphLabelPartition;

        let dir = TempDir::new().expect("tempdir");
        let ckpt_dir = graph_label_ckpt_dir(dir.path(), 0);
        std::fs::create_dir_all(&ckpt_dir).expect("mkdir");
        let file = GraphLabelCheckpointFile {
            format_version: GRAPH_LABEL_CKPT_FORMAT_VERSION + 1,
            durable_through_lsn: 5,
            partitions: vec![GraphLabelPartition {
                database_id: 0,
                tenant_id: TID,
                nodes: vec![("alice".to_string(), vec!["Person".to_string()])],
            }],
        };
        let bytes = zerompk::to_msgpack_vec(&file).expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, "STATE", &bytes).expect("write");

        let mut core = open_core_at(dir.path());
        assert!(
            core.load_graph_label_checkpoint().is_err(),
            "a file this build cannot read must abort the load, not restore nothing"
        );
    }

    /// A corrupt (non-MessagePack) checkpoint body must also fail-stop the
    /// boot — the file exists and the frame-level checksum passes, but the
    /// payload does not decode.
    #[test]
    fn corrupt_msgpack_body_is_fail_stop() {
        let dir = TempDir::new().expect("tempdir");
        let ckpt_dir = graph_label_ckpt_dir(dir.path(), 0);
        std::fs::create_dir_all(&ckpt_dir).expect("mkdir");
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, "STATE", b"not valid msgpack")
            .expect("write");

        let mut core = open_core_at(dir.path());
        assert!(
            core.load_graph_label_checkpoint().is_err(),
            "an undecodable checkpoint body must abort the load, not restore nothing"
        );
    }

    // NOTE: no test exercises the `failed > 0` partial-install branch
    // (`add_node_label` returning `Err`). That path is reached only via
    // `GraphError::NodeOverflow`, which requires ~4.3 billion distinct nodes
    // in one partition (see `node_overflow_guard_fires_on_fresh_node` in
    // `nodedb-graph/src/csr/index/tests.rs`, which documents the same
    // infeasibility and settles for a code-review-verified guard instead of a
    // runtime one). No such test existed before this change either, so the
    // fail-stop conversion above does not remove any prior coverage of that
    // branch. The 64-distinct-label bitset cap that the loader's comment
    // mentions returns `Ok(false)` from `add_node_label`, not `Err` — it is
    // silently discarded rather than counted in `failed`, and so does not
    // exercise this branch at all.
}
