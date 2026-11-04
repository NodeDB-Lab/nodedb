// SPDX-License-Identifier: BUSL-1.1

//! Install of one restored vector row (`crate::wal::VectorRestoreRedo`), for
//! restart replay and the committed-redo apply alike.
//!
//! The record carries the exact nodes its resolve chose. The install first
//! ends the row's live nodes at `end_live_at`, then installs every ended
//! node with its span, then the live nodes at their start. So no two
//! versions of the row overlap, and a search at any cut reads one. The
//! install reads nothing to choose, so every replica and every replay writes
//! the same nodes. The spans fold into the shared clock, so a later write
//! stamps above every restored node.

use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::Surrogate;
use nodedb_vector::collection::EndedNodeExport;

use super::core_loop::CoreLoop;
use super::wal_replay_vector_redo::RedoVectorWrite;
use crate::bridge::envelope::{ErrorCode, PhysicalPlan};
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::types::{DatabaseId, TenantId};
use crate::wal::VectorRestoreRedo;

impl CoreLoop {
    /// Install a restored vector row. `None` when the payload is not one.
    /// `Some(true)` when the row installed, `Some(false)` when a gate
    /// skipped it or the install failed (reported through `replay_policy`).
    pub(in crate::data::executor) fn try_replay_vector_restore(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::DatabaseTombstones<'_>,
    ) -> Option<bool> {
        let redo = VectorRestoreRedo::decode(payload)?;
        if tombstones.is_tombstoned(tenant_id, &redo.collection, record_lsn) {
            return Some(false);
        }
        // A restored row names a bound row. One under `ZERO` is refused.
        if redo.surrogate == 0 {
            self.replay_record_unapplied(
                "vector",
                "restore_identity",
                record_lsn,
                &format!(
                    "restored vector row of '{}' carries no surrogate",
                    redo.collection
                ),
            );
            return Some(false);
        }
        let dim = redo.dim as usize;
        let mismatch = redo
            .ended
            .iter()
            .map(|node| node.vector.len())
            .chain(
                redo.live
                    .iter()
                    .flat_map(|live| live.vectors.iter().map(Vec::len)),
            )
            .find(|len| *len != dim);
        if let Some(len) = mismatch {
            self.replay_record_unapplied(
                "vector",
                "dim",
                record_lsn,
                &format!(
                    "restored row of '{}' declares dim {dim} but carries {len} components",
                    redo.collection
                ),
            );
            return Some(false);
        }
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, &redo.collection, &redo.field_name);
        let surrogate = Surrogate::new(redo.surrogate);
        if !self.redo_vector_prelude(
            RedoVectorWrite {
                index_key: &index_key,
                tid: tenant_id,
                collection: &redo.collection,
                dim,
                surrogates: &[surrogate],
                ids: &[],
                sidecars: false,
            },
            None,
            record_lsn,
        ) {
            return Some(false);
        }
        match self.install_restored_vector_row(database_id, tenant_id, record_lsn, &redo) {
            Ok(()) => Some(true),
            Err(error) => {
                self.replay_record_rejected(
                    "vector",
                    record_lsn,
                    Some(Box::new(error)),
                    &format!("restore of a vector row into '{}' failed", redo.collection),
                );
                Some(false)
            }
        }
    }

    /// End the row's live nodes, then install every node of `redo`.
    fn install_restored_vector_row(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        record_lsn: u64,
        redo: &VectorRestoreRedo,
    ) -> Result<(), ErrorCode> {
        let engine_error =
            |e: nodedb_vector::error::VectorError| ErrorCode::from(crate::Error::from(e));
        let collection = redo.collection.as_str();
        let dim = redo.dim as usize;
        let surrogate = Surrogate::new(redo.surrogate);
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, collection, &redo.field_name);
        for node in &redo.ended {
            self.observe_bitemporal_ordinal(node.sys_from);
            self.observe_bitemporal_ordinal(node.sys_to);
        }
        if let Some(live) = &redo.live {
            self.observe_bitemporal_ordinal(live.sys_from);
        }
        if let Some(at) = redo.end_live_at {
            self.observe_bitemporal_ordinal(at);
        }
        let created = !self.vector_collections.contains_key(&index_key);
        let defer_seal = self.recording_redo_undo();
        let coll = self.get_or_create_vector_index(
            database_id,
            tenant_id,
            collection,
            dim,
            &redo.field_name,
        )?;
        if let Some(at) = redo.end_live_at {
            if redo.multi {
                coll.end_multi_vector_at(surrogate, at);
            } else {
                coll.end_surrogate_at(surrogate, at);
            }
        }
        for node in &redo.ended {
            coll.install_ended_node(EndedNodeExport {
                vector: node.vector.clone(),
                surrogate,
                sys_from: node.sys_from,
                sys_to: node.sys_to,
            })
            .map_err(engine_error)?;
        }
        if let Some(live) = &redo.live {
            if redo.multi {
                let slices: Vec<&[f32]> = live.vectors.iter().map(Vec::as_slice).collect();
                coll.insert_multi_vector_at(&slices, surrogate, live.sys_from)
                    .map_err(engine_error)?;
            } else {
                for vector in &live.vectors {
                    coll.insert_at(vector.clone(), surrogate, live.sys_from)
                        .map_err(engine_error)?;
                }
            }
        }
        if created && defer_seal {
            self.record_redo_undo([UndoEntry::VectorCollectionCreated {
                index_key: index_key.clone(),
            }]);
        }
        if !defer_seal {
            self.settle_vector_collection(&index_key);
        }
        let written = redo.ended.len() + redo.live.as_ref().map_or(0, |live| live.vectors.len());
        self.checkpoint_coordinator
            .mark_dirty("vector", written.max(1));
        let database = DatabaseId::new(database_id);
        let tid = TenantId::new(tenant_id);
        let qualified =
            nodedb_types::QualifiedCollection::from_stored(database, collection.to_string())
                .map_err(|e| ErrorCode::from(crate::Error::from(e)))?;
        if let Some(vshard) = self.replay_vshard("vector", record_lsn, database, collection) {
            let task = Self::replay_task(
                tid,
                database,
                vshard,
                PhysicalPlan::Vector(VectorOp::Insert {
                    collection: qualified,
                    vector: Vec::new(),
                    dim,
                    field_name: redo.field_name.clone(),
                    surrogate,
                    pk_bytes: None,
                    provenance: None,
                }),
                Some(crate::types::Lsn::new(record_lsn)),
            );
            // The write's version, so a cross-shard read-set check sees it.
            self.note_surrogate_write(&task, tenant_id, collection, surrogate.as_u32())
                .map_err(ErrorCode::from)?;
        }
        Ok(())
    }
}
