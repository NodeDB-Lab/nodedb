// SPDX-License-Identifier: BUSL-1.1

//! One committed transaction's redo for one vShard, as a commit hands it to
//! the data-group log and as every replica applies it.

use nodedb_physical::physical_plan::{
    CalvinInstall, MetaOp, PhysicalPlan, RedoOrigin, RedoPins, RedoSumTargets,
};

use nodedb_types::calvin::VersionedReadEntry;

use crate::control::server::shared::write_admission::plan_adds_storage;
use crate::control::state::SharedState;
use crate::control::surrogate::{CarriedIdentity, collect_plan_identities};
use crate::control::wal_replication::types::CalvinRedoMeta;
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId};
use crate::wal::RedoRecord;

use super::pins::{PinScope, redo_pins};
use super::sum_targets::redo_sum_targets;
use crate::control::planner::calvin::incarnation_names::{NamedIncarnation, incarnation_names};
use nodedb_cluster::calvin::types::IncarnationKind;
use nodedb_physical::physical_plan::{pin_required_collections, written_collections};

/// Everything a replica needs to apply one committed transaction's redo.
#[derive(Debug, Clone)]
pub struct TransactionRedoPayload {
    /// The resolved post-images.
    pub redo: RedoRecord,
    /// Every collection the transaction wrote.
    pub collections: Vec<String>,
    /// Every user collection and array the transaction wrote, each with its
    /// kind. The redo entry names each one for its incarnation stamp, so an
    /// array never takes a same-name collection's identity. Empty on a
    /// payload decoded from an entry: the entry carries its stamps.
    pub incarnation_names: Vec<NamedIncarnation>,
    /// Materialized-sum resolution the document writes fold into, keyed by
    /// source collection.
    pub sum_targets: Vec<RedoSumTargets>,
    /// Identities every replica binds before the apply.
    pub identities: Vec<CarriedIdentity>,
    /// Event source every replica stamps on the writes.
    pub event_source: EventSource,
    /// Which commit-boundary checks every replica's apply runs.
    pub origin: RedoOrigin,
    /// Set when the redo is a committed Calvin slice. The record's
    /// `calvin_stamp` names its `(epoch, position)`.
    pub calvin: Option<CalvinRedoMeta>,
    /// The transaction's read set. Every replica validates the entries homed
    /// on the record's vShard against `snapshot_ordinal` before it installs.
    pub reads: Vec<VersionedReadEntry>,
    /// The snapshot every read observed. `None` validates nothing: an
    /// autocommit record, or a Calvin slice its stage vote validated.
    pub snapshot_ordinal: Option<i64>,
    /// Whether the transaction can add stored bytes. The proposer sets it from
    /// the buffered plans. A Calvin slice reads `false`, because the Calvin
    /// submit classified it. A payload decoded from an entry reads `false`,
    /// because only the leader gate reads the flag.
    pub adds_storage: bool,
    /// The config content the proposer pinned for every replica's validate.
    /// Empty on a stream marker's payload: the content header carries them.
    pub pins: RedoPins,
}

/// What a committed Calvin slice's redo carries beside its record, derived
/// from the slice's local plans when it stages.
#[derive(Debug, Clone)]
pub struct CalvinSlice {
    pub collections: Vec<String>,
    pub incarnation_names: Vec<NamedIncarnation>,
    pub sum_targets: Vec<RedoSumTargets>,
    pub identities: Vec<CarriedIdentity>,
    pub event_source: EventSource,
    pub origin: RedoOrigin,
    pub meta: CalvinRedoMeta,
}

impl TransactionRedoPayload {
    /// The payload for a commit whose buffered write plans are `plans` and
    /// whose resolved redo is `redo`, built on the node that resolved it.
    pub fn from_commit(
        state: &SharedState,
        database_id: DatabaseId,
        tenant_id: TenantId,
        redo: RedoRecord,
        plans: &[PhysicalPlan],
        event_source: EventSource,
    ) -> crate::Result<Self> {
        let collections = written_collections(plans);
        let sum_targets = redo_sum_targets(plans);
        let incarnation_names = incarnation_names(plans, |plan| {
            written_collections(std::slice::from_ref(plan))
        });
        // The written names that are user collections. An array has no
        // document config, so it takes no pin.
        let pinned_names = incarnation_names
            .iter()
            .filter(|(_, kind)| matches!(kind, IncarnationKind::Collection))
            .map(|(name, _)| name.as_str());
        let scope = PinScope {
            database_id,
            tenant_id,
        };
        let required = pin_required_collections(plans);
        let pins = redo_pins(state, scope, pinned_names, &required, &sum_targets)?;
        Ok(Self {
            redo,
            collections,
            incarnation_names,
            sum_targets,
            identities: collect_plan_identities(
                &state.surrogate_assigner,
                database_id,
                tenant_id,
                plans,
            )?,
            event_source,
            origin: RedoOrigin::Commit,
            calvin: None,
            reads: Vec::new(),
            snapshot_ordinal: None,
            adds_storage: plans.iter().any(plan_adds_storage),
            pins,
        })
    }

    /// The payload with the transaction's read set and the snapshot its
    /// reads observed, which every replica's apply validates.
    pub fn with_snapshot(mut self, reads: Vec<VersionedReadEntry>, snapshot_ordinal: i64) -> Self {
        self.reads = reads;
        self.snapshot_ordinal = Some(snapshot_ordinal);
        self
    }

    /// The payload of a committed Calvin slice whose resolved redo is `redo`,
    /// built on the vShard's data-group leader. The record's `calvin_stamp`
    /// must name the slice's `(epoch, position)`.
    pub fn from_calvin(redo: RedoRecord, slice: CalvinSlice) -> Self {
        let CalvinSlice {
            collections,
            incarnation_names,
            sum_targets,
            identities,
            event_source,
            origin,
            meta,
        } = slice;
        Self {
            redo,
            collections,
            incarnation_names,
            sum_targets,
            identities,
            event_source,
            origin,
            calvin: Some(meta),
            reads: Vec::new(),
            snapshot_ordinal: None,
            adds_storage: false,
            // A Calvin slice carries no pin: its submit pins nothing.
            pins: RedoPins::default(),
        }
    }

    /// The payload as a mirror of `mirror_database` applies it.
    ///
    /// Its Calvin position belongs to the source's sequencer, so it records
    /// under the mirror's own ledger. Its cross-shard applied key serves
    /// trigger dedup, and no trigger fires on a mirror.
    pub fn in_mirror(mut self, mirror_database: u64) -> Self {
        if let Some(stamp) = self.redo.calvin_stamp.as_mut() {
            stamp.mirror_database = Some(mirror_database);
        }
        self.redo.cross_shard_applied = None;
        self
    }

    /// The Data-Plane plan that applies this redo.
    pub fn apply_plan(&self) -> crate::Result<PhysicalPlan> {
        Ok(PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            redo: self.redo.to_bytes()?,
            collections: self.collections.clone(),
            sum_targets: self.sum_targets.clone(),
            origin: self.origin,
            calvin: self.calvin_install()?,
            reads: self.reads.clone(),
            snapshot_ordinal: self.snapshot_ordinal,
            pins: self.pins.clone(),
        }))
    }

    /// The Calvin install the apply plan carries: the slice's stamp and the
    /// meta beside it. A Calvin meta on a record with no stamp names no
    /// position, so it is refused.
    fn calvin_install(&self) -> crate::Result<Option<CalvinInstall>> {
        let Some(meta) = &self.calvin else {
            return Ok(None);
        };
        let stamp = self
            .redo
            .calvin_stamp
            .as_ref()
            .ok_or_else(|| crate::Error::Internal {
                detail: "a Calvin redo entry carries no calvin_stamp, so its install names no \
                         (epoch, position); the entry is refused"
                    .into(),
            })?;
        Ok(Some(CalvinInstall {
            epoch: stamp.epoch,
            position: stamp.position,
            epoch_system_ms: meta.epoch_system_ms,
            reply: meta.reply.clone(),
            user_write: meta.user_write,
            mirror: stamp.mirror_database.is_some(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::CalvinStamp;
    use nodedb_physical::physical_plan::CalvinReplySpec;

    fn payload(
        stamp: Option<CalvinStamp>,
        calvin: Option<CalvinRedoMeta>,
    ) -> TransactionRedoPayload {
        TransactionRedoPayload {
            redo: RedoRecord {
                version: 1,
                ops: Vec::new(),
                calvin_stamp: stamp,
                cross_shard_applied: None,
                row_sources: Vec::new(),
                publishes: Vec::new(),
                row_changes: Vec::new(),
                user_roles: Vec::new(),
                sum_targets: Vec::new(),
                pins: RedoPins::default(),
            },
            collections: vec!["orders".into()],
            incarnation_names: Vec::new(),
            sum_targets: Vec::new(),
            identities: Vec::new(),
            event_source: EventSource::User,
            origin: RedoOrigin::Commit,
            calvin,
            reads: Vec::new(),
            snapshot_ordinal: None,
            adds_storage: false,
            pins: RedoPins::default(),
        }
    }

    fn meta() -> CalvinRedoMeta {
        CalvinRedoMeta {
            epoch_system_ms: 1_700,
            reply: CalvinReplySpec::Count(vec![7]),
            primary_write: true,
            user_write: true,
            returning: false,
        }
    }

    fn install_of(payload: &TransactionRedoPayload) -> crate::Result<Option<CalvinInstall>> {
        match payload.apply_plan()? {
            PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { calvin, .. }) => Ok(calvin),
            other => panic!("apply plan must be ApplyTransactionRedo, got {other:?}"),
        }
    }

    #[test]
    fn a_session_redo_installs_without_a_calvin_install() {
        let plain = payload(None, None);
        assert_eq!(install_of(&plain).expect("plan"), None);
    }

    #[test]
    fn a_calvin_redo_installs_at_its_stamped_position() {
        let stamp = CalvinStamp {
            epoch: 9,
            position: 4,
            vshard_id: 2,
            mirror_database: None,
        };
        let calvin = payload(Some(stamp), Some(meta()));
        assert_eq!(
            install_of(&calvin).expect("plan"),
            Some(CalvinInstall {
                epoch: 9,
                position: 4,
                epoch_system_ms: 1_700,
                reply: CalvinReplySpec::Count(vec![7]),
                user_write: true,
                mirror: false,
            })
        );
    }

    /// A mirror's slice records under the mirror's ledger, installs as a
    /// mirror install, and carries no trigger dedup key.
    #[test]
    fn a_mirrored_redo_names_its_mirror_and_drops_the_dedup_key() {
        let stamp = CalvinStamp {
            epoch: 9,
            position: 4,
            vshard_id: 2,
            mirror_database: None,
        };
        let mut calvin = payload(Some(stamp), Some(meta()));
        calvin.redo.cross_shard_applied = Some(crate::wal::CrossShardAppliedKey {
            source_vshard: 1,
            source_lsn: 2,
            source_sequence: 3,
            origin: "source".into(),
        });
        let mirrored = calvin.in_mirror(1024);
        assert_eq!(
            mirrored
                .redo
                .calvin_stamp
                .as_ref()
                .map(|s| s.mirror_database),
            Some(Some(1024))
        );
        assert!(mirrored.redo.cross_shard_applied.is_none());
        let install = install_of(&mirrored).expect("plan").expect("install");
        assert!(install.mirror);
    }

    #[test]
    fn a_calvin_meta_without_a_stamp_is_refused() {
        let unstamped = payload(None, Some(meta()));
        assert!(install_of(&unstamped).is_err());
    }

    #[test]
    fn a_snapshot_payload_hands_its_reads_to_the_apply() {
        let read = VersionedReadEntry {
            engine: nodedb_types::calvin::EngineTag::Document,
            collection: "orders".into(),
            key: nodedb_types::calvin::ReadKeyIdent::Predicate,
            home_vshard: None,
        };
        let plain = payload(None, None).with_snapshot(vec![read.clone()], 77);
        match plain.apply_plan().expect("plan") {
            PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
                reads,
                snapshot_ordinal,
                ..
            }) => {
                assert_eq!(reads, vec![read]);
                assert_eq!(snapshot_ordinal, Some(77));
            }
            other => panic!("apply plan must be ApplyTransactionRedo, got {other:?}"),
        }
    }

    fn commit_state() -> (std::sync::Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("tempdir");
        let wal = std::sync::Arc::new(
            crate::wal::WalManager::open_for_testing(&directory.path().join("pins.wal"))
                .expect("wal"),
        );
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        (state, directory)
    }

    fn empty_redo() -> RedoRecord {
        payload(None, None).redo
    }

    fn commit_of(
        state: &SharedState,
        plans: &[PhysicalPlan],
    ) -> crate::Result<TransactionRedoPayload> {
        TransactionRedoPayload::from_commit(
            state,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            empty_redo(),
            plans,
            EventSource::User,
        )
    }

    fn delete_from(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::PointDelete {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: "d1".into(),
            surrogate: None,
            pk_bytes: b"d1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        })
    }

    /// A commit that writes a collection with no catalog row cannot pin
    /// it, so it is refused with `42P01`.
    #[test]
    fn a_commit_to_a_collection_with_no_row_is_refused() {
        let (state, _dir) = commit_state();
        let refused = commit_of(&state, &[delete_from("ghost")]).expect_err("refused");
        assert!(
            matches!(&refused, crate::Error::CollectionNotFound { collection, .. } if collection == "ghost"),
            "{refused:?}"
        );
    }

    /// An array has no document config. A commit that writes one with no
    /// collection row of its name pins it nothing.
    #[test]
    fn a_commit_to_an_array_pins_no_config() {
        let (state, _dir) = commit_state();
        let array_delete = PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Delete {
            array_id: nodedb_array::types::ArrayId::new(TenantId::new(1), "grid"),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 0,
        });
        let committed = commit_of(&state, &[array_delete]).expect("commit");
        assert!(committed.pins.pinned());
        assert!(committed.pins.configs.is_empty());
    }

    /// An edge write accepts a collection with no catalog row, so a commit
    /// that writes edges of one is not refused. Its collection takes no
    /// pin.
    #[test]
    fn a_commit_to_an_edge_collection_with_no_row_commits() {
        let (state, _dir) = commit_state();
        let edge_put = PhysicalPlan::Graph(nodedb_physical::physical_plan::GraphOp::EdgePut {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "social"),
            src_id: "alice".into(),
            label: "follows".into(),
            dst_id: "bob".into(),
            properties: Vec::new(),
            src_surrogate: nodedb_types::Surrogate::new(1),
            dst_surrogate: nodedb_types::Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
        });
        let committed = commit_of(&state, &[edge_put]).expect("commit");
        assert!(committed.pins.pinned());
        assert!(committed.pins.configs.is_empty());
    }
}
