// SPDX-License-Identifier: BUSL-1.1

//! The judged view on the core: opened for a record's validate, read by
//! every validate check, and dropped when the record's apply ends.
//!
//! One rule picks the config a read takes while the view is open:
//!
//! - A body-shape read takes the install config, in the validate and the
//!   install alike: the freeze, its witness, the encode, the chain link,
//!   the body's CRS set, the bitemporal flag, the KV declared columns and
//!   the layout a folded target row encodes under. So the validate judges
//!   the row image the install stores.
//! - A rule or enforcement read takes the validate view during the
//!   validate, and the current config during the install.
//!
//! The view lives in the apply scope, which a parked install takes out of
//! the core whole. So it survives every yield, and a task between slices
//! reads the core's current configs.
//!
//! A plain replicated write that carries its proposer's pins opens the view
//! for its whole apply, with its rule reads on the validate view. Such a
//! write never parks, so the view drops when its task ends.
//!
//! A Calvin stage and its resolve carry their slice's pins and open the
//! view the same way. So a replica stages the rows and verdicts its
//! proposer planned, whatever configs it holds.

use nodedb_physical::physical_plan::{
    DeclaredColumn, DocumentPinNeeds, MetaOp, PhysicalPlan, PinnedWrite, RedoPins,
    document_pin_needs,
};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::doc_config_key::DocConfigKey;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::CollectionConfig;
use crate::types::{DatabaseId, TenantId};

use super::config_pins::{ConfigPinError, check_written_pinned};
use super::judged_configs::JudgedConfigs;

/// Refuse `task`, a plain replicated write pinned with `pins`, when it
/// writes or folds into a collection `pins` hold no pin of. Only a document
/// write requires pins. Every core refuses a malformed task alike.
///
/// A Calvin stage is judged by [`check_stage_pinned`] over its plans.
pub(in crate::data::executor) fn check_task_pinned(
    task: &ExecutionTask,
    pins: &RedoPins,
) -> Result<(), ConfigPinError> {
    if let PhysicalPlan::Meta(
        MetaOp::CalvinExecuteStatic { plans, .. } | MetaOp::CalvinExecuteActive { plans, .. },
    ) = task.plan()
    {
        return check_stage_pinned(task, pins, plans);
    }
    let needs = if let PhysicalPlan::Document(op) = task.plan() {
        document_pin_needs(op)
    } else {
        DocumentPinNeeds::default()
    };
    check_written_pinned(
        pins,
        task.request.database_id,
        task.request.tenant_id,
        needs.writes,
        &needs.sum_targets,
    )
}

/// Refuse `task`, a Calvin stage pinned with `pins`, when one of `plans`
/// writes or folds into a collection `pins` hold no pin of. A document
/// write requires the pins its install requires. A KV write requires its
/// collection's pin, as the committed redo's validate does. Every replica
/// refuses such a stage alike, before it stages anything.
fn check_stage_pinned(
    task: &ExecutionTask,
    pins: &RedoPins,
    plans: &[PhysicalPlan],
) -> Result<(), ConfigPinError> {
    let mut writes: Vec<(PinnedWrite, &str)> = Vec::new();
    let mut sum_targets = Vec::new();
    for plan in plans {
        if let PhysicalPlan::Document(op) = plan {
            let needs = document_pin_needs(op);
            writes.extend(needs.writes);
            sum_targets.extend(needs.sum_targets);
        } else if let PhysicalPlan::Kv(op) = plan
            && let Some(collection) = op.collection()
        {
            writes.push((PinnedWrite::KvRows, collection));
        }
    }
    check_written_pinned(
        pins,
        task.request.database_id,
        task.request.tenant_id,
        writes,
        &sum_targets,
    )
}

/// Whether `a` and `b`, the pins of two plain writes, are one pin set: the
/// same configs and the same CRS set, compared by key and digest. Two tasks
/// with no pins are one set. One batch applies under one view, so only
/// tasks of one pin set share a batch.
pub(in crate::data::executor) fn same_pin_set(a: Option<&RedoPins>, b: Option<&RedoPins>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            let same_crs = match (&a.crs, &b.crs) {
                (None, None) => true,
                (Some(a), Some(b)) => a.digest == b.digest,
                (None, Some(_)) | (Some(_), None) => false,
            };
            same_crs
                && a.configs.len() == b.configs.len()
                && a.configs
                    .iter()
                    .zip(&b.configs)
                    .all(|(a, b)| a.key == b.key && a.digest == b.digest)
        }
        (None, Some(_)) | (Some(_), None) => false,
    }
}

/// The judged view a record's apply set aside for an inner one, and puts
/// back when it ends.
pub(in crate::data::executor) struct HeldJudging {
    judged: Option<JudgedConfigs>,
    judging: bool,
}

impl CoreLoop {
    /// Open the judged view of a record pinned with `pins` and start its
    /// validate under it. A record with no pins opens no view, and its
    /// validate reads the current configs. `Err` when the CRS pin does not
    /// decode.
    pub(in crate::data::executor) fn begin_judging(
        &mut self,
        pins: &RedoPins,
    ) -> Result<(), ConfigPinError> {
        self.apply_scope.judging = false;
        self.apply_scope.judged = JudgedConfigs::open(pins, &self.crs_set, self.crs_digest)?;
        self.apply_scope.judging = true;
        Ok(())
    }

    /// End the validate. The view stays until the record's apply ends.
    /// Every pin's install config is derived here, so a pin that does not
    /// decode refuses before any write. `Err` with the first such pin.
    pub(in crate::data::executor) fn end_judging(&mut self) -> Result<(), ConfigPinError> {
        self.apply_scope.judging = false;
        let Some(view) = self.apply_scope.judged.as_mut() else {
            return Ok(());
        };
        view.prime_install(&self.doc_configs);
        match view.take_failure() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Open the judged view of `task`, a plain replicated write its proposer
    /// pinned, for the whole apply. Its row bodies derive, and its rules
    /// judge, under its pins. Returns what the view set aside, which
    /// [`Self::restore_judging`] puts back once the apply ends.
    ///
    /// `None` for a task that carries no pins, and for a committed redo,
    /// whose apply opens its own view. `Err` refuses the write before any
    /// write: a pin it requires is missing, or a pin does not hash to its
    /// digest or does not decode. Every core refuses such a write alike.
    pub(in crate::data::executor) fn begin_pinned_write(
        &mut self,
        task: &ExecutionTask,
    ) -> Result<Option<HeldJudging>, ConfigPinError> {
        let Some(pins) = task.request.config_pins.as_deref() else {
            return Ok(None);
        };
        if let PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { .. }) = task.plan() {
            return Ok(None);
        }
        check_task_pinned(task, pins)?;
        self.begin_pinned_apply(pins).map(Some)
    }

    /// Open the judged view of a plain write pinned with `pins`, for its
    /// whole apply: its validate and its install are one pass. Every pin's
    /// install config is derived first, so a pin that does not decode
    /// refuses before any write, and the core holds the view it held
    /// before. A write with no CRS pin is refused: its proposer always pins
    /// the CRS set.
    pub(in crate::data::executor) fn begin_pinned_apply(
        &mut self,
        pins: &RedoPins,
    ) -> Result<HeldJudging, ConfigPinError> {
        let held = self.hold_judging();
        let mut view = match JudgedConfigs::open(pins, &self.crs_set, self.crs_digest) {
            Ok(Some(view)) => view,
            Ok(None) => {
                self.restore_judging(held);
                return Err(ConfigPinError::CrsUnpinned);
            }
            Err(error) => {
                self.restore_judging(held);
                return Err(error);
            }
        };
        view.prime_install(&self.doc_configs);
        if let Some(error) = view.take_failure() {
            self.restore_judging(held);
            return Err(error);
        }
        self.apply_scope.judged = Some(view);
        self.apply_scope.judging = true;
        Ok(held)
    }

    /// Set the current judged view aside, so a record's apply starts with
    /// none.
    pub(in crate::data::executor) fn hold_judging(&mut self) -> HeldJudging {
        HeldJudging {
            judged: self.apply_scope.judged.take(),
            judging: std::mem::replace(&mut self.apply_scope.judging, false),
        }
    }

    /// Drop the record's judged view and put back what `held` set aside.
    pub(in crate::data::executor) fn restore_judging(&mut self, held: HeldJudging) {
        self.apply_scope.judged = held.judged;
        self.apply_scope.judging = held.judging;
    }

    /// The config a check reads for `key`: the judged view's while a
    /// record's validate runs under one, else the core's current config.
    pub(in crate::data::executor) fn judged_doc_config(
        &self,
        key: &DocConfigKey,
    ) -> Option<&CollectionConfig> {
        self.judged_doc_config_of(key.0, key.1, &key.2)
    }

    /// [`Self::judged_doc_config`] by the key's parts.
    pub(in crate::data::executor) fn judged_doc_config_of(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> Option<&CollectionConfig> {
        match self.apply_scope.judging_view() {
            Some(view) => view.config(&self.doc_configs, database_id, tenant_id, collection),
            None => self.doc_config(database_id.as_u64(), tenant_id.as_u64(), collection),
        }
    }

    /// The config a row body for `key` derives under. While a pinned
    /// record's view is open, it is the pin's install config, in the
    /// validate and the install. Otherwise it is the current config. `Err`
    /// when the pin does not decode.
    pub(in crate::data::executor) fn install_doc_config(
        &self,
        key: &DocConfigKey,
    ) -> Result<Option<&CollectionConfig>, ConfigPinError> {
        self.install_doc_config_of(key.0, key.1, &key.2)
    }

    /// [`Self::install_doc_config`] by the key's parts.
    pub(in crate::data::executor) fn install_doc_config_of(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> Result<Option<&CollectionConfig>, ConfigPinError> {
        match self.apply_scope.judged.as_ref() {
            Some(view) => {
                view.install_config(&self.doc_configs, database_id, tenant_id, collection)
            }
            None => Ok(self.doc_config(database_id.as_u64(), tenant_id.as_u64(), collection)),
        }
    }

    /// The declared columns a KV value of `collection` re-types under while
    /// a pinned record's view is open: its pin's install config. `None` when
    /// no view is open. Then the record's own columns apply.
    pub(in crate::data::executor) fn pinned_declared_columns_of(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Result<Option<&[DeclaredColumn]>, ConfigPinError> {
        let Some(view) = self.apply_scope.judged.as_ref() else {
            return Ok(None);
        };
        let config = view.install_config(
            &self.doc_configs,
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection,
        )?;
        Ok(Some(config.map_or(&[][..], |config| {
            config.declared_columns.as_slice()
        })))
    }
}
