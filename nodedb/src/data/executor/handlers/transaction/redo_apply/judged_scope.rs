// SPDX-License-Identifier: BUSL-1.1

//! The judged view on the core: opened for a record's validate, read by
//! every validate check, and dropped when the record's apply ends.
//!
//! The view lives in the apply scope, which a parked install takes out of
//! the core whole. So it survives every yield, and a task between slices
//! reads the core's current configs.

use nodedb_physical::physical_plan::RedoPins;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::doc_config_key::DocConfigKey;
use crate::engine::document::store::CollectionConfig;
use crate::types::{DatabaseId, TenantId};

use super::config_pins::ConfigPinError;
use super::judged_configs::JudgedConfigs;

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
    /// `Err` with the first pin that did not decode.
    pub(in crate::data::executor) fn end_judging(&mut self) -> Result<(), ConfigPinError> {
        self.apply_scope.judging = false;
        match self
            .apply_scope
            .judged
            .as_mut()
            .and_then(JudgedConfigs::take_failure)
        {
            Some(error) => Err(error),
            None => Ok(()),
        }
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
}
