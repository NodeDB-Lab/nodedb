// SPDX-License-Identifier: BUSL-1.1

//! The CRS context a Data Plane core resolves SRIDs against.

use std::sync::Arc;

use nodedb_crs::{CrsContext, CustomCrsSet};

use super::state::CoreLoop;
use crate::data::executor::handlers::transaction::redo_apply::JudgedConfigs;

impl CoreLoop {
    /// The CRS context for one request on this core. Build it once per
    /// request, outside any row loop, so parsed definitions are reused. The
    /// context owns a share of this core's user-defined CRS set and borrows
    /// nothing from the core, so a handler holds it across `&mut self`
    /// calls.
    ///
    /// It resolves this core's copy of the catalog's user-defined CRSs
    /// first, then the built-in registry: NodeDB's own CRSs and every EPSG
    /// CRS.
    ///
    /// A pinned record's validate resolves against the set its pin fixes.
    pub(crate) fn crs_context(&self) -> CrsContext<'static> {
        self.crs_context_under(self.apply_scope.judging_view())
    }

    /// The CRS context a row body derives under. A pinned record resolves
    /// against its pinned set from its first slice to the end of its apply.
    pub(in crate::data::executor) fn install_crs_context(&self) -> CrsContext<'static> {
        self.crs_context_under(self.apply_scope.judged.as_ref())
    }

    /// The context over `view`'s pinned set, else this core's set.
    fn crs_context_under(&self, view: Option<&JudgedConfigs>) -> CrsContext<'static> {
        match view {
            Some(view) => CrsContext::shared(Arc::clone(view.crs())),
            None => CrsContext::shared(Arc::clone(&self.crs_set)),
        }
    }

    /// Install `set` as this core's user-defined CRS set. Boot installs the
    /// catalog's set before WAL replay; `MetaOp::SetCustomCrsSet` installs
    /// each committed change.
    pub fn set_crs_set(&mut self, set: Arc<CustomCrsSet>) {
        self.crs_digest = crs_set_digest(&set, self.core_id);
        self.crs_set = set;
    }
}

/// The content digest of `set`, the CRS set core `core` holds, as a CRS
/// pin carries it: the hash of its MessagePack bytes.
///
/// A set that does not encode files one report here and takes digest 0.
/// 0 matches no pin, so a record pinned against it decodes its pin and
/// keeps its verdict.
pub(in crate::data::executor) fn crs_set_digest(set: &CustomCrsSet, core: usize) -> u64 {
    match zerompk::to_msgpack_vec(set) {
        Ok(bytes) => nodedb_physical::physical_plan::pin_digest(&bytes),
        Err(error) => {
            crate::diag::custom_crs_set_digest_unencoded(&error, core);
            0
        }
    }
}
