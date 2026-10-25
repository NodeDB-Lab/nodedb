// SPDX-License-Identifier: BUSL-1.1

//! The CRS context a Data Plane core resolves SRIDs against.

use std::sync::Arc;

use nodedb_crs::{CrsContext, CustomCrsSet};

use super::state::CoreLoop;

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
    pub(crate) fn crs_context(&self) -> CrsContext<'static> {
        CrsContext::shared(Arc::clone(&self.crs_set))
    }

    /// Install `set` as this core's user-defined CRS set. Boot installs the
    /// catalog's set before WAL replay; `MetaOp::SetCustomCrsSet` installs
    /// each committed change.
    pub fn set_crs_set(&mut self, set: Arc<CustomCrsSet>) {
        self.crs_set = set;
    }
}
