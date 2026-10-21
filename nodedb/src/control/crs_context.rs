// SPDX-License-Identifier: BUSL-1.1

//! The CRS context the Control Plane resolves SRIDs against.

use std::sync::Arc;

use nodedb_crs::{CrsContext, CustomCrsSet};

use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

/// The CRS context for one statement planned or evaluated against
/// `catalog`: its user-defined CRSs first, then the built-in registry. The
/// context owns its share of the set, so it borrows nothing.
pub fn catalog_crs_context(catalog: &SystemCatalog) -> CrsContext<'static> {
    CrsContext::shared(catalog.custom_crs_set())
}

/// The user-defined CRS set SRIDs resolve against under `state`: a share
/// of the set the catalog holds, as of this call.
pub fn crs_set_for(state: &SharedState) -> Arc<CustomCrsSet> {
    state.credentials.catalog().custom_crs_set()
}

/// The CRS context for one statement planned or evaluated against `state`.
pub fn crs_context_for(state: &SharedState) -> CrsContext<'static> {
    catalog_crs_context(state.credentials.catalog())
}
