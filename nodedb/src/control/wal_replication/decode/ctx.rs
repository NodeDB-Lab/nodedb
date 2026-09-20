// SPDX-License-Identifier: BUSL-1.1

//! Shared decode context threaded through every per-engine decode submodule.

use crate::types::{DatabaseId, TenantId};

/// The tenancy scope a committed entry decodes under. Decode rebuilds every
/// plan with the surrogates the record carries verbatim; `entry.rs` installs
/// them afterwards through the one shared plan binder.
pub(super) struct DecodeCtx {
    pub(super) database_id: DatabaseId,
    pub(super) tenant_id: TenantId,
}
