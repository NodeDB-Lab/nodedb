// SPDX-License-Identifier: BUSL-1.1

//! Fold the system-time ordinals an installed columnar engine holds into the
//! shared clock.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Fold the newest version ordinal of the columnar engine at `key`,
    /// every `sys_from` and `sys_to` of its memtable and flushed segments,
    /// into the shared clock. A snapshot install or a restore lands versions
    /// another node stamped, so a later write here stamps above every one of
    /// them. The engine reads its stamps, never a segment. A bitemporal
    /// row's `sys_from` is its `_ts_system`.
    pub(in crate::data::executor) fn fold_columnar_system_ordinals(
        &self,
        key: &(DatabaseId, TenantId, String),
    ) {
        if let Some(ordinal) = self
            .columnar_engines
            .get(key)
            .and_then(|engine| engine.newest_ordinal())
        {
            self.observe_bitemporal_ordinal(ordinal);
        }
    }
}
