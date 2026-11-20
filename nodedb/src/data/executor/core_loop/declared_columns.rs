// SPDX-License-Identifier: BUSL-1.1

//! The declared numeric columns a schemaless document write re-types, read
//! from the collection's registered config. A KV write carries its own.

use nodedb_physical::physical_plan::DeclaredColumn;

use crate::types::{DatabaseId, TenantId};

use super::CoreLoop;

impl CoreLoop {
    /// The declared numeric columns of the collection `config_key` names.
    ///
    /// Empty when the collection declares none, is strict or columnar, or is
    /// not registered on this core.
    pub(in crate::data::executor) fn declared_columns(
        &self,
        config_key: &(DatabaseId, TenantId, String),
    ) -> &[DeclaredColumn] {
        self.doc_configs
            .get(config_key)
            .map(|config| config.declared_columns.as_slice())
            .unwrap_or(&[])
    }
}
