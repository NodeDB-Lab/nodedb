// SPDX-License-Identifier: BUSL-1.1

//! The declared numeric columns a schemaless document write re-types, read
//! from the collection's install config. A KV write carries its own.

use nodedb_physical::physical_plan::DeclaredColumn;

use crate::types::{DatabaseId, TenantId};

use super::CoreLoop;

impl CoreLoop {
    /// The declared numeric columns of the collection `config_key` names.
    ///
    /// A re-typed field is row-body shape, so it reads the install config.
    /// A pinned record's is its proposer's. Otherwise it is the current one.
    ///
    /// Empty when the collection declares none, is strict or columnar, or is
    /// not registered on this core. `Err` when the collection's pin does
    /// not decode.
    pub(in crate::data::executor) fn declared_columns(
        &self,
        config_key: &(DatabaseId, TenantId, String),
    ) -> crate::Result<&[DeclaredColumn]> {
        Ok(self
            .install_doc_config(config_key)?
            .map_or(&[][..], |config| config.declared_columns.as_slice()))
    }
}
