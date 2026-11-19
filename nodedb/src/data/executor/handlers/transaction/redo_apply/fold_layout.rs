// SPDX-License-Identifier: BUSL-1.1

//! The strict layout a folded target row encodes under.
//!
//! A fold rewrites a stored target row. The row encodes under the newer of
//! two layouts: the strict schema of the install config, and the layout of
//! the stored tuple. The stored layout is log-ordered, so every replica
//! picks the same one. A later record pinned at an older schema then keeps
//! every column an earlier record stored.
//!
//! The folded document keeps only the columns of the chosen layout. A
//! column the current schema added after it reads as null and is dropped.

use std::ops::Deref;
use std::rc::Rc;

use nodedb_physical::physical_plan::StorageMode;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::doc_config_key::DocConfigKey;
use crate::engine::document::store::CollectionConfig;

/// The config one folded row encodes under, when its stored layout is newer
/// than the install config's schema.
pub(in crate::data::executor) struct RowLayout {
    key: DocConfigKey,
    surrogate: u32,
    config: CollectionConfig,
}

/// The config a row body derives under: the install config, or a folded
/// row's own layout.
pub(in crate::data::executor) enum BodyConfig<'a> {
    Install(&'a CollectionConfig),
    Row(Rc<RowLayout>),
}

impl Deref for BodyConfig<'_> {
    type Target = CollectionConfig;

    fn deref(&self) -> &CollectionConfig {
        match self {
            Self::Install(config) => config,
            Self::Row(layout) => &layout.config,
        }
    }
}

impl CoreLoop {
    /// The config the body of row `surrogate` of `key` derives under: the
    /// folded row's own layout while its fold writes it, else
    /// [`Self::install_doc_config`]. `Err` when a pin does not decode.
    pub(in crate::data::executor) fn body_doc_config(
        &self,
        key: &DocConfigKey,
        surrogate: u32,
    ) -> crate::Result<Option<BodyConfig<'_>>> {
        let layout = self.apply_scope.row_layout.borrow().clone();
        if let Some(layout) = layout
            && layout.key == *key
            && layout.surrogate == surrogate
        {
            return Ok(Some(BodyConfig::Row(layout)));
        }
        Ok(self.install_doc_config(key)?.map(BodyConfig::Install))
    }

    /// Fit `doc`, the folded body of row `surrogate` of `key`, whose stored
    /// bytes are `stored`, to the layout it encodes under. Returns the
    /// row's own layout when the stored layout is newer than the install
    /// config's schema. The caller sets it around the row's write.
    pub(in crate::data::executor) fn fold_target_layout(
        &self,
        key: &DocConfigKey,
        surrogate: u32,
        stored: &[u8],
        doc: &mut serde_json::Value,
    ) -> crate::Result<Option<Rc<RowLayout>>> {
        let Some(install) = self.install_doc_config(key)? else {
            return Ok(None);
        };
        let StorageMode::Strict { schema: pinned } = &install.storage_mode else {
            return Ok(None);
        };
        let current = self
            .doc_config(key.0.as_u64(), key.1.as_u64(), &key.2)
            .map(|config| &config.storage_mode);
        let stored_layout = match current {
            Some(StorageMode::Strict { schema: current }) => {
                let version = nodedb_strict::TupleDecoder::new(current)
                    .schema_version(stored)
                    .map_err(|error| crate::Error::Storage {
                        engine: "materialized_sum".into(),
                        detail: format!(
                            "folded row of '{}' holds no readable tuple version: {error}",
                            key.2
                        ),
                    })?;
                (version > pinned.version).then(|| current.schema_for_version(version))
            }
            Some(StorageMode::Schemaless) | None => None,
        };
        let schema = stored_layout.as_ref().unwrap_or(pinned);
        if let Some(object) = doc.as_object_mut() {
            object.retain(|field, _| schema.columns.iter().any(|column| column.name == *field));
        }
        Ok(stored_layout.map(|schema| {
            let mut config = install.clone();
            config.storage_mode = StorageMode::Strict { schema };
            Rc::new(RowLayout {
                key: key.clone(),
                surrogate,
                config,
            })
        }))
    }

    /// Set `layout` as the folded row's layout, and return the one it
    /// replaces. The caller puts that back once the row's write ends.
    pub(in crate::data::executor) fn set_row_layout(
        &self,
        layout: Option<Rc<RowLayout>>,
    ) -> Option<Rc<RowLayout>> {
        self.apply_scope.row_layout.replace(layout)
    }
}
