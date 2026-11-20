// SPDX-License-Identifier: BUSL-1.1

//! The strict layout a rewritten stored row encodes under.
//!
//! A fold rewrites a stored target row, and an update rewrites the row it
//! updates. The row encodes under the newer of
//! two layouts: the strict schema of the install config, and the layout of
//! the stored tuple. The stored layout is log-ordered, so every replica
//! picks the same one. A later record pinned at an older schema then keeps
//! every column an earlier record stored.
//!
//! The rewritten document keeps only the columns of the chosen layout. A
//! column the current schema added after it reads as null and is dropped.

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

use nodedb_physical::physical_plan::{StorageMode, UpdateValue};
use nodedb_types::Value;
use nodedb_types::columnar::StrictSchema;

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

/// Whether a rewrite of a stored row keeps `field` under `schema`, the
/// rewrite schema. A field the schema declares stays. A field the write
/// assigns (`assigned`) stays too, so an undeclared assignment still refuses
/// at encode. Any other field is a column the current schema added after
/// `schema`, which the stored row decodes as null.
pub(in crate::data::executor) fn rewrite_keeps_field(
    schema: &StrictSchema,
    field: &str,
    assigned: bool,
) -> bool {
    assigned || schema.columns.iter().any(|column| column.name == field)
}

/// Whether one of `updates` assigns `field`.
pub(in crate::data::executor) fn assigns(updates: &[(String, UpdateValue)], field: &str) -> bool {
    updates.iter().any(|(assigned, _)| assigned == field)
}

/// Whether `stored` is a Binary Tuple: it opens with the tuple magic.
fn is_binary_tuple(stored: &[u8]) -> bool {
    matches!(stored, [a, b, c, d, ..] if u32::from_le_bytes([*a, *b, *c, *d]) == nodedb_strict::encode::MAGIC)
}

/// Fit `fields`, a stored row decoded under the current schema and merged
/// with a write's fields, to `schema`, the rewrite schema from
/// [`CoreLoop::write_rewrite_schema`]. `assigned` names the fields the write
/// sets, which stay.
pub(in crate::data::executor) fn fit_stored_rewrite(
    schema: &StrictSchema,
    fields: &mut HashMap<String, Value>,
    assigned: &[String],
) {
    fields.retain(|field, _| {
        rewrite_keeps_field(schema, field, assigned.iter().any(|name| name == field))
    });
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
        let Some(schema) = self.rewrite_schema(key, stored)? else {
            return Ok(None);
        };
        if let Some(object) = doc.as_object_mut() {
            object.retain(|field, _| rewrite_keeps_field(&schema, field, false));
        }
        self.row_layout_of(key, surrogate, schema)
    }

    /// The strict schema a write that assigns user fields rewrites the
    /// stored row `stored` of `key` under: the install config's schema.
    /// `None` when the install config is not strict.
    ///
    /// A stored layout newer than the install schema means the write's
    /// proposer pinned a catalog behind the data. The pin's type guards and
    /// generated columns then miss columns the row holds, so the write is
    /// refused with `StalePinnedSchema` before any write. The stored layout
    /// is in log order, so every replica refuses alike, and the client
    /// retries once the catalog caught up. With no pinned view the install
    /// schema is current, which no stored tuple is newer than.
    ///
    /// A fold assigns no user field, so it keeps the newer layout instead
    /// ([`Self::fold_target_layout`]).
    pub(in crate::data::executor) fn write_rewrite_schema(
        &self,
        key: &DocConfigKey,
        stored: &[u8],
    ) -> crate::Result<Option<&StrictSchema>> {
        let newer = match self.rewrite_schema(key, stored)? {
            None => return Ok(None),
            Some(Cow::Borrowed(schema)) => return Ok(Some(schema)),
            Some(Cow::Owned(newer)) => newer,
        };
        let pinned_version = match self
            .install_doc_config(key)?
            .map(|config| &config.storage_mode)
        {
            Some(StorageMode::Strict { schema }) => schema.version,
            Some(StorageMode::Schemaless) | None => 0,
        };
        Err(crate::Error::DataPlane(
            crate::bridge::envelope::ErrorCode::StalePinnedSchema {
                detail: format!(
                    "collection '{}': the stored row holds tuple version {}, newer than the \
                     pinned schema version {pinned_version}",
                    key.2, newer.version
                ),
            },
        ))
    }

    /// The row layout of row `surrogate` of `key` when `schema`, its
    /// rewrite schema, is the stored row's own layout. `None` when it is the
    /// install config's schema.
    fn row_layout_of(
        &self,
        key: &DocConfigKey,
        surrogate: u32,
        schema: Cow<'_, StrictSchema>,
    ) -> crate::Result<Option<Rc<RowLayout>>> {
        let Cow::Owned(schema) = schema else {
            return Ok(None);
        };
        let Some(install) = self.install_doc_config(key)? else {
            return Ok(None);
        };
        let mut config = install.clone();
        config.storage_mode = StorageMode::Strict { schema };
        Ok(Some(Rc::new(RowLayout {
            key: key.clone(),
            surrogate,
            config,
        })))
    }

    /// The strict schema a rewrite of the stored row `stored` of `key`
    /// encodes under: the newer of the install config's schema and the
    /// stored tuple's layout. An update and a fold both rewrite a stored
    /// row by this rule, so every replica encodes the same tuple.
    ///
    /// `Owned` when the stored layout is newer. `None` when the install
    /// config is not strict. The caller keeps only the schema's columns: a
    /// column the current schema added after it reads as null on decode.
    ///
    /// A core stores every row of a strict collection as a Binary Tuple:
    /// its strict config registers before the metadata apply moves past the
    /// CREATE, every replicated write waits for that floor, and a CONVERT
    /// re-encodes every stored version. So a strict row that is not a tuple
    /// is an invariant break, `Err` with `Internal`. Bytes that open with the
    /// tuple magic but hold no readable version are corrupt, `Err` with
    /// `Storage`.
    pub(in crate::data::executor) fn rewrite_schema(
        &self,
        key: &DocConfigKey,
        stored: &[u8],
    ) -> crate::Result<Option<Cow<'_, StrictSchema>>> {
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
            Some(StorageMode::Strict { .. }) if !is_binary_tuple(stored) => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "internal invariant break: a stored row of strict collection '{}' is \
                         not a Binary Tuple, so its rewrite has no layout to encode under",
                        key.2
                    ),
                });
            }
            Some(StorageMode::Strict { schema: current }) => {
                let version = nodedb_strict::TupleDecoder::new(current)
                    .schema_version(stored)
                    .map_err(|error| crate::Error::Storage {
                        engine: "document".into(),
                        detail: format!(
                            "stored row of '{}' holds no readable tuple version: {error}",
                            key.2
                        ),
                    })?;
                (version > pinned.version).then(|| current.schema_for_version(version))
            }
            Some(StorageMode::Schemaless) | None => None,
        };
        Ok(Some(match stored_layout {
            Some(schema) => Cow::Owned(schema),
            None => Cow::Borrowed(pinned),
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
