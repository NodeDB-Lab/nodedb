// SPDX-License-Identifier: BUSL-1.1

//! The generated columns of a sum's source, applied to the post-images the
//! Control Plane folds a cross-shard delta from.
//!
//! The Data Plane recomputes every generated column on each row it stores. A
//! post-image formed from the body or the assignments alone carries the old or
//! absent generated value, so a sum over a generated column folds a delta the
//! stored row does not show. The Control Plane runs the same evaluator with
//! the same specs, read from the same catalog row.
//!
//! A strict source stores each generated value as its declared type: a
//! `DECIMAL(10,2)` column rounds to scale 2. The generated columns are re-typed
//! the same way, so the credited value is the stored one.
//!
//! Typeguards are not applied here. A typeguard DEFAULT or VALUE that rewrites
//! a bound column, and a BEFORE trigger that patches one, are not covered.

use nodedb_physical::physical_plan::{DeclaredColumn, GeneratedColumnSpec};

use crate::control::server::shared::ddl::neutral::collection::enforcement::build_generated_column_specs;
use crate::control::state::SharedState;
use crate::data::executor::handlers::generated::evaluate_generated_columns;
use crate::data::executor::strict_format::coerce_declared_doc;
use crate::types::{DatabaseId, TenantId};

/// One post-image pair: the stored row and the row the write stores.
type ImagePair = (Option<serde_json::Value>, Option<serde_json::Value>);

/// The generated columns of a sum's source, as the Data Plane holds them.
#[derive(Default)]
pub(super) struct SourceGenerated {
    /// The generated-column specs.
    specs: Vec<GeneratedColumnSpec>,
    /// The typed-schema columns that are generated and fix a stored numeric
    /// value. Empty for a source that is not strict.
    declared: Vec<DeclaredColumn>,
}

impl SourceGenerated {
    /// Evaluate the generated columns of `doc`, then re-type them.
    ///
    /// An expression that will not evaluate fails the statement, as it fails
    /// the write on the Data Plane.
    fn apply(
        &self,
        doc: &mut serde_json::Value,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<()> {
        if self.specs.is_empty() {
            return Ok(());
        }
        evaluate_generated_columns(doc, &self.specs, crs).map_err(crate::Error::DataPlane)?;
        coerce_declared_doc(doc, &self.declared)
    }

    /// Recompute the generated columns of every document in `docs`.
    pub(super) fn recompute_docs(
        &self,
        docs: &mut [serde_json::Value],
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<()> {
        for doc in docs {
            self.apply(doc, crs)?;
        }
        Ok(())
    }

    /// Recompute the generated columns of every post-image in `images`.
    ///
    /// A pre-image is a stored row, so it already carries the generated values
    /// the Data Plane wrote.
    pub(super) fn recompute_post_images(
        &self,
        images: &mut [ImagePair],
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<()> {
        for (_, new) in images {
            if let Some(doc) = new {
                self.apply(doc, crs)?;
            }
        }
        Ok(())
    }
}

/// The generated columns of `collection`, as the Data Plane holds them.
///
/// `collection` is the plan's name, db-qualified or bare. A collection with no
/// catalog row carries none.
pub(super) fn source_generated(
    state: &SharedState,
    collection: &str,
    tenant_id: TenantId,
    database_id: DatabaseId,
) -> crate::Result<SourceGenerated> {
    let name = nodedb_types::CollectionKey::from_relation_name(database_id, collection)?.name();
    let catalog = state.credentials.catalog();
    let Some(stored) = catalog.get_collection(database_id, tenant_id.as_u64(), name)? else {
        return Ok(SourceGenerated::default());
    };
    let specs = build_generated_column_specs(&stored);
    let declared = match &stored.collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            schema
                .columns
                .iter()
                .filter(|column| specs.iter().any(|spec| spec.name == column.name))
                .filter_map(DeclaredColumn::from_column_def)
                .collect()
        }
        _ => Vec::new(),
    };
    Ok(SourceGenerated { specs, declared })
}
