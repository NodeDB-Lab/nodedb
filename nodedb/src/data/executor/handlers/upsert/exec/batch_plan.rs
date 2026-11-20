// SPDX-License-Identifier: BUSL-1.1

//! Pure merging of a `DocumentOp::BatchUpsert` page into per-key candidate bodies.

use std::collections::HashMap;

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::Surrogate;
use nodedb_types::columnar::StrictSchema;

use super::merge_body::{decode_stored_row, merge_upsert_body};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::doc_config_key::DocConfigKey;
use crate::engine::document::store::{RowIdentity, StorageKey};

/// One key the page writes: its rows, the row the key held before the page,
/// and the body the page stores for it.
pub(super) struct PageKey<'a> {
    pub surrogate: Surrogate,
    pub identity: RowIdentity,
    /// The committed row before the page. `None` when the key was absent.
    pub committed: Option<Vec<u8>>,
    /// The MessagePack body the page stores: the first row's body for an
    /// absent key, the merge of every row of the key otherwise.
    pub post: Vec<u8>,
    /// The fields the merges wrote, for typeguards. `None` when the key was
    /// absent, so the whole document is judged.
    pub assigned: Option<Vec<String>>,
    /// The page rows that name this key.
    pub rows: Vec<&'a str>,
}

/// The statement-wide inputs the page is judged under.
pub(super) struct PageScope<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub strict_schema: Option<&'a StrictSchema>,
}

impl PageScope<'_> {
    /// The config key of the page's collection.
    fn config_key(&self) -> DocConfigKey {
        (
            crate::types::DatabaseId::new(self.database_id),
            crate::types::TenantId::new(self.tid),
            self.collection.to_string(),
        )
    }
}

/// The rows of a page and the clause every row merges with.
pub(super) struct PageRows<'a> {
    pub documents: &'a [(String, Vec<u8>)],
    pub surrogates: &'a [Surrogate],
    pub on_conflict_updates: &'a [(String, UpdateValue)],
}

impl CoreLoop {
    /// Merge page inputs without materializing defaults, generators, or policies.
    /// Keys return in first-row order.
    pub(super) fn plan_upsert_page<'a>(
        &self,
        scope: &PageScope<'_>,
        rows: PageRows<'a>,
    ) -> Result<Vec<PageKey<'a>>, ErrorCode> {
        let PageRows {
            documents,
            surrogates,
            on_conflict_updates,
        } = rows;
        let mut keys: Vec<PageKey<'a>> = Vec::new();
        let mut slot_of: HashMap<Surrogate, usize> = HashMap::new();
        for ((document_id, value), &surrogate) in documents.iter().zip(surrogates) {
            let earlier = slot_of
                .get(&surrogate)
                .copied()
                .and_then(|slot| keys.get_mut(slot));
            match earlier {
                // The key's earlier row stored `post`, so this row merges
                // into it.
                Some(key) => {
                    let existing = nodedb_types::value_from_msgpack(&key.post).map_err(|_| {
                        ErrorCode::Internal {
                            detail: "failed to decode document for upsert".into(),
                        }
                    })?;
                    let merged = merge_upsert_body(
                        existing,
                        value,
                        on_conflict_updates,
                        &self.crs_context(),
                    )?;
                    key.post = merged.body;
                    if let Some(assigned) = &mut key.assigned {
                        assigned.extend(merged.assigned);
                    }
                    key.rows.push(document_id.as_str());
                }
                None => {
                    let storage_key = StorageKey::for_surrogate(surrogate);
                    let committed = self
                        .current_row_body(
                            scope.database_id,
                            scope.tid,
                            scope.collection,
                            &storage_key,
                        )
                        .map_err(ErrorCode::from)?;
                    let (post, assigned) = match &committed {
                        None => (value.clone(), None),
                        Some(current) => {
                            let mut merged = merge_upsert_body(
                                decode_stored_row(current, scope.strict_schema)?,
                                value,
                                on_conflict_updates,
                                &self.crs_context(),
                            )?;
                            // The merged row rewrites the stored one, so it
                            // encodes under the rewrite schema.
                            self.fit_merged_upsert(&scope.config_key(), current, &mut merged)?;
                            (merged.body, Some(merged.assigned))
                        }
                    };
                    slot_of.insert(surrogate, keys.len());
                    keys.push(PageKey {
                        surrogate,
                        identity: RowIdentity::from_user_key(document_id.as_str()),
                        committed,
                        post,
                        assigned,
                        rows: vec![document_id.as_str()],
                    });
                }
            }
        }
        Ok(keys)
    }
}
