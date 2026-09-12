// SPDX-License-Identifier: BUSL-1.1

//! Shared MERGE plan layer: classify the target/source rows into resolved
//! UPDATE/DELETE/INSERT arms without writing. Shared by the RESOLVE and APPLY
//! passes so both derive an identical action set.

use std::collections::HashSet;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::doc_format::encode_resolved_wire_body as encode_doc_body;
use crate::engine::document::store::{RowIdentity, StorageKey};
use nodedb_physical::physical_plan::document::merge_types::{
    MergeActionOp, MergeClauseKind as MergeClauseKindOp,
};

use super::super::merge::MergeParams;
use super::super::merge_helpers::{
    build_insert_doc, build_merged, build_update_doc, find_arm, json_to_str,
};

/// A matched / not-matched-by-source UPDATE arm resolved to a rewrite.
pub(super) struct MergeUpdate {
    /// Existing target row's storage key.
    pub(super) key: StorageKey,
    /// Post-update document as MessagePack (pre-strict-encoding).
    pub(super) body: Vec<u8>,
    /// The target row as it stood BEFORE the arm, as MessagePack. An UPDATE
    /// arm's materialized-sum delta is the difference between the two images —
    /// without both, the arm's whole new value gets credited on top.
    pub(super) old_body: Vec<u8>,
}

/// A matched / not-matched-by-source DELETE arm resolved to a removal.
pub(super) struct MergeDelete {
    pub(super) key: StorageKey,
    /// The deleted target row as MessagePack, so the Control-Plane expander can
    /// extract its primary key when rewriting the delete into a concrete
    /// `PointDelete` for an in-transaction MERGE at COMMIT.
    pub(super) body: Vec<u8>,
}

/// A NOT-MATCHED INSERT arm resolved to a new row.
pub(super) struct MergeInsert {
    /// Source join value — the key the orchestrator's surrogate map is keyed by.
    pub(super) join_key: String,
    /// New document as MessagePack (pre-strict-encoding).
    pub(super) body: Vec<u8>,
}

/// The full resolved action set of a MERGE against a consistent read snapshot.
pub(super) struct MergePlanActions {
    pub(super) updates: Vec<MergeUpdate>,
    pub(super) deletes: Vec<MergeDelete>,
    pub(super) inserts: Vec<MergeInsert>,
}

/// Decode a stored target row into JSON, with `id` injected for a schemaless
/// row whose body carries none. Fails rather than skipping — a row the
/// classifier can't read is not "absent", and treating it as absent inserts a
/// duplicate of a row that already exists.
///
/// A schemaless collection with no declared `id` field carries its identity
/// only in the row's storage key, never in the body — so a MERGE arm's
/// `AND id ...` condition, matched by [`find_arm`], must see the row's
/// client-visible identity injected here, never the raw storage key. A
/// strict row already surfaces `id` as a real tuple column, so injection
/// only runs on the schemaless arm.
fn decode_target(
    identity: &RowIdentity,
    bytes: &[u8],
    strict_schema: &Option<nodedb_types::columnar::StrictSchema>,
) -> crate::Result<serde_json::Value> {
    let mut doc = doc_format::decode_document_or_binary_tuple(
        bytes,
        strict_schema.as_ref(),
        "MERGE target row",
    )?;
    if strict_schema.is_none()
        && let Some(obj) = doc.as_object_mut()
        && !obj.contains_key("id")
    {
        obj.insert(
            "id".to_string(),
            serde_json::Value::String(identity.as_str().to_string()),
        );
    }
    Ok(doc)
}

impl CoreLoop {
    /// Classify a MERGE against a point-in-time snapshot without writing.
    /// Shared by [`Self::execute_merge_resolve`] and
    /// [`Self::execute_merge_apply`] so both derive an identical action set.
    /// `txn_id` classifies against base ∪ overlay; `None` is base only.
    pub(super) fn collect_merge_plan(
        &self,
        database_id: u64,
        tid: u64,
        txn_id: Option<crate::types::TxnId>,
        params: &MergeParams<'_>,
    ) -> crate::Result<MergePlanActions> {
        let source_map = self.build_merge_source_map(
            database_id,
            tid,
            params.source_collection,
            params.source_join_col,
            params.source_rows,
        )?;
        let strict_schema = self.merge_strict_schema(database_id, tid, params.target_collection);
        let target_docs =
            self.collect_target_docs(database_id, tid, params.target_collection, txn_id)?;

        let mut updates: Vec<MergeUpdate> = Vec::new();
        let mut deletes: Vec<MergeDelete> = Vec::new();
        let mut matched_source_keys: HashSet<String> = HashSet::new();
        // Stand-in "no source row" document for NOT-MATCHED-BY-SOURCE arms,
        // matching the legacy walk's `&serde_json::Value::Null`.
        let null_source = serde_json::Value::Null;

        for (key, bytes) in &target_docs {
            let key = *key;
            let identity = key.to_identity();
            let target_doc = decode_target(&identity, bytes, &strict_schema)?;
            let join_val = target_doc
                .get(params.target_join_col)
                .map(json_to_str)
                .unwrap_or_default();

            let (arm_kind, source_doc): (MergeClauseKindOp, &serde_json::Value) =
                if let Some(source_doc) = source_map.get(&join_val) {
                    matched_source_keys.insert(join_val.clone());
                    (MergeClauseKindOp::Matched, source_doc)
                } else {
                    (MergeClauseKindOp::NotMatchedBySource, &null_source)
                };

            // MATCHED selects against the merged doc; NOT-MATCHED-BY-SOURCE
            // selects against the target alone (no source row).
            let context = if arm_kind == MergeClauseKindOp::Matched {
                build_merged(&target_doc, source_doc, params.source_alias)
            } else {
                target_doc.clone()
            };

            if let Some(arm) = find_arm(params.clauses, arm_kind, &context)? {
                match &arm.action {
                    MergeActionOp::Update { updates: upd } => {
                        // A strict target already refuses a NULL primary key
                        // at encode time; the guard only needs to run here
                        // for schemaless.
                        let pk = if strict_schema.is_none() {
                            params.declared_primary_key
                        } else {
                            None
                        };
                        let updated = build_update_doc(
                            params.target_collection,
                            &target_doc,
                            source_doc,
                            params.source_alias,
                            upd,
                            pk,
                        )?;
                        updates.push(MergeUpdate {
                            key,
                            body: encode_doc_body(&updated),
                            old_body: encode_doc_body(&target_doc),
                        });
                    }
                    MergeActionOp::Delete => deletes.push(MergeDelete {
                        key,
                        body: encode_doc_body(&target_doc),
                    }),
                    // INSERT is not a target-row arm; DoNothing is a no-op.
                    MergeActionOp::Insert { .. } | MergeActionOp::DoNothing => {}
                }
            }
        }

        // Unmatched source rows → NOT-MATCHED INSERT arms.
        let mut inserts: Vec<MergeInsert> = Vec::new();
        for (src_key, src_doc) in &source_map {
            if matched_source_keys.contains(src_key.as_str()) {
                continue;
            }
            if let Some(arm) = find_arm(params.clauses, MergeClauseKindOp::NotMatched, src_doc)?
                && let MergeActionOp::Insert { columns, values } = &arm.action
            {
                let body = encode_doc_body(&build_insert_doc(
                    columns,
                    values,
                    src_doc,
                    params.source_alias,
                )?);
                inserts.push(MergeInsert {
                    join_key: src_key.clone(),
                    body,
                });
            }
        }

        Ok(MergePlanActions {
            updates,
            deletes,
            inserts,
        })
    }
}
