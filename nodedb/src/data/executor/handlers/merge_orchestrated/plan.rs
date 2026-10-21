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

use super::super::identity_guard::IdentitySnapshot;
use super::super::merge::MergeParams;
use super::super::merge_helpers::{build_insert_doc, build_merged, build_update_doc, find_arm};

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
    /// The fields the arm assigns. Typeguards judge them and the fields a
    /// guard injects.
    pub(super) assigned: Vec<String>,
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
    /// The source row's id (its position in the shipped source) — the key
    /// the orchestrator's surrogate map is keyed by. Unique per row, NULL
    /// join keys included.
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

/// Decode a stored target row into JSON, with its identity injected under
/// `identity_column` for a schemaless row whose body lacks that column. Fails
/// rather than skipping — a row the classifier can't read is not "absent",
/// and treating it as absent inserts a duplicate of a row that already exists.
///
/// A schemaless row that lacks its identity column carries its identity only
/// in the row's storage key, never in the body — so a MERGE arm's condition
/// on that column, matched by [`find_arm`], must see the row's client-visible
/// identity injected here, never the raw storage key. A declared-key row
/// holds its key and gains no `id`. A strict row surfaces its key as a real
/// tuple column, so injection only runs on the schemaless arm.
fn decode_target(
    identity: &RowIdentity,
    bytes: &[u8],
    strict_schema: &Option<nodedb_types::columnar::StrictSchema>,
    identity_column: &str,
) -> crate::Result<serde_json::Value> {
    let mut doc = doc_format::decode_document_or_binary_tuple(
        bytes,
        strict_schema.as_ref(),
        "MERGE target row",
    )?;
    if strict_schema.is_none()
        && let Some(obj) = doc.as_object_mut()
        && !obj.contains_key(identity_column)
    {
        obj.insert(
            identity_column.to_string(),
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
        let source = super::super::join_source_map::JoinSource::build(
            "MERGE",
            params.source_label,
            params.source_join_col,
            params.source_rows,
        )?;
        let strict_schema = self.merge_strict_schema(database_id, tid, params.target_collection);
        let target_docs =
            self.collect_target_docs(database_id, tid, params.target_collection, txn_id)?;
        let identity_column = self.identity_column(database_id, tid, params.target_collection);

        let mut updates: Vec<MergeUpdate> = Vec::new();
        let mut deletes: Vec<MergeDelete> = Vec::new();
        let mut matched_source_rows: HashSet<&str> = HashSet::new();
        // Stand-in "no source row" document for NOT-MATCHED-BY-SOURCE arms,
        // matching the legacy walk's `&serde_json::Value::Null`.
        let null_source = serde_json::Value::Null;
        let crs = self.crs_context();

        for (key, bytes) in &target_docs {
            let key = *key;
            let identity = key.to_identity();
            let target_doc = decode_target(&identity, bytes, &strict_schema, &identity_column)?;
            // The source row whose join key a WHERE `=` would match.
            let (arm_kind, source_doc): (MergeClauseKindOp, &serde_json::Value) =
                if let Some(source_row) = source.probe(target_doc.get(params.target_join_col)) {
                    matched_source_rows.insert(source_row.id.as_str());
                    (MergeClauseKindOp::Matched, &source_row.doc)
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

            if let Some(arm) = find_arm(params.clauses, arm_kind, &context, &crs)? {
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
                            &crs,
                        )?;
                        IdentitySnapshot::capture(
                            strict_schema.as_ref(),
                            params.declared_primary_key,
                            upd,
                            &target_doc,
                        )
                        .check_unchanged(params.target_collection, &updated)?;
                        updates.push(MergeUpdate {
                            key,
                            body: encode_doc_body(&updated),
                            old_body: encode_doc_body(&target_doc),
                            assigned: crate::data::executor::handlers::write_body::assigned_names(
                                upd,
                            ),
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

        // Unmatched source rows → NOT-MATCHED INSERT arms. A source row whose
        // join key is NULL matches no target row, so it is NOT MATCHED too.
        let mut inserts: Vec<MergeInsert> = Vec::new();
        for source_row in source.rows() {
            if matched_source_rows.contains(source_row.id.as_str()) {
                continue;
            }
            if let Some(arm) = find_arm(
                params.clauses,
                MergeClauseKindOp::NotMatched,
                &source_row.doc,
                &crs,
            )? && let MergeActionOp::Insert { columns, values } = &arm.action
            {
                let body = encode_doc_body(&build_insert_doc(
                    columns,
                    values,
                    &source_row.doc,
                    params.source_alias,
                    &crs,
                )?);
                inserts.push(MergeInsert {
                    join_key: source_row.id.clone(),
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
