// SPDX-License-Identifier: BUSL-1.1

//! Shared MERGE plan layer: classify the target/source rows into resolved
//! UPDATE/DELETE/INSERT arms without writing. Shared by the RESOLVE and APPLY
//! passes so both derive an identical action set.

use std::collections::HashSet;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::engine::document::store::doc_id_to_surrogate;
use nodedb_physical::physical_plan::document::merge_types::{
    MergeActionOp, MergeClauseKind as MergeClauseKindOp,
};
use nodedb_types::Surrogate;

use super::super::merge::MergeParams;
use super::super::merge_helpers::{
    build_insert_doc, build_merged, build_update_doc, find_arm, json_to_str,
};

/// A matched / not-matched-by-source UPDATE arm resolved to a rewrite.
pub(super) struct MergeUpdate {
    /// Existing target storage key (the surrogate hex).
    pub(super) doc_id: String,
    /// The row's registered surrogate, parsed from `doc_id`. `None` only for
    /// legacy non-surrogate rows that predate surrogate-keyed storage.
    pub(super) surrogate: Option<Surrogate>,
    /// Post-update document as MessagePack (pre-strict-encoding).
    pub(super) body: Vec<u8>,
}

/// A matched / not-matched-by-source DELETE arm resolved to a removal.
pub(super) struct MergeDelete {
    pub(super) doc_id: String,
    pub(super) surrogate: Option<Surrogate>,
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

/// Encode a merge row body to the standard schemaless document wire format.
///
/// The document store, the scan decoder, AND the vector-search result flattener
/// (`from_msgpack::<HashMap<String, nodedb_types::Value>>`) all expect the
/// `nodedb_types::Value` msgpack encoding that plain `INSERT` produces. Encoding
/// via `json_to_msgpack` instead yields a JSON-flavoured map that the scan can
/// still read but the vector flattener cannot decode — so a merge-inserted row's
/// non-key fields come back empty from a vector search. Route through
/// `value_to_msgpack` so a merge body is byte-compatible with a plain insert.
fn encode_doc_body(doc: &serde_json::Value) -> Vec<u8> {
    let value: nodedb_types::Value = doc.clone().into();
    nodedb_types::value_to_msgpack(&value).unwrap_or_else(|_| doc_format::encode_to_msgpack(doc))
}

/// Decode a stored target row into JSON, using the strict schema when present.
fn decode_target(
    bytes: &[u8],
    strict_schema: &Option<nodedb_types::columnar::StrictSchema>,
) -> Option<serde_json::Value> {
    if let Some(schema) = strict_schema {
        crate::data::executor::strict_format::binary_tuple_to_json(bytes, schema)
    } else {
        doc_format::decode_document(bytes)
    }
}

impl CoreLoop {
    /// Classify a MERGE against a point-in-time snapshot WITHOUT writing.
    ///
    /// Walks the target rows (matched → UPDATE/DELETE, unmatched-by-source →
    /// UPDATE/DELETE) and the unmatched source rows (→ INSERT), collecting the
    /// resolved bodies. Shared by [`Self::execute_merge_resolve`] and
    /// [`Self::execute_merge_apply`] so both derive an identical action set.
    ///
    /// `txn_id` threads the staged transaction's identity into the target scan
    /// (`collect_target_docs`) so both the RESOLVE and APPLY passes classify
    /// against the transaction's CURRENT view = base ∪ overlay, seeing rows
    /// staged by earlier statements in the same transaction. `None` (autocommit)
    /// classifies against committed base storage only.
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

        for (doc_id, bytes) in &target_docs {
            let target_doc = match decode_target(bytes, &strict_schema) {
                Some(v) => v,
                None => continue,
            };
            let join_val = target_doc
                .get(params.target_join_col)
                .map(json_to_str)
                .unwrap_or_default();
            let surrogate = doc_id_to_surrogate(doc_id);

            let (arm_kind, source_doc): (MergeClauseKindOp, &serde_json::Value) =
                if let Some(source_doc) = source_map.get(&join_val) {
                    matched_source_keys.insert(join_val.clone());
                    (MergeClauseKindOp::Matched, source_doc)
                } else {
                    (MergeClauseKindOp::NotMatchedBySource, &null_source)
                };

            // MATCHED arms select against the merged (target + qualified source)
            // document; NOT-MATCHED-BY-SOURCE arms select against the target
            // alone (there is no source row). Mirrors the legacy walk.
            let context = if arm_kind == MergeClauseKindOp::Matched {
                build_merged(&target_doc, source_doc, params.source_alias)
            } else {
                target_doc.clone()
            };

            if let Some(arm) = find_arm(params.clauses, arm_kind, &context) {
                match &arm.action {
                    MergeActionOp::Update { updates: upd } => {
                        let updated =
                            build_update_doc(&target_doc, source_doc, params.source_alias, upd);
                        updates.push(MergeUpdate {
                            doc_id: doc_id.clone(),
                            surrogate,
                            body: encode_doc_body(&updated),
                        });
                    }
                    MergeActionOp::Delete => deletes.push(MergeDelete {
                        doc_id: doc_id.clone(),
                        surrogate,
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
            if let Some(arm) = find_arm(params.clauses, MergeClauseKindOp::NotMatched, src_doc)
                && let MergeActionOp::Insert { columns, values } = &arm.action
            {
                let body = encode_doc_body(&build_insert_doc(
                    columns,
                    values,
                    src_doc,
                    params.source_alias,
                ));
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
