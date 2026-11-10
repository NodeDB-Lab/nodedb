// SPDX-License-Identifier: BUSL-1.1

//! Snapshot payload finalization: subscriber redaction, then shape-predicate
//! filtering.
//!
//! A shape snapshot seeds an offline replica, so whatever leaves here is
//! persisted on the device rather than merely displayed once. The Data-Plane
//! payload arrives as a msgpack array of `{id, data}` envelopes whose `data`
//! value is the stored row map verbatim, and it is shipped to the client as-is
//! — it never reaches the named-projection shaping core where a SELECT's rows
//! are redacted. The subscriber's column-redaction rules are therefore applied
//! to those stored bytes here, through the same
//! [`RedactionStore::apply_flat_row`](crate::control::security::redaction::RedactionStore::apply_flat_row)
//! matching every other delivery path uses.

use tracing::warn;

use nodedb_types::filter::MetadataFilter;
use nodedb_types::sync::wire::ShapeSnapshotRefusal;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::redaction::RedactionStore;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::{
    QueryRedaction, redact_document_row_bytes,
};
use crate::control::server::sync::shape::handler::ShapeSnapshotData;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::snapshot::SnapshotFailure;

/// Resolve the subscriber's redaction inputs for one snapshot dispatch.
///
/// Resolved once per snapshot rather than per row, and from the same
/// `RequestAuthScope` the dispatch itself authorizes against, so the roles a
/// policy is keyed on cannot disagree with the roles the read was authorized
/// under.
pub(super) fn snapshot_redaction(
    shared: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<QueryRedaction> {
    let scope = RequestAuthScope::for_database(identity, shared.auth_stores(), database_id)?;
    Ok(QueryRedaction::for_plan(
        identity.tenant_id,
        database_id,
        scope.auth(),
        plan,
    ))
}

/// The first shape-predicate field a redaction rule covers, if any.
///
/// A shape predicate is evaluated on the server against the STORED values, and
/// only matching rows are shipped. So a predicate naming a redacted column
/// discloses that column one probe at a time through row *presence*: a
/// subscriber that may not read `ssn` can still subscribe with `ssn = '...'`
/// and learn the answer from whether the snapshot comes back empty — masking
/// the delivered cell protects nothing. There is no correct filtering here, so
/// the caller refuses the subscription, mirroring how the plan-level refusal
/// resolves an aggregate over a redacted column.
///
/// An undecodable predicate reports `None`: it names no field this can act on,
/// and [`finalize_snapshot`] refuses it with a snapshot reject.
pub(super) fn predicate_redacted_field(
    predicate_bytes: &[u8],
    redaction: &QueryRedaction,
    store: &RedactionStore,
) -> Option<String> {
    if predicate_bytes.is_empty() || !redaction.has_any_rule(store) {
        return None;
    }
    let filter = zerompk::from_msgpack::<MetadataFilter>(predicate_bytes).ok()?;

    let mut fields = Vec::new();
    if !collect_predicate_fields(&filter, &mut fields) {
        // A predicate shape this cannot read might name a redacted column, so
        // it is reported as if it did.
        return Some("<unreadable predicate>".to_string());
    }
    fields
        .into_iter()
        .find(|field| redaction.field_has_rule(store, field))
}

/// Collect every field name `filter` tests, returning false when a variant
/// this does not recognize is reached.
fn collect_predicate_fields(filter: &MetadataFilter, out: &mut Vec<String>) -> bool {
    match filter {
        MetadataFilter::Eq { field, .. }
        | MetadataFilter::Ne { field, .. }
        | MetadataFilter::Gt { field, .. }
        | MetadataFilter::Gte { field, .. }
        | MetadataFilter::Lt { field, .. }
        | MetadataFilter::Lte { field, .. } => out.push(field.clone()),
        MetadataFilter::In { field, .. } | MetadataFilter::NotIn { field, .. } => {
            out.push(field.clone())
        }
        MetadataFilter::And(children) | MetadataFilter::Or(children) => {
            for child in children {
                if !collect_predicate_fields(child, out) {
                    return false;
                }
            }
        }
        MetadataFilter::Not(child) => return collect_predicate_fields(child, out),
        // `MetadataFilter` is `#[non_exhaustive]`, so a variant added to it
        // lands here. Reporting it as unreadable keeps this fail-closed: an
        // unrecognized predicate must not be assumed to name only unprotected
        // columns.
        _ => return false,
    }
    true
}

/// Everything needed to turn a raw snapshot payload into the delivered
/// snapshot.
pub(super) struct SnapshotPayload<'a> {
    /// Raw Data-Plane scan payload: a msgpack array of `{id, data}` envelopes.
    pub payload: Vec<u8>,
    /// Serialized `MetadataFilter`, empty when the shape has no predicate.
    pub predicate: &'a [u8],
    pub shape_id: &'a str,
    pub redaction: &'a QueryRedaction,
    pub store: &'a RedactionStore,
}

/// Redact a raw snapshot payload, then filter it by the shape predicate.
///
/// Redaction runs BEFORE the predicate so no row is ever evaluated in a state
/// the subscriber may not see. That ordering is only safe because
/// [`predicate_redacted_field`] has already refused any predicate naming a
/// redacted column — for every predicate that survives that gate, the fields it
/// tests are untouched by redaction and the two orders select identical rows.
///
/// Returns a [`SnapshotFailure`] when the payload cannot be delivered
/// safely. The caller then answers a snapshot reject and sends no snapshot.
/// An empty snapshot is not a substitute: it asserts the shape matches
/// nothing, and a client that believes it holds a complete baseline never
/// asks again.
pub(super) fn finalize_snapshot(
    req: SnapshotPayload<'_>,
) -> Result<ShapeSnapshotData, SnapshotFailure> {
    use crate::control::server::sync::shape::handler::decode_document_or_empty;
    use crate::data::executor::response_codec::{
        RowContext, encode_raw_document_rows, scan_rows_in_context,
    };
    use nodedb_query::metadata_filter::matches_metadata_filter;

    // A shape snapshot is a document scan: `{id, data}` envelopes. A payload
    // in no envelope shape cannot be delivered safely, so the subscriber is
    // told its snapshot failed.
    let docs = match scan_rows_in_context(&req.payload, RowContext::ScanEnvelope) {
        Ok(docs) => docs,
        Err(err) => {
            warn!(
                shape_id = req.shape_id,
                error = %err,
                "shape snapshot: the scan payload does not split into rows; sending a \
                 snapshot reject"
            );
            return Err(SnapshotFailure::new(
                ShapeSnapshotRefusal::MalformedRows,
                format!("the scan payload does not split into rows: {err}"),
            ));
        }
    };

    let redacting = req.redaction.has_any_rule(req.store);

    // Nothing to rewrite and nothing to filter: the payload is delivered
    // byte-identical, never round-tripped through a decode.
    if req.predicate.is_empty() && !redacting {
        let doc_count = docs.len();
        return Ok(ShapeSnapshotData {
            data: req.payload,
            doc_count,
        });
    }

    let filter = if req.predicate.is_empty() {
        None
    } else {
        match zerompk::from_msgpack::<MetadataFilter>(req.predicate) {
            Ok(f) => Some(f),
            Err(err) => {
                warn!(
                    shape_id = req.shape_id,
                    error = %err,
                    "shape snapshot: failed to decode predicate; sending a snapshot reject"
                );
                return Err(SnapshotFailure::new(
                    ShapeSnapshotRefusal::InvalidPredicate,
                    format!("the shape predicate does not decode: {err}"),
                ));
            }
        }
    };

    let row_redaction = redacting.then_some(req.redaction);
    let mut matching: Vec<(String, Vec<u8>)> = Vec::new();

    for (doc_id, mut data_bytes) in docs {
        if !redact_document_row_bytes(row_redaction, req.store, &mut data_bytes) {
            warn!(
                shape_id = req.shape_id,
                "shape snapshot: a row covered by a redaction policy could not be \
                 rewritten; sending a snapshot reject"
            );
            return Err(SnapshotFailure::new(
                ShapeSnapshotRefusal::MalformedRows,
                "a row covered by a redaction policy could not be rewritten",
            ));
        }
        let matches = match &filter {
            Some(filter) => matches_metadata_filter(&decode_document_or_empty(&data_bytes), filter),
            None => true,
        };
        if matches {
            matching.push((doc_id, data_bytes));
        }
    }

    let doc_count = matching.len();
    match encode_raw_document_rows(&matching) {
        Ok(data) => Ok(ShapeSnapshotData { data, doc_count }),
        Err(err) => {
            // Fail closed: a re-encode failure ships no rows, and an empty
            // snapshot would assert the shape matches nothing.
            warn!(
                shape_id = req.shape_id,
                error = %err,
                "shape snapshot: failed to encode filtered rows; sending a snapshot reject"
            );
            Err(SnapshotFailure::new(
                ShapeSnapshotRefusal::MalformedRows,
                format!("the filtered rows do not encode: {err}"),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::TenantId;

    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};
    use crate::data::executor::response_codec::{
        RowContext, encode_raw_document_rows, scan_rows_in_context,
    };

    use super::*;

    fn store_with_mask(collection: &str, role: &str, field: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: 1,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        store
    }

    fn redaction_for(collection: &str, role: &str) -> QueryRedaction {
        QueryRedaction::new(
            TenantId::new(1),
            vec![role.to_string()],
            vec![(String::new(), collection.to_string())],
        )
    }

    /// A Data-Plane document scan payload: the `{id, data}` envelope array the
    /// snapshot path ships to the device verbatim.
    fn scan_payload(rows: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let encoded: Vec<(String, Vec<u8>)> = rows
            .iter()
            .map(|(id, body)| {
                (
                    (*id).to_string(),
                    nodedb_types::json_to_msgpack(body).expect("encode stored row"),
                )
            })
            .collect();
        encode_raw_document_rows(&encoded).expect("encode scan payload")
    }

    fn rows_of(snapshot: &ShapeSnapshotData) -> Vec<(String, serde_json::Value)> {
        scan_rows_in_context(&snapshot.data, RowContext::ScanEnvelope)
            .expect("snapshot rows split")
            .into_iter()
            .map(|(id, data)| {
                (
                    id,
                    nodedb_types::json_from_msgpack(&data).expect("decode delivered row"),
                )
            })
            .collect()
    }

    fn finalize(
        payload: Vec<u8>,
        predicate: &[u8],
        redaction: &QueryRedaction,
        store: &RedactionStore,
    ) -> Result<ShapeSnapshotData, SnapshotFailure> {
        finalize_snapshot(SnapshotPayload {
            payload,
            predicate,
            shape_id: "sh1",
            redaction,
            store,
        })
    }

    /// The leak this module exists to close: a device-sync snapshot of a
    /// collection carrying a `Mask` rule for the subscriber's role shipped
    /// every column in the clear, and the device then PERSISTED them. The
    /// delivered payload must carry the mask.
    #[test]
    fn snapshot_of_a_ruled_collection_is_masked() {
        let store = store_with_mask("users", "support", "email");
        let payload = scan_payload(&[
            ("u1", serde_json::json!({"id": "u1", "email": "a@b.c"})),
            ("u2", serde_json::json!({"id": "u2", "email": "d@e.f"})),
        ]);

        let snapshot = finalize(payload, &[], &redaction_for("users", "support"), &store)
            .expect("snapshot is deliverable");

        let rows = rows_of(&snapshot);
        assert_eq!(snapshot.doc_count, 2);
        assert_eq!(rows[0].1["email"], "***");
        assert_eq!(rows[1].1["email"], "***");
        // The envelope shape itself is untouched — only the ruled cell.
        assert_eq!(rows[0].0, "u1");
        assert_eq!(rows[0].1["id"], "u1");
    }

    /// A role no policy names reads the stored value.
    #[test]
    fn snapshot_for_an_unruled_role_carries_the_stored_value() {
        let store = store_with_mask("users", "support", "email");
        let payload = scan_payload(&[("u1", serde_json::json!({"email": "a@b.c"}))]);

        let snapshot = finalize(payload, &[], &redaction_for("users", "analyst"), &store)
            .expect("snapshot is deliverable");

        assert_eq!(rows_of(&snapshot)[0].1["email"], "a@b.c");
    }

    /// No policy at all: the payload is handed on byte-identical, so an
    /// installation with no redaction configured sends exactly the bytes it
    /// sent before.
    #[test]
    fn snapshot_without_any_policy_is_byte_identical() {
        let payload = scan_payload(&[("u1", serde_json::json!({"email": "a@b.c"}))]);
        let original = payload.clone();

        let snapshot = finalize(
            payload,
            &[],
            &redaction_for("users", "support"),
            &RedactionStore::new(),
        )
        .expect("snapshot is deliverable");

        assert_eq!(snapshot.data, original);
        assert_eq!(snapshot.doc_count, 1);
    }

    /// A predicate over an unprotected column still filters, and the surviving
    /// rows are still masked.
    #[test]
    fn predicate_over_an_unruled_column_filters_and_still_masks() {
        let store = store_with_mask("users", "support", "email");
        let payload = scan_payload(&[
            (
                "u1",
                serde_json::json!({"status": "active", "email": "a@b.c"}),
            ),
            (
                "u2",
                serde_json::json!({"status": "closed", "email": "d@e.f"}),
            ),
        ]);
        let predicate = zerompk::to_msgpack_vec(&MetadataFilter::eq("status", "active"))
            .expect("encode predicate");

        let snapshot = finalize(
            payload,
            &predicate,
            &redaction_for("users", "support"),
            &store,
        )
        .expect("snapshot is deliverable");

        let rows = rows_of(&snapshot);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "u1");
        assert_eq!(rows[0].1["email"], "***");
    }

    /// Row presence is itself a disclosure: a predicate naming a redacted
    /// column is refused, so a subscriber cannot binary-search a masked value
    /// by watching which snapshots come back empty.
    #[test]
    fn predicate_naming_a_redacted_column_is_refused() {
        let store = store_with_mask("users", "support", "email");
        let redaction = redaction_for("users", "support");
        let predicate =
            zerompk::to_msgpack_vec(&MetadataFilter::eq("email", "a@b.c")).expect("encode");

        assert_eq!(
            predicate_redacted_field(&predicate, &redaction, &store),
            Some("email".to_string())
        );
    }

    /// The refusal reaches a redacted column nested under boolean structure —
    /// the shape an actual probe would take.
    #[test]
    fn nested_predicate_naming_a_redacted_column_is_refused() {
        let store = store_with_mask("users", "support", "email");
        let redaction = redaction_for("users", "support");
        let predicate = zerompk::to_msgpack_vec(&MetadataFilter::and(vec![
            MetadataFilter::eq("status", "active"),
            MetadataFilter::Not(Box::new(MetadataFilter::eq("email", "a@b.c"))),
        ]))
        .expect("encode");

        assert_eq!(
            predicate_redacted_field(&predicate, &redaction, &store),
            Some("email".to_string())
        );
    }

    /// A predicate over unprotected columns, an unruled role, and an empty
    /// predicate must all pass the gate — the refusal must not over-reach.
    #[test]
    fn predicate_gate_passes_when_no_redacted_column_is_named() {
        let store = store_with_mask("users", "support", "email");
        let predicate =
            zerompk::to_msgpack_vec(&MetadataFilter::eq("status", "active")).expect("encode");

        assert_eq!(
            predicate_redacted_field(&predicate, &redaction_for("users", "support"), &store),
            None
        );
        assert_eq!(
            predicate_redacted_field(&predicate, &redaction_for("users", "analyst"), &store),
            None
        );
        assert_eq!(
            predicate_redacted_field(&[], &redaction_for("users", "support"), &store),
            None
        );
    }

    /// A row a rule covers but whose stored bytes cannot be read must not be
    /// delivered, and must not be delivered as an empty snapshot either.
    #[test]
    fn unreadable_row_under_a_policy_sends_no_snapshot() {
        let store = store_with_mask("users", "support", "email");
        // Well-formed msgpack, but not the field map a stored row must be —
        // so the rules have nothing to match and the row cannot be cleared.
        let not_a_row =
            nodedb_types::json_to_msgpack(&serde_json::json!("not-a-row")).expect("encode");
        let payload =
            encode_raw_document_rows(&[("u1".to_string(), not_a_row)]).expect("encode payload");

        assert_eq!(
            finalize(payload, &[], &redaction_for("users", "support"), &store)
                .expect_err("an unredactable row must send no snapshot")
                .refusal,
            ShapeSnapshotRefusal::MalformedRows
        );
    }

    /// A scan payload that does not split into `{id, data}` rows is a typed
    /// failure the subscriber receives as a `ShapeSnapshotReject` frame,
    /// never a skipped or empty snapshot. This holds on the byte-identical
    /// path too, with no policy and no predicate.
    #[test]
    fn a_malformed_payload_is_a_snapshot_reject() {
        use crate::control::server::sync::wire::{SyncFrame, SyncMessageType};
        use nodedb_types::sync::wire::ShapeSnapshotRejectMsg;

        let flat = encode_raw_document_rows(&[]).expect("encode empty payload");
        let store = RedactionStore::new();
        assert!(finalize(flat, &[], &redaction_for("users", "support"), &store).is_ok());

        let not_envelopes =
            nodedb_types::json_to_msgpack(&serde_json::json!([{"k": 1}])).expect("encode rows");
        let failure = finalize(
            not_envelopes,
            &[],
            &redaction_for("users", "support"),
            &RedactionStore::new(),
        )
        .expect_err("rows with no envelope are refused");
        assert_eq!(failure.refusal, ShapeSnapshotRefusal::MalformedRows);

        let frame = failure.reject_frame("sh1").expect("reject frame encodes");
        assert_eq!(frame.msg_type, SyncMessageType::ShapeSnapshotReject);
        let reject: ShapeSnapshotRejectMsg = SyncFrame::from_bytes(&frame.to_bytes())
            .expect("frame decodes")
            .decode_body()
            .expect("reject decodes");
        assert_eq!(reject.shape_id, "sh1");
        assert_eq!(reject.refusal, ShapeSnapshotRefusal::MalformedRows);
    }

    /// An undecodable shape predicate is refused, never answered with an
    /// empty snapshot that claims the shape matches nothing.
    #[test]
    fn an_undecodable_predicate_is_a_snapshot_reject() {
        let payload = scan_payload(&[("u1", serde_json::json!({"status": "active"}))]);
        let failure = finalize(
            payload,
            &[0xc1],
            &redaction_for("users", "support"),
            &RedactionStore::new(),
        )
        .expect_err("an undecodable predicate is refused");
        assert_eq!(failure.refusal, ShapeSnapshotRefusal::InvalidPredicate);
    }
}
