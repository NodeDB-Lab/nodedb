// SPDX-License-Identifier: BUSL-1.1

//! Data Plane enforcement of the row-level-security WRITE policy.
//!
//! The Control Plane compiles a collection's write policy into the plan's
//! `rls_write_check` slot. For a write whose row image is produced where it is
//! persisted — an update's post-image, a delete's pre-image, an upsert's merged
//! body — that image does not exist at plan time, so the predicate travels with
//! the plan and is decided here, against the bytes actually about to be written.
//!
//! Two rules this module exists to hold:
//!
//! - **A rejected row fails the statement.** Skipping it would report a write
//!   that never happened, and leave the remaining rows of a multi-row statement
//!   applied — a partial write the caller cannot see or undo.
//! - **[`RlsWriteCheck`] names why no predicate is attached.** A bare empty
//!   byte slice used to mean either "no policy applies" or "the predicate was
//!   lost", and the two were indistinguishable, which silently disabled the
//!   write policy when injection failed to run. The enum keeps those states
//!   apart: [`RlsWriteCheck::NoPolicyApplies`] and
//!   [`RlsWriteCheck::AlreadyDecidedElsewhere`] admit everything, exactly as
//!   an empty check used to; a plan that reached this gate before RLS
//!   injection ran carries [`RlsWriteCheck::PendingInjection`] instead, and
//!   that is denied, not admitted.
//!
//! Distinct from [`super::rls_eval`], which decides the READ policy: that one
//! bounds which rows a `RETURNING` clause may show, this one bounds which rows
//! may be written at all.

use nodedb_types::columnar::{ColumnarSchema, StrictSchema};
use nodedb_types::{RlsWriteCheck, WriteGateDecision};

use crate::bridge::scan_filter::ScanFilter;

use super::columnar_read::filter::value_matches_filters;
use super::columnar_write::row_values_to_object;
use super::returning_doc;
use super::rls_eval;

/// Decide one already-decoded row image against the compiled write policy.
///
/// Fails closed: an undecodable filter payload or an evaluation error denies,
/// so an adversarial predicate cannot be turned into an admitted write.
pub(in crate::data::executor) fn admit_row(
    rls_write_check: &RlsWriteCheck,
    image: &serde_json::Value,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    match rls_write_check.decision() {
        WriteGateDecision::AdmitAll => Ok(()),
        WriteGateDecision::Evaluate(bytes) => {
            if rls_eval::rls_check_document(bytes, image) {
                return Ok(());
            }
            Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(tid),
                resource: format!("RLS write policy on '{collection}' rejected the row"),
            })
        }
        WriteGateDecision::DenyNotInjected => Err(crate::Error::Internal {
            detail: format!(
                "write plan for '{collection}' reached the Data Plane RLS write gate before \
                 RLS injection ran; this is an internal invariant break, not a policy rejection"
            ),
        }),
    }
}

/// Decide one STORED row body — the bytes about to be written, or the bytes
/// about to be removed — against the compiled write policy.
///
/// `strict_schema` is `Some` exactly when the collection stores Binary Tuples.
/// The decode goes through [`returning_doc::from_stored`] for the reason that
/// module exists: the MessagePack decoder does not reject a Binary Tuple, it
/// succeeds and yields a document with every real column missing, which would
/// fail every predicate and reject writes the policy actually permits.
///
/// A body that does not decode at all is refused rather than written
/// unchecked — an image the policy could not be evaluated against is not an
/// image the policy admitted.
///
/// `identity` is the row's client-visible identity. The caller decides the
/// encoding: a caller holding a document storage key converts it first.
pub(in crate::data::executor) fn admit_stored_row(
    rls_write_check: &RlsWriteCheck,
    body: &[u8],
    identity: &crate::engine::document::store::RowIdentity,
    strict_schema: Option<&StrictSchema>,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    match rls_write_check.decision() {
        WriteGateDecision::AdmitAll => Ok(()),
        WriteGateDecision::DenyNotInjected => Err(crate::Error::Internal {
            detail: format!(
                "write plan for '{collection}' reached the Data Plane RLS write gate before \
                 RLS injection ran; this is an internal invariant break, not a policy rejection"
            ),
        }),
        WriteGateDecision::Evaluate(_) => {
            match returning_doc::from_stored(body, identity, strict_schema) {
                Ok(image) => admit_row(rls_write_check, &image, tid, collection),
                Err(e) => Err(crate::Error::RejectedAuthz {
                    tenant_id: crate::types::TenantId::new(tid),
                    resource: format!(
                        "RLS write policy on '{collection}': row '{identity}' did not decode, so \
                     the policy could not be evaluated against it: {e}"
                    ),
                }),
            }
        }
    }
}

/// Decide one graph edge's STORED property object — the image of the edge
/// about to be tombstoned — against the compiled write policy.
///
/// An edge stores the `PROPERTIES` clause as the JSON object text the DSL
/// produced, not MessagePack, so the decode goes through JSON and the result is
/// decided by [`admit_row`]: the same evaluator the document path uses, so one
/// compiled predicate cannot mean one thing for a document row and another for
/// an edge's properties.
///
/// `properties` is `None` when no live edge version exists, and a body that is
/// not a JSON object — including an edge written with no `PROPERTIES` clause —
/// carries no field the predicate can test. Both deny: an image the policy
/// could not be evaluated against is not an image the policy admitted.
pub(in crate::data::executor) fn admit_edge_properties(
    rls_write_check: &RlsWriteCheck,
    properties: Option<&[u8]>,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    match rls_write_check.decision() {
        WriteGateDecision::AdmitAll => Ok(()),
        WriteGateDecision::DenyNotInjected => Err(crate::Error::Internal {
            detail: format!(
                "write plan for '{collection}' reached the Data Plane RLS write gate before \
                 RLS injection ran; this is an internal invariant break, not a policy rejection"
            ),
        }),
        WriteGateDecision::Evaluate(_) => {
            let decoded =
                properties.and_then(|bytes| sonic_rs::from_slice::<serde_json::Value>(bytes).ok());
            match decoded {
                Some(image @ serde_json::Value::Object(_)) => {
                    admit_row(rls_write_check, &image, tid, collection)
                }
                _ => Err(crate::Error::RejectedAuthz {
                    tenant_id: crate::types::TenantId::new(tid),
                    resource: format!(
                        "RLS write policy on '{collection}': the edge carries no decodable \
                         property object, so the policy could not be evaluated against it"
                    ),
                }),
            }
        }
    }
}

/// Decide one row image held as a [`nodedb_types::Value`] object.
///
/// The columnar family never materializes a row as a JSON document: its rows
/// are typed `Value`s, in schema order, and its own WHERE evaluation already
/// tests them through [`value_matches_filters`]. Routing the write gate through
/// that same evaluator is what keeps one compiled predicate from meaning one
/// thing on the read side and another on the write side — a JSON round-trip
/// here would retype every value on the way through.
///
/// Fails closed: an undecodable filter payload or an evaluation error denies.
pub(in crate::data::executor) fn admit_value_row(
    rls_write_check: &RlsWriteCheck,
    image: &nodedb_types::Value,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    let bytes = match rls_write_check.decision() {
        WriteGateDecision::AdmitAll => return Ok(()),
        WriteGateDecision::DenyNotInjected => {
            return Err(crate::Error::Internal {
                detail: format!(
                    "write plan for '{collection}' reached the Data Plane RLS write gate before \
                     RLS injection ran; this is an internal invariant break, not a policy \
                     rejection"
                ),
            });
        }
        WriteGateDecision::Evaluate(bytes) => bytes,
    };
    let admitted = match zerompk::from_msgpack::<Vec<ScanFilter>>(bytes) {
        Ok(filters) => value_matches_filters(image, &filters).unwrap_or(false),
        Err(_) => false,
    };
    if admitted {
        return Ok(());
    }
    Err(crate::Error::RejectedAuthz {
        tenant_id: crate::types::TenantId::new(tid),
        resource: format!("RLS write policy on '{collection}' rejected the row"),
    })
}

/// Decide one schema-ordered columnar row — the values about to be written, or
/// the values about to be removed — against the compiled write policy.
pub(in crate::data::executor) fn admit_columnar_row(
    rls_write_check: &RlsWriteCheck,
    row: &[nodedb_types::value::Value],
    schema: &ColumnarSchema,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    admit_value_row(
        rls_write_check,
        &row_values_to_object(schema, row),
        tid,
        collection,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn owner_policy(value: &str) -> Vec<u8> {
        let filter = ScanFilter {
            field: "owner".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String(value.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
    }

    #[test]
    fn no_policy_applies_admits_every_row() {
        assert!(
            admit_row(
                &RlsWriteCheck::NoPolicyApplies,
                &json!({"owner": "mallory"}),
                1,
                "orders"
            )
            .is_ok()
        );
    }

    #[test]
    fn pending_injection_is_denied() {
        assert!(matches!(
            admit_row(
                &RlsWriteCheck::PendingInjection,
                &json!({"owner": "mallory"}),
                1,
                "orders"
            ),
            Err(crate::Error::Internal { .. })
        ));
    }

    #[test]
    fn a_conforming_row_is_admitted() {
        assert!(
            admit_row(
                &RlsWriteCheck::Predicate(owner_policy("alice")),
                &json!({"owner": "alice"}),
                1,
                "orders"
            )
            .is_ok()
        );
    }

    #[test]
    fn a_violating_row_is_rejected() {
        assert!(matches!(
            admit_row(
                &RlsWriteCheck::Predicate(owner_policy("alice")),
                &json!({"owner": "bob"}),
                1,
                "orders"
            ),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A row missing the governed column cannot satisfy the predicate, so it is
    /// rejected rather than admitted by omission.
    #[test]
    fn a_row_without_the_governed_column_is_rejected() {
        assert!(
            admit_row(
                &RlsWriteCheck::Predicate(owner_policy("alice")),
                &json!({"note": "x"}),
                1,
                "orders"
            )
            .is_err()
        );
    }

    #[test]
    fn an_empty_check_admits_an_edge_with_no_properties() {
        assert!(admit_edge_properties(&RlsWriteCheck::NoPolicyApplies, None, 1, "knows").is_ok());
    }

    #[test]
    fn a_conforming_edge_property_object_is_admitted() {
        assert!(
            admit_edge_properties(
                &RlsWriteCheck::Predicate(owner_policy("alice")),
                Some(br#"{"owner":"alice"}"#),
                1,
                "knows"
            )
            .is_ok()
        );
    }

    #[test]
    fn a_violating_edge_property_object_is_rejected() {
        assert!(
            admit_edge_properties(
                &RlsWriteCheck::Predicate(owner_policy("alice")),
                Some(br#"{"owner":"bob"}"#),
                1,
                "knows"
            )
            .is_err()
        );
    }

    /// An edge with no live version, an empty `PROPERTIES` clause, or a body
    /// that is not a JSON object gives the predicate nothing to test, so each
    /// denies rather than being admitted by omission.
    #[test]
    fn an_edge_without_a_decodable_property_object_is_rejected() {
        let policy = RlsWriteCheck::Predicate(owner_policy("alice"));
        assert!(admit_edge_properties(&policy, None, 1, "knows").is_err());
        assert!(admit_edge_properties(&policy, Some(b""), 1, "knows").is_err());
        assert!(admit_edge_properties(&policy, Some(b"[1,2]"), 1, "knows").is_err());
    }

    /// A filter payload that does not deserialize denies rather than passing
    /// the row through unchecked.
    #[test]
    fn a_corrupt_check_denies() {
        assert!(
            admit_row(
                &RlsWriteCheck::Predicate(vec![0xFF, 0xFE]),
                &json!({"owner": "alice"}),
                1,
                "orders"
            )
            .is_err()
        );
    }
}
