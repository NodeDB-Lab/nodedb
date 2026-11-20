// SPDX-License-Identifier: BUSL-1.1

//! The refusal of a KV row write that carries no surrogate. The live dispatch
//! and transaction staging both run it before any state changes.

use nodedb_physical::physical_plan::{KvOp, KvResolvedMutation};
use nodedb_types::Surrogate;

use crate::bridge::envelope::ErrorCode;

/// The refusal of a KV write that binds a row to `Surrogate::ZERO`. Every
/// row-writing op carries each row's bound surrogate, so `ZERO` there names
/// no row. A rewrite of an existing row and a key-named op carry none.
pub(in crate::data::executor) fn refuse_unbound_kv_write(op: &KvOp) -> Option<ErrorCode> {
    use crate::data::executor::handlers::unbound_surrogate::refuse_unbound;
    fn first_unbound(
        collection: &str,
        surrogates: impl IntoIterator<Item = Surrogate>,
    ) -> Option<ErrorCode> {
        surrogates
            .into_iter()
            .find_map(|surrogate| refuse_unbound("kv", collection, surrogate))
    }
    match op {
        KvOp::Put {
            collection,
            surrogate,
            ..
        }
        | KvOp::Insert {
            collection,
            surrogate,
            ..
        }
        | KvOp::InsertIfAbsent {
            collection,
            surrogate,
            ..
        }
        | KvOp::InsertOnConflictUpdate {
            collection,
            surrogate,
            ..
        }
        | KvOp::FieldSet {
            collection,
            surrogate,
            ..
        }
        | KvOp::Incr {
            collection,
            surrogate,
            ..
        }
        | KvOp::IncrFloat {
            collection,
            surrogate,
            ..
        }
        | KvOp::Cas {
            collection,
            surrogate,
            ..
        }
        | KvOp::GetSet {
            collection,
            surrogate,
            ..
        }
        | KvOp::TransferItem {
            dest_collection: collection,
            surrogate,
            ..
        } => refuse_unbound("kv", collection.as_str(), *surrogate),
        // One bound surrogate per entry. A shorter list leaves rows unbound.
        KvOp::BatchPut {
            collection,
            entries,
            surrogates,
            ..
        } if surrogates.len() != entries.len() => Some(ErrorCode::RejectedPrevalidation {
            reason: format!(
                "kv batch write into '{}' carries {} surrogates for {} entries; every row must \
                 be bound before it is stored",
                collection.as_str(),
                surrogates.len(),
                entries.len()
            ),
        }),
        KvOp::BatchPut {
            collection,
            surrogates,
            ..
        } => first_unbound(collection.as_str(), surrogates.iter().copied()),
        KvOp::Transfer {
            collection,
            debit_surrogate,
            credit_surrogate,
            ..
        } => first_unbound(collection.as_str(), [*debit_surrogate, *credit_surrogate]),
        KvOp::ResolvedWrite { mutations, .. } => mutations.iter().find_map(|mutation| {
            let KvResolvedMutation::Put {
                collection,
                surrogate,
                ..
            } = mutation
            else {
                return None;
            };
            refuse_unbound("kv", collection.as_str(), *surrogate)
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    fn put(surrogate: Surrogate) -> KvOp {
        KvOp::Put {
            declared_columns: nodedb_physical::physical_plan::KvDeclaredColumns::none(),
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        }
    }

    fn batch(surrogates: Vec<Surrogate>) -> KvOp {
        KvOp::BatchPut {
            declared_columns: nodedb_physical::physical_plan::KvDeclaredColumns::none(),
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            entries: vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ],
            ttl_ms: 0,
            intent: nodedb_physical::physical_plan::KvBatchIntent::Put,
            surrogates,
            returning: None,
            rls_filters: Vec::new(),
        }
    }

    #[test]
    fn an_unbound_row_write_is_refused_and_a_bound_one_passes() {
        assert!(matches!(
            refuse_unbound_kv_write(&put(Surrogate::ZERO)),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ));
        assert!(refuse_unbound_kv_write(&put(Surrogate::new(4))).is_none());
    }

    #[test]
    fn a_batch_needs_one_bound_surrogate_per_entry() {
        assert!(
            refuse_unbound_kv_write(&batch(vec![Surrogate::new(1), Surrogate::new(2)])).is_none()
        );
        assert!(
            refuse_unbound_kv_write(&batch(vec![Surrogate::new(1), Surrogate::ZERO])).is_some()
        );
        assert!(refuse_unbound_kv_write(&batch(vec![Surrogate::new(1)])).is_some());
    }
}
