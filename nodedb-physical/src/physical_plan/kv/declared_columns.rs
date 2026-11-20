// SPDX-License-Identifier: Apache-2.0

//! The declared numeric columns a KV write re-types its values under.

use std::sync::Arc;

use nodedb_types::QualifiedCollection;

use super::op::KvOp;
use crate::physical_plan::document::DeclaredColumn;

/// The declared columns a KV write re-types under, carried on the op.
///
/// The Control Plane resolves them from the catalog before the write is
/// journaled or dispatched. The live write, its WAL record and its replay
/// all re-type under these columns, never under the config a core holds.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum KvDeclaredColumns {
    /// Set when the op is built. The Control Plane resolves it before the
    /// write leaves for the WAL or a core.
    Pending,
    /// The resolved columns. Empty when the collection declares none.
    Resolved(Arc<Vec<DeclaredColumn>>),
}

/// A KV write that reached a consumer before its declared columns resolved.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "internal invariant break: a KV write on '{collection}' carries unresolved declared \
     columns; the Control Plane resolves them before the write is journaled or dispatched"
)]
pub struct KvColumnsPending {
    pub collection: String,
}

impl KvColumnsPending {
    fn new(collection: &str) -> Self {
        Self {
            collection: collection.to_string(),
        }
    }
}

impl KvDeclaredColumns {
    /// The state of an op the Control Plane has not resolved yet.
    pub fn pending() -> Self {
        Self::Pending
    }

    /// `columns`, resolved.
    pub fn resolved(columns: Vec<DeclaredColumn>) -> Self {
        Self::Resolved(Arc::new(columns))
    }

    /// Resolved to no declared columns.
    pub fn none() -> Self {
        Self::resolved(Vec::new())
    }

    /// Whether the Control Plane has not resolved the columns yet.
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    /// The resolved columns. `Err` while pending. `collection` names the
    /// write's collection in the error.
    pub fn columns(&self, collection: &str) -> Result<&[DeclaredColumn], KvColumnsPending> {
        match self {
            Self::Resolved(columns) => Ok(columns.as_slice()),
            Self::Pending => Err(KvColumnsPending::new(collection)),
        }
    }

    /// The resolved columns as the shared list a record carries. `Err`
    /// while pending. `collection` names the write's collection in the error.
    pub fn shared(&self, collection: &str) -> Result<Arc<Vec<DeclaredColumn>>, KvColumnsPending> {
        match self {
            Self::Resolved(columns) => Ok(Arc::clone(columns)),
            Self::Pending => Err(KvColumnsPending::new(collection)),
        }
    }
}

impl KvOp {
    /// The declared columns this op re-types under, with the collection
    /// they belong to. `None` for an op that re-types no value.
    pub fn declared_columns(&self) -> Option<(&QualifiedCollection, &KvDeclaredColumns)> {
        match self {
            KvOp::Put {
                collection,
                declared_columns,
                ..
            }
            | KvOp::Insert {
                collection,
                declared_columns,
                ..
            }
            | KvOp::InsertIfAbsent {
                collection,
                declared_columns,
                ..
            }
            | KvOp::InsertOnConflictUpdate {
                collection,
                declared_columns,
                ..
            }
            | KvOp::BatchPut {
                collection,
                declared_columns,
                ..
            }
            | KvOp::FieldSet {
                collection,
                declared_columns,
                ..
            }
            | KvOp::Incr {
                collection,
                declared_columns,
                ..
            }
            | KvOp::IncrFloat {
                collection,
                declared_columns,
                ..
            }
            | KvOp::Cas {
                collection,
                declared_columns,
                ..
            }
            | KvOp::GetSet {
                collection,
                declared_columns,
                ..
            }
            | KvOp::Transfer {
                collection,
                declared_columns,
                ..
            }
            | KvOp::PredicateUpdate {
                collection,
                declared_columns,
                ..
            }
            | KvOp::TransferItem {
                dest_collection: collection,
                declared_columns,
                ..
            } => Some((collection, declared_columns)),
            KvOp::ResolveWrite(inner) => inner.declared_columns(),
            // These ops re-type no value. A resolved write applies images
            // its resolve already fitted.
            KvOp::Get { .. }
            | KvOp::Delete { .. }
            | KvOp::Scan { .. }
            | KvOp::Expire { .. }
            | KvOp::Persist { .. }
            | KvOp::GetTtl { .. }
            | KvOp::BatchGet { .. }
            | KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::FieldGet { .. }
            | KvOp::Truncate { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. }
            | KvOp::SortedIndexTxnRead { .. }
            | KvOp::MaterializeScan { .. }
            | KvOp::ResolvedWrite { .. }
            | KvOp::PredicateDelete { .. } => None,
        }
    }

    /// [`Self::declared_columns`], for the Control Plane to resolve.
    pub fn declared_columns_mut(
        &mut self,
    ) -> Option<(&QualifiedCollection, &mut KvDeclaredColumns)> {
        match self {
            KvOp::Put {
                collection,
                declared_columns,
                ..
            }
            | KvOp::Insert {
                collection,
                declared_columns,
                ..
            }
            | KvOp::InsertIfAbsent {
                collection,
                declared_columns,
                ..
            }
            | KvOp::InsertOnConflictUpdate {
                collection,
                declared_columns,
                ..
            }
            | KvOp::BatchPut {
                collection,
                declared_columns,
                ..
            }
            | KvOp::FieldSet {
                collection,
                declared_columns,
                ..
            }
            | KvOp::Incr {
                collection,
                declared_columns,
                ..
            }
            | KvOp::IncrFloat {
                collection,
                declared_columns,
                ..
            }
            | KvOp::Cas {
                collection,
                declared_columns,
                ..
            }
            | KvOp::GetSet {
                collection,
                declared_columns,
                ..
            }
            | KvOp::Transfer {
                collection,
                declared_columns,
                ..
            }
            | KvOp::PredicateUpdate {
                collection,
                declared_columns,
                ..
            }
            | KvOp::TransferItem {
                dest_collection: collection,
                declared_columns,
                ..
            } => Some((collection, declared_columns)),
            KvOp::ResolveWrite(inner) => inner.declared_columns_mut(),
            // These ops re-type no value. A resolved write applies images
            // its resolve already fitted.
            KvOp::Get { .. }
            | KvOp::Delete { .. }
            | KvOp::Scan { .. }
            | KvOp::Expire { .. }
            | KvOp::Persist { .. }
            | KvOp::GetTtl { .. }
            | KvOp::BatchGet { .. }
            | KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::FieldGet { .. }
            | KvOp::Truncate { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. }
            | KvOp::SortedIndexTxnRead { .. }
            | KvOp::MaterializeScan { .. }
            | KvOp::ResolvedWrite { .. }
            | KvOp::PredicateDelete { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_refuses_and_resolved_answers() {
        let err = KvDeclaredColumns::pending()
            .columns("c")
            .expect_err("pending");
        assert_eq!(err.collection, "c");
        assert!(
            KvDeclaredColumns::none()
                .columns("c")
                .expect("resolved")
                .is_empty()
        );
        let declared = DeclaredColumn::from_declared("v", "INT").expect("int");
        let resolved = KvDeclaredColumns::resolved(vec![declared.clone()]);
        assert_eq!(
            resolved.columns("c").expect("resolved"),
            std::slice::from_ref(&declared)
        );
        assert_eq!(*resolved.shared("c").expect("resolved"), vec![declared]);
        assert_eq!(
            KvDeclaredColumns::pending()
                .shared("c")
                .expect_err("pending")
                .collection,
            "c"
        );
    }

    #[test]
    fn a_wrapped_op_and_an_item_move_name_the_columns_they_carry() {
        let dest = QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, "dest");
        let mut moved = KvOp::TransferItem {
            source_collection: QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, "src"),
            dest_collection: dest.clone(),
            declared_columns: KvDeclaredColumns::pending(),
            item_key: b"i".to_vec(),
            dest_key: b"d".to_vec(),
            surrogate: nodedb_types::Surrogate::new(1),
            source_rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            dest_rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        };
        let (named, columns) = moved.declared_columns().expect("an item move re-types");
        assert_eq!(named, &dest, "the destination's columns apply");
        assert!(columns.is_pending());

        let mut wrapped = KvOp::ResolveWrite(Box::new(moved.clone()));
        if let Some((_, slot)) = wrapped.declared_columns_mut() {
            *slot = KvDeclaredColumns::none();
        }
        let KvOp::ResolveWrite(inner) = &wrapped else {
            panic!("still a resolve");
        };
        assert!(!inner.declared_columns().expect("columns").1.is_pending());
        assert!(moved.declared_columns_mut().is_some());

        let truncate = KvOp::Truncate {
            collection: dest,
            restart_identity: false,
        };
        assert!(truncate.declared_columns().is_none());
    }

    #[test]
    fn round_trips_through_msgpack() {
        let declared = DeclaredColumn::from_declared("v", "DECIMAL(5,2)").expect("decimal");
        for value in [
            KvDeclaredColumns::pending(),
            KvDeclaredColumns::none(),
            KvDeclaredColumns::resolved(vec![declared]),
        ] {
            let bytes = zerompk::to_msgpack_vec(&value).expect("encode");
            let decoded: KvDeclaredColumns = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(decoded, value);
        }
    }
}
