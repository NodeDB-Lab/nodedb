// SPDX-License-Identifier: BUSL-1.1

//! Footprints of KV ops.
//!
//! A KV write keys the collections it writes. A sync frame, a write with
//! sync provenance, also advances its producer's high-water mark, which no
//! key names, so it is a task of its whole database. A sorted-index op that
//! names only its index names no collection, so it is one too.

use nodedb_physical::physical_plan::KvOp;

use super::class::{Footprint, Scope};
use super::keys::{KeySet, read_of, write_of};

pub(super) fn kv_footprint(op: &KvOp, scope: &Scope) -> Footprint {
    match op {
        KvOp::Get { collection, .. }
        | KvOp::Scan { collection, .. }
        | KvOp::GetTtl { collection, .. }
        | KvOp::BatchGet { collection, .. }
        | KvOp::FieldGet { collection, .. }
        | KvOp::SortedIndexTxnRead { collection, .. }
        | KvOp::MaterializeScan { collection, .. } => read_of(collection, scope),
        KvOp::Put {
            collection,
            provenance,
            ..
        }
        | KvOp::Delete {
            collection,
            provenance,
            ..
        } => match provenance {
            Some(_) => scope.whole_database(),
            None => write_of(collection, scope),
        },
        KvOp::Insert { collection, .. }
        | KvOp::InsertIfAbsent { collection, .. }
        | KvOp::InsertOnConflictUpdate { collection, .. }
        | KvOp::Expire { collection, .. }
        | KvOp::Persist { collection, .. }
        | KvOp::BatchPut { collection, .. }
        | KvOp::RegisterIndex { collection, .. }
        | KvOp::DropIndex { collection, .. }
        | KvOp::FieldSet { collection, .. }
        | KvOp::Truncate { collection, .. }
        | KvOp::Incr { collection, .. }
        | KvOp::IncrFloat { collection, .. }
        | KvOp::Cas { collection, .. }
        | KvOp::GetSet { collection, .. }
        | KvOp::Transfer { collection, .. }
        | KvOp::RegisterSortedIndex { collection, .. }
        | KvOp::PredicateUpdate { collection, .. }
        | KvOp::PredicateDelete { collection, .. } => write_of(collection, scope),
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            ..
        } => KeySet::new(scope)
            .collection(source_collection)
            .collection(dest_collection)
            .write(),
        KvOp::ResolvedWrite { mutations, .. } => mutations
            .iter()
            .fold(KeySet::new(scope), |keys, mutation| {
                keys.collection(mutation.collection())
            })
            .write(),
        // The resolve pass reads what the wrapped write writes.
        KvOp::ResolveWrite(inner) => kv_footprint(inner, scope),
        KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => scope.whole_database(),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{KvDeclaredColumns, PhysicalPlan};
    use nodedb_types::Surrogate;
    use nodedb_types::sync::wire::SyncProvenance;

    use super::super::keys::tests::{coll, key, scope, whole, write};
    use super::super::plan::plan_footprint;
    use super::*;

    fn class(op: KvOp) -> Footprint {
        plan_footprint(&PhysicalPlan::Kv(op), &scope())
    }

    fn put(provenance: Option<SyncProvenance>) -> KvOp {
        KvOp::Put {
            collection: coll("k"),
            declared_columns: KvDeclaredColumns::none(),
            key: Vec::new(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance,
        }
    }

    #[test]
    fn a_put_keys_its_collection() {
        assert_eq!(class(put(None)), write(vec![key("k")]));
    }

    #[test]
    fn a_sync_frame_is_its_whole_database() {
        let provenance = SyncProvenance {
            producer_id: 1,
            epoch: 1,
            stream_id: 1,
            seq: 1,
        };
        assert_eq!(class(put(Some(provenance))), whole());
    }

    #[test]
    fn a_transfer_of_an_item_keys_its_source_and_its_destination() {
        let transfer = KvOp::TransferItem {
            source_collection: coll("src"),
            dest_collection: coll("dst"),
            declared_columns: KvDeclaredColumns::none(),
            item_key: Vec::new(),
            dest_key: Vec::new(),
            surrogate: Surrogate::new(1),
            source_rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            dest_rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        };
        assert_eq!(class(transfer), write(vec![key("src"), key("dst")]));
    }

    #[test]
    fn an_index_registration_keys_its_collection() {
        let register = KvOp::RegisterIndex {
            collection: coll("k"),
            field: "f".into(),
            field_position: 0,
            backfill: false,
        };
        assert_eq!(class(register), write(vec![key("k")]));
    }

    #[test]
    fn a_sorted_index_op_by_name_is_its_whole_database() {
        let drop = KvOp::DropSortedIndex {
            index_name: "board".into(),
        };
        assert_eq!(class(drop), whole());
    }

    #[test]
    fn a_get_is_a_read_of_its_collection() {
        let get = KvOp::Get {
            collection: coll("k"),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        };
        assert_eq!(class(get), Footprint::Keys(vec![key("k")]));
    }
}
