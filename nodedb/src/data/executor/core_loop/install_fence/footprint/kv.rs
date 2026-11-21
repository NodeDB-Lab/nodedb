// SPDX-License-Identifier: BUSL-1.1

//! Footprints of KV ops.
//!
//! A KV write keys the collections it writes. A sync frame, a write with
//! sync provenance, also keys the high-water mark of its stream. A
//! sorted-index op that names only its index keys the collection the core
//! registered the index over, and is a task of its whole database when no
//! such index is registered.

use nodedb_physical::physical_plan::KvOp;

use super::class::{Footprint, Scope};
use super::keys::{KeySet, read_of, write_of};

pub(super) fn kv_footprint(op: &KvOp, scope: &Scope<'_>) -> Footprint {
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
        } => KeySet::new(scope)
            .collection(collection)
            .sync(provenance.as_ref())
            .write(),
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
        KvOp::DropSortedIndex { index_name } => match sorted_index_keys(index_name, scope) {
            Some(keys) => keys.write(),
            None => scope.whole_database(),
        },
        KvOp::SortedIndexRank { index_name, .. }
        | KvOp::SortedIndexTopK { index_name, .. }
        | KvOp::SortedIndexRange { index_name, .. }
        | KvOp::SortedIndexCount { index_name, .. }
        | KvOp::SortedIndexScore { index_name, .. } => match sorted_index_keys(index_name, scope) {
            Some(keys) => keys.read(),
            None => scope.whole_database(),
        },
    }
}

/// The key of the collection the sorted index `index_name` covers. `None`
/// when the request's database and tenant register no such index.
fn sorted_index_keys<'s>(index_name: &str, scope: &'s Scope<'s>) -> Option<KeySet<'s>> {
    let collection = scope
        .registry
        .sorted_index_collection(scope.db, scope.tenant, index_name)?;
    Some(KeySet::new(scope).stored(&collection))
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{KvDeclaredColumns, PhysicalPlan};
    use nodedb_types::Surrogate;
    use nodedb_types::sync::wire::SyncProvenance;

    use super::super::keys::tests::{TENANT, coll, key, scope, whole, write};
    use super::super::plan::plan_footprint;
    use super::*;
    use crate::data::executor::core_loop::install_fence::FenceKey;
    use crate::types::DatabaseId;

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
    fn a_sync_frame_keys_its_collection_and_its_stream() {
        let provenance = SyncProvenance {
            producer_id: 1,
            epoch: 1,
            stream_id: 2,
            seq: 1,
        };
        assert_eq!(
            class(put(Some(provenance))),
            write(vec![
                key("k"),
                FenceKey::SyncStream {
                    producer_id: 1,
                    stream_id: 2,
                },
            ])
        );
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
    fn a_sorted_index_op_of_no_registered_index_is_its_whole_database() {
        let drop = KvOp::DropSortedIndex {
            index_name: "board".into(),
        };
        assert_eq!(class(drop), whole());
    }

    /// A core resolves a sorted index by name to the collection it covers.
    #[test]
    fn a_sorted_index_op_keys_the_collection_of_its_index() {
        let (mut core, _tx, _rx, _dir) = crate::data::executor::core_loop::tests::make_core();
        let def = crate::engine::kv::sorted_index::manager::SortedIndexDef {
            name: "board".into(),
            collection: "scores".into(),
            key_column: "player".into(),
            encoder: crate::engine::kv::sorted_index::key::SortKeyEncoder::new(Vec::new()),
            window: crate::engine::kv::sorted_index::window::WindowConfig::none(),
        };
        core.kv_engine.register_sorted_index(
            DatabaseId::DEFAULT.as_u64(),
            TENANT.as_u64(),
            "scores",
            def,
        );
        let scope = Scope {
            registry: &core,
            ..scope()
        };
        let count = KvOp::SortedIndexCount {
            index_name: "board".into(),
            system_time: nodedb_types::SystemTimeScope::Current,
        };
        assert_eq!(
            kv_footprint(&count, &scope),
            Footprint::Keys(vec![key("scores")])
        );
        let drop = KvOp::DropSortedIndex {
            index_name: "board".into(),
        };
        assert_eq!(kv_footprint(&drop, &scope), write(vec![key("scores")]));
        let other = KvOp::DropSortedIndex {
            index_name: "other".into(),
        };
        assert_eq!(kv_footprint(&other, &scope), whole());
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
