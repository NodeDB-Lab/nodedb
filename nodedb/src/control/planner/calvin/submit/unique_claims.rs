// SPDX-License-Identifier: BUSL-1.1

//! UNIQUE value claims of a Calvin transaction's writes.
//!
//! A document write that sets a UNIQUE-indexed value locks
//! `(collection, index, value)` exclusively. Two transactions that claim one
//! value then serialize, so the Data Plane's UNIQUE judgment of the second
//! one sees the first one's row on every replica.
//!
//! A write whose claimed value the plan does not carry (an expression
//! update, a body this module cannot decode) locks its whole collection
//! instead. That over-locks, and never under-locks.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::{BTreeMap, BTreeSet};

use nodedb_cluster::calvin::types::{EngineKeySet, SortedVec, TxClass};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, UpdateValue};

use crate::Error;
use crate::control::state::SharedState;
use crate::engine::document::store::extract_index_values;

/// One UNIQUE index of a collection, as its claims are extracted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UniquePath {
    /// Index name.
    pub name: String,
    /// Indexed path, without a trailing `[]`.
    pub path: String,
    /// Whether the path indexes each array element.
    pub is_array: bool,
    /// Whether values are lowercased before the index judges them.
    pub case_insensitive: bool,
}

/// Add the UNIQUE claims of `tx_class`'s plans to its write set, read from
/// this node's catalog.
///
/// A class already split into parts was stamped by its coordinator, so it is
/// left as it is. A forwarded class is stamped again with the same result:
/// a key set already in the write set is not added twice.
pub(crate) fn stamp_unique_claims(
    state: &SharedState,
    tx_class: &mut TxClass,
) -> crate::Result<()> {
    if tx_class.is_multi_part() {
        return Ok(());
    }
    let plans =
        nodedb_physical::physical_plan::wire::decode_batch(&tx_class.plans).map_err(|e| {
            Error::Serialization {
                format: "msgpack".into(),
                detail: format!("calvin unique claims: plan decode: {e}"),
            }
        })?;
    let catalog = state.credentials.catalog();
    let database_id = tx_class.database_id;
    let tenant_id = tx_class.tenant_id.as_u64();
    let sets = unique_claim_sets(&plans, |collection| {
        let key = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)
            .unwrap_or_else(|_| nodedb_types::CollectionKey::from_bare(database_id, collection));
        let stored = catalog.get_collection(key.database_id(), tenant_id, key.name())?;
        Ok(stored.map_or_else(Vec::new, |stored| {
            stored
                .indexes
                .iter()
                .filter(|index| index.unique)
                .map(|index| UniquePath {
                    name: index.name.clone(),
                    path: index
                        .field
                        .strip_suffix("[]")
                        .unwrap_or(&index.field)
                        .to_owned(),
                    is_array: index.field.ends_with("[]"),
                    case_insensitive: index.case_insensitive,
                })
                .collect()
        }))
    })?;
    for set in sets {
        if !tx_class.write_set.0.contains(&set) {
            tx_class.write_set.0.push(set);
        }
    }
    Ok(())
}

/// The UNIQUE claim key sets of `plans`. `unique_paths` names the UNIQUE
/// indexes of a collection, as the plans name it.
pub(crate) fn unique_claim_sets(
    plans: &[PhysicalPlan],
    mut unique_paths: impl FnMut(&str) -> crate::Result<Vec<UniquePath>>,
) -> crate::Result<Vec<EngineKeySet>> {
    let mut claims = Claims::default();
    let mut paths_of: BTreeMap<String, Vec<UniquePath>> = BTreeMap::new();
    for plan in plans {
        let op = match plan {
            PhysicalPlan::Document(op) => op,
            PhysicalPlan::Kv(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => continue,
        };
        let Some(collection) = claiming_collection(op) else {
            continue;
        };
        if !paths_of.contains_key(collection) {
            let paths = unique_paths(collection)?;
            paths_of.insert(collection.to_owned(), paths);
        }
        let Some(paths) = paths_of.get(collection) else {
            continue;
        };
        if paths.is_empty() {
            continue;
        }
        claims.add_op(collection, op, paths);
    }
    Ok(claims.into_key_sets())
}

/// The collection of a document op that can claim a UNIQUE value. `None`
/// for a delete, a read, a DDL op, a write that locks its collection whole,
/// and a write no Calvin transaction sequences.
fn claiming_collection(op: &DocumentOp) -> Option<&str> {
    match op {
        DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::BatchInsert { collection, .. }
        | DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::ApplyBalanceDelta { collection, .. } => Some(collection.as_str()),
        DocumentOp::PointDelete { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => None,
    }
}

/// Claimed values by collection and index, and collections locked whole
/// because a claim is unknown. Ordered maps keep the key sets identical on
/// every node.
#[derive(Default)]
struct Claims {
    values: BTreeMap<(String, String), BTreeSet<Vec<u8>>>,
    whole: BTreeSet<String>,
}

impl Claims {
    /// Add the claims of `op` on `collection` against its UNIQUE `paths`.
    fn add_op(&mut self, collection: &str, op: &DocumentOp, paths: &[UniquePath]) {
        match op {
            DocumentOp::PointPut { value, .. } | DocumentOp::PointInsert { value, .. } => {
                self.add_body(collection, value, paths);
            }
            DocumentOp::Upsert {
                value,
                on_conflict_updates,
                ..
            } => {
                self.add_body(collection, value, paths);
                self.add_updates(collection, on_conflict_updates, paths);
            }
            DocumentOp::BatchInsert { documents, .. } => {
                for (_, body) in documents {
                    self.add_body(collection, body, paths);
                }
            }
            DocumentOp::PointUpdate { updates, .. } => {
                self.add_updates(collection, updates, paths);
            }
            // A delta moves a numeric column by an amount the plan does not
            // resolve to a value.
            DocumentOp::ApplyBalanceDelta { column, .. } => {
                if paths.iter().any(|path| touches(path, column)) {
                    self.whole.insert(collection.to_owned());
                }
            }
            DocumentOp::PointDelete { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::BulkUpdate { .. }
            | DocumentOp::BulkDelete { .. }
            | DocumentOp::Truncate { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::ResolvedWrite { .. }
            | DocumentOp::ResolveWrite(_)
            | DocumentOp::PointGet { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::MaterializeScan { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. } => {}
        }
    }

    /// Claim every UNIQUE value of a submitted document body.
    fn add_body(&mut self, collection: &str, body: &[u8], paths: &[UniquePath]) {
        let Some(doc) = decode_body(body) else {
            self.whole.insert(collection.to_owned());
            return;
        };
        for path in paths {
            self.claim(collection, path, &doc);
        }
    }

    /// Claim the UNIQUE values field updates assign. An update that touches
    /// an indexed path without a literal for exactly that path claims an
    /// unknown value.
    fn add_updates(
        &mut self,
        collection: &str,
        updates: &[(String, UpdateValue)],
        paths: &[UniquePath],
    ) {
        for (field, value) in updates {
            for path in paths.iter().filter(|path| touches(path, field)) {
                let literal = match value {
                    UpdateValue::Literal(bytes) if normalize(&path.path) == field.as_str() => {
                        nodedb_types::json_from_msgpack(bytes).ok()
                    }
                    UpdateValue::Literal(_) | UpdateValue::Expr(_) => None,
                };
                match literal {
                    Some(literal) => {
                        let doc = nest(field, literal);
                        self.claim(collection, path, &doc);
                    }
                    None => {
                        self.whole.insert(collection.to_owned());
                    }
                }
            }
        }
    }

    /// Claim the values of `path` in `doc`, canonicalized as the index
    /// judges them.
    fn claim(&mut self, collection: &str, path: &UniquePath, doc: &serde_json::Value) {
        let values = extract_index_values(doc, &path.path, path.is_array);
        if values.is_empty() {
            return;
        }
        let claimed = self
            .values
            .entry((collection.to_owned(), path.name.clone()))
            .or_default();
        for value in values {
            let value = if path.case_insensitive {
                value.to_lowercase()
            } else {
                value
            };
            claimed.insert(value.into_bytes());
        }
    }

    /// One `Unique` key set per collection and index, then one `Collection`
    /// key set per collection with an unknown claim.
    fn into_key_sets(self) -> Vec<EngineKeySet> {
        let mut sets: Vec<EngineKeySet> = self
            .values
            .into_iter()
            .map(|((collection, index), values)| EngineKeySet::Unique {
                collection,
                index,
                values: SortedVec::new(values.into_iter().collect()),
            })
            .collect();
        sets.extend(
            self.whole
                .into_iter()
                .map(|collection| EngineKeySet::Collection {
                    collection,
                    vshards: SortedVec::new(Vec::new()),
                }),
        );
        sets
    }
}

/// `path` without a leading `$.` or `$`.
fn normalize(path: &str) -> &str {
    path.strip_prefix("$.")
        .or_else(|| path.strip_prefix('$'))
        .unwrap_or(path)
}

/// Whether assigning `field` can change the value at the indexed `path`:
/// the two name one field, or one lies inside the other.
fn touches(path: &UniquePath, field: &str) -> bool {
    let indexed = normalize(&path.path);
    indexed == field
        || indexed
            .strip_prefix(field)
            .is_some_and(|rest| rest.starts_with('.'))
        || field
            .strip_prefix(indexed)
            .is_some_and(|rest| rest.starts_with('.'))
}

/// A document holding `value` at the dotted `field`.
fn nest(field: &str, value: serde_json::Value) -> serde_json::Value {
    field.rsplit('.').fold(value, |inner, segment| {
        let mut object = serde_json::Map::new();
        object.insert(segment.to_owned(), inner);
        serde_json::Value::Object(object)
    })
}

/// Decode a submitted document body: a MessagePack map or JSON. `None` for
/// any other form, such as a tagged value array or a strict Binary Tuple.
fn decode_body(body: &[u8]) -> Option<serde_json::Value> {
    let first = *body.first()?;
    let msgpack_map = matches!(first, 0x80..=0x8f | 0xde | 0xdf);
    let msgpack_array = matches!(first, 0x90..=0x9f | 0xdc | 0xdd);
    if msgpack_map {
        return nodedb_types::json_from_msgpack(body).ok();
    }
    if msgpack_array {
        return None;
    }
    sonic_rs::from_slice::<serde_json::Value>(body)
        .ok()
        .filter(serde_json::Value::is_object)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::cluster::calvin::scheduler::{
        AcquireOutcome, LockKey, LockManager, LockMode, TxnId,
    };
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    const USERS: &str = "users";

    fn email_index() -> UniquePath {
        UniquePath {
            name: "users_email".to_owned(),
            path: "$.email".to_owned(),
            is_array: false,
            case_insensitive: true,
        }
    }

    fn paths(collection: &str) -> crate::Result<Vec<UniquePath>> {
        Ok(if collection == USERS {
            vec![email_index()]
        } else {
            Vec::new()
        })
    }

    fn insert(id: &str, surrogate: u32, email: &str) -> PhysicalPlan {
        let body = nodedb_types::json_to_msgpack(&serde_json::json!({ "email": email }))
            .expect("encode body");
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, USERS),
            document_id: id.to_owned(),
            value: body,
            if_absent: false,
            surrogate: Surrogate::new(surrogate),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    fn update(id: &str, value: UpdateValue) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, USERS),
            document_id: id.to_owned(),
            surrogate: Some(Surrogate::new(3)),
            pk_bytes: id.as_bytes().to_vec(),
            updates: vec![("email".to_owned(), value)],
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    /// The scheduler's lock request for the claims of `plans`, as write keys.
    fn claim_locks(plans: &[PhysicalPlan]) -> BTreeMap<LockKey, LockMode> {
        let sets = unique_claim_sets(plans, paths).expect("claims");
        let tx_class = TxClass::new_single_vshard(
            nodedb_cluster::calvin::types::ReadWriteSet::new(Vec::new()),
            nodedb_cluster::calvin::types::ReadWriteSet::new(sets),
            Vec::new(),
            nodedb_types::TenantId::new(1),
            None,
            nodedb_cluster::calvin::types::VersionedReadSet::default(),
        )
        .expect("tx class");
        crate::control::cluster::calvin::scheduler::driver::helpers::expand_rw_set(
            &nodedb_cluster::calvin::types::SequencedTxn {
                epoch: 1,
                position: 0,
                tx_class,
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 2,
                lock_owner: None,
            },
        )
    }

    #[test]
    fn an_insert_claims_its_unique_value_canonicalized() {
        let sets = unique_claim_sets(&[insert("a", 1, "A@x.io")], paths).expect("claims");
        assert_eq!(
            sets,
            vec![EngineKeySet::Unique {
                collection: USERS.to_owned(),
                index: "users_email".to_owned(),
                values: SortedVec::new(vec![b"a@x.io".to_vec()]),
            }]
        );
    }

    #[test]
    fn two_inserts_of_one_unique_value_serialize() {
        let (first, second) = (TxnId::new(1, 0), TxnId::new(1, 1));
        let mut table = LockManager::new();
        assert_eq!(
            table.acquire(first, claim_locks(&[insert("a", 1, "a@x.io")])),
            AcquireOutcome::Ready
        );
        assert_eq!(
            table.acquire(second, claim_locks(&[insert("b", 2, "A@X.IO")])),
            AcquireOutcome::Blocked,
            "a second claim of one value waits for the first"
        );
        let third = TxnId::new(1, 2);
        assert_eq!(
            table.acquire(third, claim_locks(&[insert("c", 4, "c@x.io")])),
            AcquireOutcome::Ready,
            "a claim of another value runs beside them"
        );
        assert_eq!(table.release(first), vec![second]);
    }

    #[test]
    fn a_literal_update_claims_and_an_expression_update_locks_the_collection() {
        let literal = nodedb_types::json_to_msgpack(&serde_json::json!("New@x.io")).expect("lit");
        let sets =
            unique_claim_sets(&[update("a", UpdateValue::Literal(literal))], paths).expect("ok");
        assert_eq!(
            sets,
            vec![EngineKeySet::Unique {
                collection: USERS.to_owned(),
                index: "users_email".to_owned(),
                values: SortedVec::new(vec![b"new@x.io".to_vec()]),
            }]
        );

        let expr = UpdateValue::Expr(nodedb_query::expr::SqlExpr::Column("other".to_owned()));
        let sets = unique_claim_sets(&[update("a", expr)], paths).expect("ok");
        assert_eq!(
            sets,
            vec![EngineKeySet::Collection {
                collection: USERS.to_owned(),
                vshards: SortedVec::new(Vec::new()),
            }]
        );
    }

    #[test]
    fn a_collection_without_unique_indexes_claims_nothing() {
        let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "plain"),
            document_id: "a".to_owned(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        assert!(unique_claim_sets(&[plan], paths).expect("ok").is_empty());
    }
}
