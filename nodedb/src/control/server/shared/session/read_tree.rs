// SPDX-License-Identifier: BUSL-1.1

//! The read-set entries of a read plan and of every read nested in it.
//!
//! A plan can run child read plans inside its own task: a vector search's
//! prefilter, a hash join's bitmap producers and inputs, an aggregate's
//! input. Each child read records its own entry, with the collection and the
//! read key the child records when it runs alone. The children come from
//! [`children`], the one child walk every whole-plan pass shares.
//!
//! Every entry is homed like the root read: one entry on its collection's
//! vShard, and one more on each other vShard the read's cores reported.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::plan_children::children;
use crate::control::server::shared::plan_util::{extract_collection, plan_engine, read_key_of};
use crate::types::{DatabaseId, ReadShards, TenantId, VShardId};

use super::read_set::ReadSetEntry;

/// One read plan tree to turn into read-set entries.
pub(super) struct ReadTree<'a> {
    /// The plan the read's task ran.
    pub plan: &'a PhysicalPlan,
    /// The vShards the read's cores reported.
    pub read_vshards: &'a ReadShards,
    /// Whether the root point read observed a present row.
    pub found: bool,
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
}

impl ReadTree<'_> {
    /// The entries of the root read, then of every read nested in it.
    /// Duplicate entries are kept once.
    pub(super) fn entries(&self) -> Vec<ReadSetEntry> {
        let mut entries = Vec::new();
        for entry in self.plan_entries(self.plan, self.found) {
            push_distinct(&mut entries, entry);
        }
        self.push_nested(&mut entries);
        entries
    }

    /// The entries of every read nested in the root, without the root's
    /// own. Duplicate entries are kept once.
    pub(super) fn nested_entries(&self) -> Vec<ReadSetEntry> {
        let mut entries = Vec::new();
        self.push_nested(&mut entries);
        entries
    }

    /// Append the entries of every plan below the root, breadth first.
    ///
    /// A nested query operator that names no collection, or an empty one,
    /// records nothing of its own. It either wraps child plans the walk
    /// reaches, or reads rows the Control Plane gathered and captured
    /// already. A nested point read records as a miss: the response's hit
    /// flag describes the root only.
    fn push_nested(&self, entries: &mut Vec<ReadSetEntry>) {
        let mut pending = children(self.plan);
        let mut next = 0;
        while let Some(&child) = pending.get(next) {
            next += 1;
            let rows_of_its_own = !matches!(child, PhysicalPlan::Query(_))
                || extract_collection(child).is_some_and(|collection| !collection.is_empty());
            if rows_of_its_own {
                for entry in self.plan_entries(child, false) {
                    push_distinct(entries, entry);
                }
            }
            pending.extend(children(child));
        }
    }

    /// The entries of one plan, as a root read records them.
    ///
    /// A plan of one collection records one entry on the collection's
    /// vShard, and one homed entry per other reported vShard. A plan that
    /// names no collection observed every collection on each reported
    /// vShard: it records one homed entry per reported vShard.
    fn plan_entries(&self, plan: &PhysicalPlan, found: bool) -> Vec<ReadSetEntry> {
        let engine = plan_engine(plan);
        let key = read_key_of(plan, found);
        let collection = extract_collection(plan)
            .map(String::from)
            .unwrap_or_default();
        let entry = |home: Option<VShardId>| ReadSetEntry {
            engine,
            database_id: self.database_id,
            tenant_id: self.tenant_id,
            collection: collection.clone(),
            key: key.clone(),
            // The read ran at the block's snapshot.
            observed_at: None,
            home,
        };
        match nodedb_types::CollectionKey::from_qualified_str(self.database_id, &collection) {
            Ok(collection_key) if !collection.is_empty() => {
                let validation = collection_key.vshard();
                let mut entries = vec![entry(None)];
                entries.extend(
                    self.read_vshards
                        .iter()
                        .filter(|vshard| *vshard != validation)
                        .map(|vshard| entry(Some(vshard))),
                );
                entries
            }
            _ => self
                .read_vshards
                .iter()
                .map(|vshard| entry(Some(vshard)))
                .collect(),
        }
    }
}

/// Append `entry` unless `entries` holds it already.
fn push_distinct(entries: &mut Vec<ReadSetEntry>, entry: ReadSetEntry) {
    if !entries.contains(&entry) {
        entries.push(entry);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::types::ArrayId;
    use nodedb_physical::physical_plan::{
        ArrayOp, DocumentOp, ExchangeMode, ExchangeOp, QueryOp, VectorOp,
    };
    use nodedb_types::calvin::EngineTag;
    use nodedb_types::vector_distance::DistanceMetric;
    use nodedb_types::{QualifiedCollection, SystemTimeScope};

    use super::*;
    use crate::control::server::shared::session::read_set::ReadKey;

    const TENANT: TenantId = TenantId::new(1);

    fn qualified(collection: &str) -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, collection)
    }

    fn home_of(collection: &str) -> VShardId {
        nodedb_types::CollectionKey::from_qualified_str(DatabaseId::DEFAULT, collection)
            .expect("collection key")
            .vshard()
    }

    fn index_lookup(collection: &str, path: &str, value: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::IndexLookup {
            collection: qualified(collection),
            path: path.to_owned(),
            value: value.to_owned(),
            system_time: SystemTimeScope::Current,
        })
    }

    fn indexed_fetch(collection: &str, path: &str, value: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::IndexedFetch {
            collection: qualified(collection),
            path: path.to_owned(),
            value: value.to_owned(),
            also: Vec::new(),
            filters: Vec::new(),
            projection: Vec::new(),
            limit: usize::MAX,
            offset: 0,
            sort_keys: Vec::new(),
            system_time: SystemTimeScope::Current,
        })
    }

    fn array_scan(array: &str) -> PhysicalPlan {
        PhysicalPlan::Array(ArrayOp::SurrogateBitmapScan {
            array_id: ArrayId::new(TENANT, array),
            slice_msgpack: Vec::new(),
            system_time: SystemTimeScope::Current,
        })
    }

    fn search(collection: &str, prefilter: Option<PhysicalPlan>) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::Search {
            collection: qualified(collection),
            query_vector: vec![1.0, 0.0, 0.0],
            top_k: 10,
            ef_search: 0,
            metric: DistanceMetric::L2,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: prefilter.map(Box::new),
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            system_time: SystemTimeScope::Current,
        })
    }

    /// A join whose sides are embedded, so it names no collection itself,
    /// with one bitmap producer per side.
    fn embedded_join(left_bitmap: PhysicalPlan, right_bitmap: PhysicalPlan) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: qualified(""),
            right_collection: qualified(""),
            left_alias: None,
            right_alias: None,
            on: vec![("id".to_owned(), "id".to_owned())],
            join_type: "inner".to_owned(),
            limit: 0,
            projection: Vec::new(),
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters: Vec::new(),
            left_input: None,
            right_input: None,
            left_bitmap: Some(Box::new(left_bitmap)),
            right_bitmap: Some(Box::new(right_bitmap)),
            left_rls_filters: Vec::new(),
            right_rls_filters: Vec::new(),
            left_scan_filters: Vec::new(),
            right_scan_filters: Vec::new(),
        })
    }

    fn tree<'a>(plan: &'a PhysicalPlan, read_vshards: &'a ReadShards) -> ReadTree<'a> {
        ReadTree {
            plan,
            read_vshards,
            found: true,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TENANT,
        }
    }

    #[test]
    fn an_index_prefilter_records_its_collection_with_its_index_key() {
        let plan = search("memo", Some(index_lookup("agents", "agent_id", "a")));
        let reported = ReadShards::single(home_of("memo"));
        let entries = tree(&plan, &reported).entries();

        let prefilter: Vec<&ReadSetEntry> = entries
            .iter()
            .filter(|entry| entry.collection == "agents")
            .collect();
        assert_ne!(home_of("agents"), home_of("memo"), "the names home apart");
        assert_eq!(prefilter.len(), 2, "{entries:?}");
        assert!(prefilter.iter().all(|entry| {
            entry.engine == EngineTag::Document
                && entry.key
                    == ReadKey::IndexEq {
                        field: "agent_id".to_owned(),
                        value: "a".to_owned(),
                    }
                && entry.observed_at.is_none()
        }));
        // One entry on its own vShard, one homed on the search's vShard.
        assert_eq!(prefilter[0].home, None);
        assert_eq!(prefilter[1].home, Some(home_of("memo")));
        assert!(
            entries
                .iter()
                .any(|entry| entry.collection == "memo" && entry.engine == EngineTag::Vector),
            "the search records its own collection"
        );
    }

    #[test]
    fn an_array_prefilter_records_the_array_as_a_slice_does() {
        let plan = search("genes", Some(array_scan("genome")));
        let reported = ReadShards::single(home_of("genes"));
        let entries = tree(&plan, &reported).entries();

        let array: Vec<&ReadSetEntry> = entries
            .iter()
            .filter(|entry| entry.collection == "genome")
            .collect();
        assert!(!array.is_empty(), "{entries:?}");
        assert!(
            array
                .iter()
                .all(|entry| entry.engine == EngineTag::Array && entry.key == ReadKey::Predicate)
        );
        assert!(
            array
                .iter()
                .any(|entry| entry.validation_vshard() == Some(home_of("genes"))),
            "the scan ran on the search's vShard"
        );
    }

    #[test]
    fn nested_entries_leave_out_the_root_read() {
        let plan = search("memo", Some(index_lookup("agents", "agent_id", "a")));
        let reported = ReadShards::single(home_of("memo"));
        let entries = tree(&plan, &reported).nested_entries();
        assert!(!entries.is_empty());
        assert!(entries.iter().all(|entry| entry.collection == "agents"));
    }

    #[test]
    fn a_plan_with_no_nested_read_records_only_itself() {
        let plan = search("memo", None);
        let reported = ReadShards::single(home_of("memo"));
        let read = tree(&plan, &reported);
        assert!(read.nested_entries().is_empty());
        let entries = read.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].collection, "memo");
        assert_eq!(entries[0].home, None);
    }

    #[test]
    fn join_bitmap_producers_record_and_a_collectionless_join_records_nothing() {
        let plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(embedded_join(
                indexed_fetch("orders", "status", "open"),
                indexed_fetch("users", "tier", "gold"),
            )),
            mode: ExchangeMode::Gather {
                as_aggregate: false,
            },
        }));
        let reported = ReadShards::new();
        let entries = tree(&plan, &reported).nested_entries();
        assert_eq!(
            entries,
            vec![
                ReadSetEntry {
                    engine: EngineTag::Document,
                    database_id: DatabaseId::DEFAULT,
                    tenant_id: TENANT,
                    collection: "orders".to_owned(),
                    key: ReadKey::IndexEq {
                        field: "status".to_owned(),
                        value: "open".to_owned(),
                    },
                    observed_at: None,
                    home: None,
                },
                ReadSetEntry {
                    engine: EngineTag::Document,
                    database_id: DatabaseId::DEFAULT,
                    tenant_id: TENANT,
                    collection: "users".to_owned(),
                    key: ReadKey::IndexEq {
                        field: "tier".to_owned(),
                        value: "gold".to_owned(),
                    },
                    observed_at: None,
                    home: None,
                },
            ],
            "the join names no collection, so it adds no whole-vShard entry"
        );
    }

    #[test]
    fn a_repeated_nested_read_records_once() {
        let plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(embedded_join(
                indexed_fetch("orders", "status", "open"),
                indexed_fetch("orders", "status", "open"),
            )),
            mode: ExchangeMode::Gather {
                as_aggregate: false,
            },
        }));
        let reported = ReadShards::new();
        assert_eq!(tree(&plan, &reported).nested_entries().len(), 1);
    }
}
