// SPDX-License-Identifier: BUSL-1.1

//! Copy the database-scoped catalog rows of a clone source into its target.
//!
//! `CLONE DATABASE` stamps a shadow descriptor for every source collection.
//! A descriptor carries the collection's shape, not the rows other catalog
//! tables hold about it:
//!
//! - vector index parameters
//! - vector model metadata
//! - column statistics
//! - index records
//! - version-history checkpoints
//! - RLS and redaction policies
//! - triggers
//! - retention policies
//! - alert rules
//! - sequences, definition and counter
//! - continuous aggregates
//! - materialized views
//! - streaming materialized views
//!
//! Synonym groups and custom types are database-scoped too, and are
//! deliberately absent. Each needs effects this module cannot reach: the
//! in-memory registry `SHOW` reads, and for a synonym group the per-node FTS
//! backend. The clone handler proposes one catalog entry per row instead, so
//! the applier and both post-apply lanes run for it on every node.
//!
//! Each row is keyed by `database_id` and stays in the source unless this
//! module copies it.
//!
//! A clone missing them answers queries differently from the source
//! database:
//!
//! - no vector index builds
//! - the planner costs every scan with no statistics
//! - row-level security stops filtering
//! - `SHOW VERSIONS` and `AT VERSION` resolve no named checkpoint
//! - a materialized view's target collection is stamped with no definition
//!   to maintain it
//!
//! Every write here is fatal on failure, for the same reason the shadow
//! stamp is. Nothing later re-copies a row that silently failed to land.

use std::collections::{HashMap, HashSet};

use nodedb_types::{DatabaseId, QualifiedCollection};

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredOwner, SystemCatalog};

/// The collection names a clone stamps a descriptor for, per tenant.
///
/// Only active source collections are stamped. A row keyed on any other
/// collection lands in the target with no descriptor to belong to.
type StampedCollections = HashMap<u64, HashSet<String>>;

/// Copy every database-scoped catalog row of `source` into `target`.
///
/// Each row is rewritten to carry `target` before it is written. The copy
/// lands under the clone's own keys. The source rows stay untouched.
///
/// Call this after the target's shadow collection descriptors are stamped.
/// The per-collection policy tables are enumerated from the source's
/// collection list.
pub fn copy_database_metadata(
    catalog: &SystemCatalog,
    source: DatabaseId,
    target: DatabaseId,
) -> crate::Result<()> {
    let mut stamped = StampedCollections::new();
    for coll in catalog
        .load_all_collections(source)?
        .into_iter()
        .filter(|c| c.is_active)
    {
        stamped.entry(coll.tenant_id).or_default().insert(coll.name);
    }

    copy_collection_shape(catalog, source, target, &stamped)?;
    copy_scoped_objects(catalog, source, target)?;
    copy_collection_policies(catalog, source, target, &stamped)
}

/// Copy the rows describing the shape and history of a collection: vector
/// index build parameters, per-column embedding models, ANALYZE statistics,
/// the index identity registry, and named version-history checkpoints.
fn copy_collection_shape(
    catalog: &SystemCatalog,
    source: DatabaseId,
    target: DatabaseId,
    stamped: &StampedCollections,
) -> crate::Result<()> {
    let source_id = source.as_u64();
    let target_id = target.as_u64();

    for mut params in catalog
        .list_vector_index_params_in_database(source_id)?
        .into_iter()
        .filter(|p| is_stamped(stamped, p.tenant_id, &p.collection))
    {
        params.database_id = target_id;
        catalog.put_vector_index_params(&params).map_err(|e| {
            copy_err(
                "vector index params",
                &format!("{}.{}", params.collection, params.field_name),
                target,
                e,
            )
        })?;
    }

    for mut model in catalog
        .list_vector_models_in_database(source_id)?
        .into_iter()
        .filter(|m| is_stamped(stamped, m.tenant_id, &m.collection))
    {
        model.database_id = target_id;
        catalog.put_vector_model(&model).map_err(|e| {
            copy_err(
                "vector model",
                &format!("{}.{}", model.collection, model.column),
                target,
                e,
            )
        })?;
    }

    for mut stats in catalog
        .load_column_stats_in_database(source_id)?
        .into_iter()
        .filter(|s| is_stamped(stamped, s.tenant_id, &s.collection))
    {
        stats.database_id = target_id;
        catalog.put_column_stats(&stats).map_err(|e| {
            copy_err(
                "column stats",
                &format!("{}.{}", stats.collection, stats.column),
                target,
                e,
            )
        })?;
    }

    for mut record in catalog
        .list_index_records_in_database(source_id)?
        .into_iter()
        .filter(|r| r.is_active && is_stamped(stamped, r.tenant_id, &r.collection))
    {
        record.database_id = target_id;
        catalog
            .put_index_record(&record)
            .map_err(|e| copy_err("index record", &record.name, target, e))?;
    }

    // A checkpoint names a collection and a document the clone holds through
    // copy-up, and its version vector reads against the same oplog. Leaving it
    // behind makes `SHOW VERSIONS` and `AT VERSION` answer differently in the
    // clone than in the source.
    for (tenant_id, names) in stamped {
        for name in names {
            catalog
                .copy_checkpoints_for_collection(source_id, target_id, *tenant_id, name)
                .map_err(|e| copy_err("checkpoints", name, target, e))?;
        }
    }

    Ok(())
}

/// Copy the database-scoped objects that act on the clone's own collections:
/// triggers, retention policies, alert rules, sequences, materialized views,
/// continuous aggregates, and streaming materialized views.
///
/// Each object the integrity check pairs with an owner row gets that row here,
/// carrying the copied record's own in-band owner.
fn copy_scoped_objects(
    catalog: &SystemCatalog,
    source: DatabaseId,
    target: DatabaseId,
) -> crate::Result<()> {
    let source_id = source.as_u64();
    let target_id = target.as_u64();

    for mut trigger in catalog.load_triggers_for_database(source)? {
        trigger.database_id = target;
        catalog
            .put_trigger(&trigger)
            .map_err(|e| copy_err("trigger", &trigger.name, target, e))?;
        copy_owner_row(
            catalog,
            object_type::TRIGGER,
            target,
            trigger.tenant_id,
            &trigger.name,
            &trigger.owner,
        )?;
    }

    // Schedules are deliberately absent. A trigger fires on a write to the
    // clone's own collection, so it belongs to collection shape. A schedule
    // fires on its own clock instead. Copying one makes the clone issue DML
    // nobody asked for, doubling every external effect the source produces.
    // A missing schedule shows up in `SHOW SCHEDULES` and is cheap to recreate.

    for mut policy in catalog.load_retention_policies_in_database(source_id)? {
        policy.database_id = target_id;
        catalog
            .put_retention_policy(&policy)
            .map_err(|e| copy_err("retention policy", &policy.name, target, e))?;
    }

    for mut rule in catalog.load_alert_rules_in_database(source_id)? {
        rule.database_id = target_id;
        catalog
            .put_alert_rule(&rule)
            .map_err(|e| copy_err("alert rule", &rule.name, target, e))?;
    }

    // A `SERIAL` column's descriptor names its sequence, so the clone stamps
    // collections that read a sequence the source owns. The counter travels
    // with the definition: a clone restarting at START reissues identifiers the
    // source already handed out, and rows copied up carry those identifiers.
    for mut sequence in catalog.load_sequences_in_database(source_id)? {
        let source_state =
            catalog.get_sequence_state(source_id, sequence.tenant_id, &sequence.name)?;
        sequence.database_id = target_id;
        catalog
            .put_sequence(&sequence)
            .map_err(|e| copy_err("sequence", &sequence.name, target, e))?;
        if let Some(mut state) = source_state {
            state.database_id = target_id;
            catalog
                .put_sequence_state(&state)
                .map_err(|e| copy_err("sequence state", &sequence.name, target, e))?;
        }
        // A sequence owner row is keyed at database 0, matching the key the
        // sequence applier writes and the startup integrity check probes.
        catalog
            .put_owner(&StoredOwner {
                database_id: DatabaseId::DEFAULT.as_u64(),
                object_type: object_type::SEQUENCE.to_string(),
                object_name: sequence.name.clone(),
                tenant_id: sequence.tenant_id,
                owner_username: sequence.owner.clone(),
            })
            .map_err(|e| copy_err("sequence owner", &sequence.name, target, e))?;
    }

    // The clone stamps a descriptor for the view's implementation-owned target
    // collection, so the definition must travel with it. Without it the clone
    // holds a target no refresh loop maintains, and a cascade over the source
    // collection finds no dependent view to drop.
    for mut view in catalog.list_materialized_views_in_database(source_id)? {
        view.database_id = target_id;
        catalog
            .put_materialized_view(&view)
            .map_err(|e| copy_err("materialized view", &view.name, target, e))?;
        copy_owner_row(
            catalog,
            object_type::MATERIALIZED_VIEW,
            target,
            view.tenant_id,
            &view.name,
            &view.owner,
        )?;
    }

    for mut cagg in catalog.list_continuous_aggregates_in_database(source_id)? {
        cagg.database_id = target_id;
        catalog
            .put_continuous_aggregate(&cagg)
            .map_err(|e| copy_err("continuous aggregate", &cagg.name, target, e))?;
        copy_owner_row(
            catalog,
            object_type::CONTINUOUS_AGGREGATE,
            target,
            cagg.tenant_id,
            &cagg.name,
            &cagg.owner,
        )?;
    }

    for mut mv in catalog.load_streaming_mvs_for_database(source)? {
        mv.database_id = target;
        catalog
            .put_streaming_mv(&mv)
            .map_err(|e| copy_err("streaming mv", &mv.name, target, e))?;
        copy_owner_row(
            catalog,
            object_type::STREAMING_MATERIALIZED_VIEW,
            target,
            mv.tenant_id,
            &mv.name,
            &mv.owner,
        )?;
    }

    Ok(())
}

/// Copy the RLS and redaction policies of every active source collection.
///
/// Both tables key on the database-qualified collection name rather than a
/// leading `database_id`, so each collection is enumerated by its own prefix
/// and the qualified name is rebuilt for the target.
fn copy_collection_policies(
    catalog: &SystemCatalog,
    source: DatabaseId,
    target: DatabaseId,
    stamped: &StampedCollections,
) -> crate::Result<()> {
    for (tenant_id, names) in stamped {
        for name in names {
            let source_name = QualifiedCollection::new(source, name);
            let target_name = QualifiedCollection::new(target, name);

            for mut policy in
                catalog.list_rls_policies_for_collection(*tenant_id, source_name.as_str())?
            {
                policy.collection = target_name.as_str().to_string();
                catalog
                    .put_rls_policy(&policy)
                    .map_err(|e| copy_err("rls policy", &policy.name, target, e))?;
            }

            for mut policy in
                catalog.list_redaction_policies_for_collection(*tenant_id, source_name.as_str())?
            {
                policy.collection = target_name.as_str().to_string();
                catalog
                    .put_redaction_policy(&policy)
                    .map_err(|e| copy_err("redaction policy", &policy.for_role, target, e))?;
            }
        }
    }

    Ok(())
}

/// Write the `StoredOwner` row for one copied object, keyed by the target
/// database.
///
/// The startup integrity check pairs collections, functions, procedures,
/// triggers, materialized views, streaming materialized views, sequences,
/// schedules, change streams, and continuous aggregates with an owner row
/// keyed by the object's own database. Ownership also gates DDL
/// authorization, so the row must land with the copy rather than wait for a
/// later pass.
fn copy_owner_row(
    catalog: &SystemCatalog,
    object_type: &str,
    target: DatabaseId,
    tenant_id: u64,
    object_name: &str,
    owner_username: &str,
) -> crate::Result<()> {
    catalog
        .put_owner(&StoredOwner {
            database_id: target.as_u64(),
            object_type: object_type.to_string(),
            object_name: object_name.to_string(),
            tenant_id,
            owner_username: owner_username.to_string(),
        })
        .map_err(|e| copy_err(&format!("{object_type} owner"), object_name, target, e))
}

/// Whether a row's `(tenant_id, collection)` names a collection the clone
/// stamps a descriptor for.
fn is_stamped(stamped: &StampedCollections, tenant_id: u64, collection: &str) -> bool {
    stamped
        .get(&tenant_id)
        .is_some_and(|names| names.contains(collection))
}

/// Wrap a failed row copy with the table and row it belongs to.
fn copy_err(kind: &str, name: &str, target: DatabaseId, cause: crate::Error) -> crate::Error {
    crate::Error::Storage {
        engine: "catalog".into(),
        detail: format!(
            "clone: copying {kind} '{name}' into database {} failed: {cause}",
            target.as_u64()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nodedb_types::{StoredVectorIndexParams, VectorModelEntry, VectorModelMetadata};

    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::catalog::column_stats::StoredColumnStats;
    use crate::control::security::catalog::index_record::{IndexKind, StoredIndexRecord};

    const TENANT: u64 = 7;
    const SOURCE: DatabaseId = DatabaseId::new(2);
    const TARGET: DatabaseId = DatabaseId::new(3);

    fn open() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().expect("tmpdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("open catalog");
        (dir, catalog)
    }

    /// Seed one source collection carrying a vector index, a vector model row,
    /// and column statistics.
    fn seed_source(catalog: &SystemCatalog) {
        let mut coll = StoredCollection::stamped_for_test(TENANT, "chunks", "cloner");
        coll.database_id = SOURCE;
        catalog
            .put_collection(SOURCE, &coll)
            .expect("seed collection");

        catalog
            .put_vector_index_params(&StoredVectorIndexParams {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                collection: "chunks".into(),
                field_name: "embedding".into(),
                dim: 384,
                metric: "cosine".into(),
                m: 24,
                ef_construction: 200,
                index_type: String::new(),
                pq_m: 0,
                ivf_cells: 0,
                ivf_nprobe: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
            })
            .expect("seed vector index params");

        catalog
            .put_index_record(&StoredIndexRecord {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                name: "chunks_emb_idx".into(),
                kind: IndexKind::Vector,
                collection: "chunks".into(),
                fields: vec!["embedding".into()],
                is_active: true,
                state: crate::control::security::catalog::IndexRecordState::Ready,
                analyzer: None,
                fuzzy_default: None,
            })
            .expect("seed index record");

        catalog
            .put_vector_model(&VectorModelEntry {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                collection: "chunks".into(),
                column: "embedding".into(),
                metadata: VectorModelMetadata {
                    model: "all-MiniLM-L6-v2".into(),
                    dimensions: 384,
                    created_at: "2026-01-01".into(),
                    strict_dimensions: true,
                },
            })
            .expect("seed vector model");

        catalog
            .put_column_stats(&StoredColumnStats {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                collection: "chunks".into(),
                column: "body".into(),
                row_count: 10_000,
                null_count: 12,
                distinct_count: 9_800,
                min_value: Some("aardvark".into()),
                max_value: Some("zebra".into()),
                avg_value_len: Some(31),
                analyzed_at: 1_700_000_000_000,
            })
            .expect("seed column stats");
    }

    #[test]
    fn the_target_holds_every_copied_row_under_its_own_database() {
        let (_dir, catalog) = open();
        seed_source(&catalog);

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        let params = catalog
            .get_vector_index_params(TARGET.as_u64(), TENANT, "chunks", "embedding")
            .expect("read params")
            .expect("params must land in the target");
        assert_eq!(params.database_id, TARGET.as_u64());
        assert_eq!(params.dim, 384);
        assert_eq!(params.m, 24);
        assert_eq!(params.ef_construction, 200);
        assert_eq!(params.metric, "cosine");

        let model = catalog
            .get_vector_model(TARGET.as_u64(), TENANT, "chunks", "embedding")
            .expect("read model")
            .expect("model must land in the target");
        assert_eq!(model.database_id, TARGET.as_u64());
        assert_eq!(model.metadata.dimensions, 384);
        assert_eq!(model.metadata.model, "all-MiniLM-L6-v2");

        let stats = catalog
            .load_column_stats(TARGET.as_u64(), TENANT, "chunks")
            .expect("read stats");
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].database_id, TARGET.as_u64());
        assert_eq!(stats[0].row_count, 10_000);
        assert_eq!(stats[0].distinct_count, 9_800);
        assert_eq!(stats[0].max_value.as_deref(), Some("zebra"));

        let records = catalog
            .list_index_records_in_database(TARGET.as_u64())
            .expect("read index records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].database_id, TARGET.as_u64());
        assert_eq!(records[0].name, "chunks_emb_idx");
        assert_eq!(records[0].kind, IndexKind::Vector);
        assert_eq!(records[0].fields, vec!["embedding".to_string()]);
    }

    /// A `SERIAL` column's sequence must reach the clone with its counter
    /// intact. A clone restarting at START reissues identifiers the copied-up
    /// rows already carry.
    #[test]
    fn a_sequence_and_its_counter_follow_the_database_into_the_clone() {
        use crate::control::security::catalog::sequence_types::{SequenceState, StoredSequence};

        let (_dir, catalog) = open();
        seed_source(&catalog);
        catalog
            .put_sequence(&StoredSequence::new(
                SOURCE.as_u64(),
                TENANT,
                "chunks_id_seq".into(),
                "cloner".into(),
            ))
            .expect("seed sequence");
        catalog
            .put_sequence_state(&SequenceState::new(
                SOURCE.as_u64(),
                TENANT,
                "chunks_id_seq".into(),
                4_200,
                1,
            ))
            .expect("seed sequence state");

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        let copied = catalog
            .get_sequence(TARGET.as_u64(), TENANT, "chunks_id_seq")
            .expect("read sequence")
            .expect("the sequence must land in the target");
        assert_eq!(copied.database_id, TARGET.as_u64());
        assert_eq!(copied.owner, "cloner");

        let state = catalog
            .get_sequence_state(TARGET.as_u64(), TENANT, "chunks_id_seq")
            .expect("read sequence state")
            .expect("the counter must land in the target");
        assert_eq!(state.current_value, 4_200);

        assert!(
            catalog
                .get_sequence(SOURCE.as_u64(), TENANT, "chunks_id_seq")
                .expect("read source sequence")
                .is_some(),
            "the source keeps its sequence"
        );
    }

    #[test]
    fn the_source_rows_survive_the_copy_unchanged() {
        let (_dir, catalog) = open();
        seed_source(&catalog);

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        let params = catalog
            .get_vector_index_params(SOURCE.as_u64(), TENANT, "chunks", "embedding")
            .expect("read params")
            .expect("the source keeps its params");
        assert_eq!(params.database_id, SOURCE.as_u64());
        assert_eq!(params.dim, 384);

        let stats = catalog
            .load_column_stats(SOURCE.as_u64(), TENANT, "chunks")
            .expect("read stats");
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].database_id, SOURCE.as_u64());
        assert_eq!(stats[0].row_count, 10_000);

        let records = catalog
            .list_index_records_in_database(SOURCE.as_u64())
            .expect("read index records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].database_id, SOURCE.as_u64());
    }

    /// The clone stamps a descriptor for `chunks`, so its named checkpoints
    /// must reach the target under the target's own key.
    #[test]
    fn checkpoints_follow_the_collection_into_the_clone() {
        use crate::control::security::catalog::types::{CheckpointDoc, CheckpointRecord};

        let (_dir, catalog) = open();
        seed_source(&catalog);
        catalog
            .put_checkpoint(&CheckpointRecord {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                collection: "chunks".into(),
                doc_id: "doc-1".into(),
                checkpoint_name: "launch".into(),
                version_vector_json: "{\"n1\":4}".into(),
                created_by: "cloner".into(),
                created_at: 10,
            })
            .expect("seed checkpoint");

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        let doc = CheckpointDoc::new(TARGET.as_u64(), TENANT, "chunks", "doc-1");
        let copied = catalog
            .get_checkpoint(doc, "launch")
            .expect("read checkpoint")
            .expect("the checkpoint must land in the target");
        assert_eq!(copied.database_id, TARGET.as_u64());
        assert_eq!(copied.version_vector_json, "{\"n1\":4}");

        let source_doc = CheckpointDoc::new(SOURCE.as_u64(), TENANT, "chunks", "doc-1");
        assert!(
            catalog
                .get_checkpoint(source_doc, "launch")
                .expect("read source")
                .is_some(),
            "the source keeps its checkpoint"
        );
    }

    /// The clone stamps the view's target collection, so the definition must
    /// travel with it.
    #[test]
    fn a_materialized_view_definition_reaches_the_clone() {
        let (_dir, catalog) = open();
        seed_source(&catalog);
        seed_owner_paired_objects(&catalog);

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        let copied = catalog
            .get_committed_materialized_view(TARGET.as_u64(), TENANT, "chunk_counts")
            .expect("read view")
            .expect("the definition must land in the target");
        assert_eq!(copied.database_id, TARGET.as_u64());
        assert_eq!(copied.source, "chunks");
        assert!(
            catalog
                .get_committed_materialized_view(SOURCE.as_u64(), TENANT, "chunk_counts")
                .expect("read source")
                .is_some(),
            "the source keeps its definition"
        );
    }

    /// Seed one source object of every kind the integrity check pairs with an
    /// owner row and `copy_scoped_objects` copies.
    fn seed_owner_paired_objects(catalog: &SystemCatalog) {
        use crate::control::security::catalog::StoredContinuousAggregate;
        use crate::control::security::catalog::StoredMaterializedView;
        use crate::control::security::catalog::trigger_types::{
            StoredTrigger, TriggerEvents, TriggerGranularity, TriggerTiming,
        };
        use crate::event::streaming_mv::StreamingMvDef;

        catalog
            .put_trigger(&StoredTrigger {
                tenant_id: TENANT,
                database_id: SOURCE,
                name: "chunks_audit".into(),
                collection: "chunks".into(),
                timing: TriggerTiming::After,
                events: TriggerEvents {
                    on_insert: true,
                    on_update: false,
                    on_delete: false,
                },
                granularity: TriggerGranularity::Row,
                when_condition: None,
                body_sql: "BEGIN END".into(),
                priority: 0,
                enabled: true,
                execution_mode: Default::default(),
                security: Default::default(),
                batch_mode: Default::default(),
                owner: "cloner".into(),
                created_at: 0,
                descriptor_version: 1,
                modification_hlc: Default::default(),
            })
            .expect("seed trigger");

        catalog
            .put_materialized_view(&StoredMaterializedView {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                name: "chunk_counts".into(),
                source: "chunks".into(),
                query_sql: "SELECT count(*) FROM chunks".into(),
                refresh_mode: "auto".into(),
                owner: "cloner".into(),
                created_at: 0,
                descriptor_version: 1,
                modification_hlc: Default::default(),
            })
            .expect("seed materialized view");

        catalog
            .put_continuous_aggregate(&StoredContinuousAggregate {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                name: "chunks_hourly".into(),
                source: "chunks".into(),
                def_bytes: Vec::new(),
                owner: "cloner".into(),
                created_at: 0,
                descriptor_version: 1,
                modification_hlc: Default::default(),
            })
            .expect("seed continuous aggregate");

        catalog
            .put_streaming_mv(&StreamingMvDef {
                database_id: SOURCE,
                tenant_id: TENANT,
                name: "chunks_live".into(),
                source_stream: "chunks_stream".into(),
                group_by_columns: Vec::new(),
                aggregates: Vec::new(),
                filter_expr: None,
                owner: "cloner".into(),
                created_at: 0,
            })
            .expect("seed streaming mv");
    }

    /// Every copied object the integrity check pairs with an owner gets that
    /// row under the target database, so DDL authorization works on the clone
    /// without waiting for a later pass.
    #[test]
    fn every_owner_paired_copy_lands_its_owner_row() {
        let (_dir, catalog) = open();
        seed_source(&catalog);
        seed_owner_paired_objects(&catalog);

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        let owners = catalog.load_all_owners().expect("read owners");
        for (kind, name) in [
            (object_type::TRIGGER, "chunks_audit"),
            (object_type::MATERIALIZED_VIEW, "chunk_counts"),
            (object_type::CONTINUOUS_AGGREGATE, "chunks_hourly"),
            (object_type::STREAMING_MATERIALIZED_VIEW, "chunks_live"),
        ] {
            assert!(
                owners
                    .iter()
                    .any(|owner| owner.database_id == TARGET.as_u64()
                        && owner.object_type == kind
                        && owner.object_name == name
                        && owner.tenant_id == TENANT
                        && owner.owner_username == "cloner"),
                "{kind} '{name}' must carry an owner row in the target database"
            );
        }
    }

    /// Synonym groups and custom types stay with the source here. Each needs
    /// the in-memory registry and, for a group, the per-node FTS backend —
    /// neither of which this module can reach. The clone handler proposes a
    /// catalog entry per row instead, and the clone handler's own test covers
    /// that they arrive.
    #[test]
    fn synonym_groups_and_custom_types_stay_with_the_source() {
        use crate::control::security::catalog::{
            CustomTypeDef, StoredCustomType, StoredSynonymGroup,
        };

        let (_dir, catalog) = open();
        seed_source(&catalog);
        catalog
            .put_synonym_group(&StoredSynonymGroup {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                name: "db_terms".into(),
                terms: vec!["database".into(), "db".into()],
                created_at: 5,
                modification_hlc: nodedb_types::Hlc::ZERO,
            })
            .expect("seed synonym group");
        catalog
            .put_custom_type(&StoredCustomType {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                name: "mood".into(),
                def: CustomTypeDef::Enum {
                    labels: vec!["joy".into(), "anger".into()],
                },
                oid: 70_005,
                created_at: 5,
            })
            .expect("seed custom type");

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        assert!(
            catalog
                .get_synonym_group(TARGET.as_u64(), TENANT, "db_terms")
                .expect("read target group")
                .is_none(),
            "a catalog-only copy would list a group whose expansion does nothing"
        );
        assert!(
            catalog
                .get_custom_type(TARGET.as_u64(), TENANT, "mood")
                .expect("read target type")
                .is_none(),
            "a catalog-only copy would leave the type unresolvable until restart"
        );
        assert!(
            catalog
                .get_synonym_group(SOURCE.as_u64(), TENANT, "db_terms")
                .expect("read source group")
                .is_some(),
            "the source keeps its group"
        );
    }

    /// A database the clone never touched keeps an empty metadata set.
    #[test]
    fn an_unrelated_database_gains_nothing() {
        let (_dir, catalog) = open();
        seed_source(&catalog);

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        let other = DatabaseId::new(9);
        assert!(
            catalog
                .list_vector_index_params_in_database(other.as_u64())
                .expect("read params")
                .is_empty()
        );
        assert!(
            catalog
                .load_column_stats_in_database(other.as_u64())
                .expect("read stats")
                .is_empty()
        );
    }
}
