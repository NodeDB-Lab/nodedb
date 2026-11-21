// SPDX-License-Identifier: BUSL-1.1

//! Propose the document index builds and retires a committed collection
//! descriptor owes its data log.
//!
//! Each Data Plane core maintains the index paths its maintained set holds.
//! The set changes only when a `DocIndexBuild` or `DocIndexRetire` applies
//! from the collection's data log. The descriptor alone never changes it: a
//! descriptor reaches each replica in metadata-log order, which stands apart
//! from the data log on every replica.
//!
//! A descriptor change therefore proposes the steps on the data log after it
//! commits, as the CRDT constraint install does:
//!
//! - A schemaless collection maintains every top-level key. A new
//!   incarnation builds that mode once, and a declared field added or
//!   removed proposes nothing.
//! - A declared field added to a strict collection builds its auto-index.
//!   A declared field removed retires it.
//! - An explicit index that arrives `Ready` under a name the prior
//!   descriptor did not hold builds it. A restored collection carries its
//!   indexes this way. A `CREATE INDEX` commits its index `Building` first
//!   and builds it itself.
//!
//! A new incarnation starts from no maintained path: its apply cleared the
//! name's storage. Each step is idempotent on every replica, so a step two
//! descriptors both owe applies once. A build whose owner a replica already
//! holds with the same spec reads no row there.
//!
//! The DDL is committed when the steps run, so a failed step never fails
//! it. The reconcile loop on the metadata leader delivers every step a
//! committed descriptor owes, which also covers a crash or a leadership
//! change between the commit and the proposal.

use std::future::Future;
use std::pin::Pin;

use nodedb_physical::physical_plan::{
    DocIndexFence, DocIndexOwner, DocumentOp, PhysicalPlan, TOP_LEVEL_BUILD_PATH,
};
use nodedb_types::{Hlc, QualifiedCollection};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::{IndexBuildState, StoredCollection, SystemCatalog};
use crate::control::server::shared::ddl::neutral::collection::index::engine_step::{
    IndexStepTarget, propose_index_step_as,
};
use crate::control::server::shared::ddl::neutral::collection::register::derive_auto_indexes;
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId};

/// The index state of a committed descriptor that the steps diff against.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct DocIndexShape {
    incarnation: Hlc,
    /// Whether every top-level key is maintained, as a schemaless
    /// collection's declared owner.
    top_level: bool,
    /// The extraction paths the declared fields of a strict collection
    /// derive.
    declared: Vec<String>,
    /// The names of the explicit indexes.
    named: Vec<String>,
}

/// A collection whose descriptor a DDL changes, with its committed shape
/// before the change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DocIndexTarget {
    database_id: DatabaseId,
    tenant_id: u64,
    name: String,
    /// `None` when no committed descriptor held the name.
    prior: Option<DocIndexShape>,
}

/// One step a target owes.
#[derive(Debug, Clone, PartialEq)]
enum DocIndexStep {
    Build {
        path: String,
        is_array: bool,
        unique: bool,
        case_insensitive: bool,
        predicate: Option<String>,
        owner: DocIndexOwner,
    },
    Retire {
        path: String,
        owner: DocIndexOwner,
    },
}

/// The collections among stamped, not yet applied `entries` that a
/// descriptor entry changes. Call it after the stamp and before the entries
/// apply, so the committed row is the prior one. Each collection is listed
/// once, with the shape before the first entry.
pub(crate) fn doc_index_targets(
    catalog: &SystemCatalog,
    entries: &[CatalogEntry],
) -> crate::Result<Vec<DocIndexTarget>> {
    let mut targets: Vec<DocIndexTarget> = Vec::new();
    for entry in entries {
        let stored = match entry {
            CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
                stored
            }
            _ => continue,
        };
        let listed = targets.iter().any(|t| {
            t.database_id == stored.database_id
                && t.tenant_id == stored.tenant_id
                && t.name == stored.name
        });
        if listed {
            continue;
        }
        let prior = catalog
            .get_committed_collection(stored.database_id, stored.tenant_id, &stored.name)?
            .map(|prior| shape_of(&prior));
        targets.push(DocIndexTarget {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
            prior,
        });
    }
    Ok(targets)
}

/// Propose every step the targets owe, against each target's committed
/// descriptor. Call it after the DDL applied. A target the DDL left absent
/// or inactive owes nothing: its purge clears the maintained set.
///
/// The DDL is committed when this runs, so a failed step never reaches the
/// caller. The error goes to the recorder and the log. The collection keeps
/// the paths it maintained before, a read through a path it lacks is
/// refused as retryable, and the reconcile loop delivers the step.
///
/// Returns a boxed future: DDL reaches this from inside the gateway's own
/// statement routing, and the box gives that cycle a finite size.
pub(crate) fn install_doc_indexes<'a>(
    shared: &'a SharedState,
    targets: &'a [DocIndexTarget],
) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        for target in targets {
            if let Err(error) = install_target(shared, target).await {
                tracing::warn!(
                    collection = %target.name,
                    tenant_id = target.tenant_id,
                    %error,
                    "document index step after a committed DDL failed; \
                     the reconcile loop delivers it"
                );
                crate::diag::doc_index_step_deferred(
                    &error,
                    &crate::diag::DocIndexStepTarget {
                        database_id: target.database_id.as_u64(),
                        tenant_id: target.tenant_id,
                        collection: &target.name,
                    },
                );
            }
        }
    })
}

/// Propose every step committed `coll` owes from no maintained path, as the
/// reconcile loop delivers them: the top-level build of a schemaless
/// collection, each declared path of a strict one, and each `Ready` explicit
/// index. A replica that holds a step's owner with the same spec reads no
/// row for it. Returns how many steps it proposed.
///
/// Returns a boxed future for the same reason as [`install_doc_indexes`].
pub(crate) fn deliver_doc_indexes<'a>(
    shared: &'a SharedState,
    coll: &'a StoredCollection,
) -> Pin<Box<dyn Future<Output = crate::Result<usize>> + Send + 'a>> {
    Box::pin(async move {
        if !coll.is_active {
            return Ok(0);
        }
        let steps = steps_for(None, coll);
        let count = steps.len();
        propose_steps(shared, coll, steps, EventSource::User).await?;
        Ok(count)
    })
}

async fn install_target(shared: &SharedState, target: &DocIndexTarget) -> crate::Result<()> {
    let catalog = shared.credentials.catalog();
    let Some(stored) =
        catalog.get_committed_collection(target.database_id, target.tenant_id, &target.name)?
    else {
        return Ok(());
    };
    if !stored.is_active {
        return Ok(());
    }
    let steps = steps_for(target.prior.as_ref(), &stored);
    propose_steps(shared, &stored, steps, EventSource::User).await
}

/// Build every document index `coll` declares, as a collection whose rows
/// arrive after the builds. A mirror's bootstrap calls this before it
/// re-issues the rows of a collection its catalog image installed, with the
/// follower source its write gate admits.
pub(crate) async fn build_doc_indexes_of(
    shared: &SharedState,
    coll: &StoredCollection,
    event_source: EventSource,
) -> crate::Result<()> {
    if !coll.is_active {
        return Ok(());
    }
    propose_steps(shared, coll, steps_for(None, coll), event_source).await
}

/// Propose `steps` on `coll`'s data log and wait until this node applied
/// each.
async fn propose_steps(
    shared: &SharedState,
    coll: &StoredCollection,
    steps: Vec<DocIndexStep>,
    event_source: EventSource,
) -> crate::Result<()> {
    if steps.is_empty() {
        return Ok(());
    }
    crate::fail_point_err!("doc_index::before_propose", |detail: String| {
        crate::Error::Internal { detail }
    });
    let database_id = coll.database_id;
    // Every step derives from `coll`, so all carry its fence.
    let fence = doc_index_fence(coll);
    let proposals = steps.into_iter().map(|step| {
        let (plan, context) = step_plan(database_id, &coll.name, step, fence);
        async move {
            propose_index_step_as(
                shared,
                IndexStepTarget {
                    tenant_id: TenantId::new(coll.tenant_id),
                    database_id,
                    collection: &coll.name,
                    event_source,
                },
                plan,
                context,
            )
            .await
            .map(|_| ())
            .map_err(crate::Error::from)
        }
    });
    futures::future::try_join_all(proposals).await?;
    Ok(())
}

/// The fence of every document index step `coll` derives: its incarnation
/// and descriptor version. Every proposer of a step stamps this.
pub(crate) fn doc_index_fence(coll: &StoredCollection) -> DocIndexFence {
    DocIndexFence {
        incarnation: coll.incarnation,
        descriptor_version: coll.descriptor_version,
    }
}

/// The shape of `coll` the steps diff against. A collection whose rows do
/// not live in the document engine maintains no document index. A
/// schemaless collection maintains every top-level key in place of its
/// declared fields.
fn shape_of(coll: &StoredCollection) -> DocIndexShape {
    if !coll.collection_type.is_document() {
        return DocIndexShape {
            incarnation: coll.incarnation,
            ..DocIndexShape::default()
        };
    }
    let top_level = coll.collection_type.is_schemaless();
    let declared = if top_level {
        Vec::new()
    } else {
        derive_auto_indexes(coll.fields.iter().map(|(n, _)| n.as_str()))
            .into_iter()
            .map(|index| index.path)
            .collect()
    };
    DocIndexShape {
        incarnation: coll.incarnation,
        top_level,
        declared,
        named: coll
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .collect(),
    }
}

/// The steps that take a collection from `prior` to `stored`.
fn steps_for(prior: Option<&DocIndexShape>, stored: &StoredCollection) -> Vec<DocIndexStep> {
    let after = shape_of(stored);
    let empty = DocIndexShape::default();
    // A new incarnation's storage starts empty on every replica.
    let before = prior
        .filter(|prior| prior.incarnation == after.incarnation)
        .unwrap_or(&empty);
    // The steps apply in any order on the data log, and every order ends at
    // the same entries: a path a stored owner decides keeps its entries
    // across a top-level build or retire.
    let mut steps = Vec::new();
    if after.top_level && !before.top_level {
        steps.push(DocIndexStep::Build {
            path: TOP_LEVEL_BUILD_PATH.to_string(),
            is_array: false,
            unique: false,
            case_insensitive: false,
            predicate: None,
            owner: DocIndexOwner::DeclaredTopLevel,
        });
    }
    if before.top_level && !after.top_level {
        steps.push(DocIndexStep::Retire {
            path: TOP_LEVEL_BUILD_PATH.to_string(),
            owner: DocIndexOwner::DeclaredTopLevel,
        });
    }
    for path in after
        .declared
        .iter()
        .filter(|p| !before.declared.contains(p))
    {
        steps.push(DocIndexStep::Build {
            path: path.clone(),
            is_array: false,
            unique: false,
            case_insensitive: false,
            predicate: None,
            owner: DocIndexOwner::Declared,
        });
    }
    for path in before
        .declared
        .iter()
        .filter(|p| !after.declared.contains(p))
    {
        steps.push(DocIndexStep::Retire {
            path: path.clone(),
            owner: DocIndexOwner::Declared,
        });
    }
    if stored.collection_type.is_document() {
        for index in stored.indexes.iter().filter(|index| {
            index.state == IndexBuildState::Ready && !before.named.contains(&index.name)
        }) {
            let is_array = index.field.ends_with("[]");
            let path = index
                .field
                .strip_suffix("[]")
                .unwrap_or(&index.field)
                .to_string();
            steps.push(DocIndexStep::Build {
                path,
                is_array,
                unique: index.unique,
                case_insensitive: index.case_insensitive,
                predicate: index.predicate.clone(),
                owner: DocIndexOwner::Named(index.name.clone()),
            });
        }
    }
    steps
}

/// The plan of one step, and the context its errors carry.
fn step_plan(
    database_id: DatabaseId,
    collection: &str,
    step: DocIndexStep,
    fence: DocIndexFence,
) -> (PhysicalPlan, &'static str) {
    let collection = QualifiedCollection::new(database_id, collection);
    match step {
        DocIndexStep::Build {
            path,
            is_array,
            unique,
            case_insensitive,
            predicate,
            owner,
        } => (
            PhysicalPlan::Document(DocumentOp::BackfillIndex {
                collection,
                path,
                is_array,
                unique,
                case_insensitive,
                predicate,
                owner,
                fence,
            }),
            "index build of a committed collection",
        ),
        DocIndexStep::Retire { path, owner } => (
            PhysicalPlan::Document(DocumentOp::DropIndex {
                collection,
                field: path,
                owner,
                fence,
            }),
            "index retire of a committed collection",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredIndex;

    /// A strict collection declaring `a` and `b`.
    fn docs() -> StoredCollection {
        use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

        let mut coll = StoredCollection::new(1, "docs", "owner");
        coll.collection_type = nodedb_types::CollectionType::strict(
            StrictSchema::new(vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::nullable("a", ColumnType::String),
                ColumnDef::nullable("b", ColumnType::String),
            ])
            .expect("schema"),
        );
        coll.fields = vec![("a".into(), "any".into()), ("b".into(), "any".into())];
        coll
    }

    /// A schemaless collection declaring `a` and `b`.
    fn schemaless() -> StoredCollection {
        let mut coll = StoredCollection::new(1, "docs", "owner");
        coll.fields = vec![("a".into(), "any".into()), ("b".into(), "any".into())];
        coll
    }

    #[test]
    fn a_new_schemaless_collection_builds_one_top_level_step() {
        match steps_for(None, &schemaless()).as_slice() {
            [DocIndexStep::Build { path, owner, .. }] => {
                assert_eq!(path, TOP_LEVEL_BUILD_PATH);
                assert_eq!(owner, &DocIndexOwner::DeclaredTopLevel);
            }
            other => panic!("expected one top-level build, got {other:?}"),
        }
    }

    #[test]
    fn a_schemaless_field_change_proposes_nothing() {
        let prior = shape_of(&schemaless());
        let mut after = schemaless();
        after.fields = vec![("c".into(), "any".into())];
        assert!(steps_for(Some(&prior), &after).is_empty());
    }

    #[test]
    fn a_conversion_moves_between_top_level_and_declared_paths() {
        let to_strict = steps_for(Some(&shape_of(&schemaless())), &docs());
        assert_eq!(
            paths(&to_strict),
            vec![
                (TOP_LEVEL_BUILD_PATH.to_string(), false),
                ("$.a".to_string(), true),
                ("$.b".to_string(), true),
            ]
        );
        let to_schemaless = steps_for(Some(&shape_of(&docs())), &schemaless());
        assert_eq!(
            paths(&to_schemaless),
            vec![
                (TOP_LEVEL_BUILD_PATH.to_string(), true),
                ("$.a".to_string(), false),
                ("$.b".to_string(), false),
            ]
        );
    }

    fn paths(steps: &[DocIndexStep]) -> Vec<(String, bool)> {
        steps
            .iter()
            .map(|step| match step {
                DocIndexStep::Build { path, .. } => (path.clone(), true),
                DocIndexStep::Retire { path, .. } => (path.clone(), false),
            })
            .collect()
    }

    #[test]
    fn a_new_collection_builds_every_declared_field() {
        let steps = steps_for(None, &docs());
        assert_eq!(
            paths(&steps),
            vec![("$.a".to_string(), true), ("$.b".to_string(), true)]
        );
    }

    #[test]
    fn a_field_change_builds_the_added_and_retires_the_removed() {
        let prior = shape_of(&docs());
        let mut after = docs();
        after.fields = vec![("b".into(), "any".into()), ("c".into(), "any".into())];
        assert_eq!(
            paths(&steps_for(Some(&prior), &after)),
            vec![("$.c".to_string(), true), ("$.a".to_string(), false)]
        );
        assert!(steps_for(Some(&prior), &docs()).is_empty());
    }

    #[test]
    fn a_new_incarnation_builds_from_no_path() {
        let prior = shape_of(&docs());
        let mut recreated = docs();
        recreated.incarnation = Hlc::new(prior.incarnation.wall_ns + 1, 0);
        assert_eq!(steps_for(Some(&prior), &recreated).len(), 2);
    }

    #[test]
    fn only_a_ready_index_under_a_new_name_builds() {
        let mut building = docs();
        building.fields.clear();
        building.indexes.push(StoredIndex {
            name: "by_tags".into(),
            field: "$.tags[]".into(),
            unique: false,
            case_insensitive: true,
            predicate: None,
            state: IndexBuildState::Building,
            owner: "owner".into(),
        });
        assert!(steps_for(None, &building).is_empty());

        let prior = shape_of(&building);
        let mut ready = building.clone();
        ready.indexes[0].state = IndexBuildState::Ready;
        assert!(steps_for(Some(&prior), &ready).is_empty());

        match steps_for(None, &ready).as_slice() {
            [
                DocIndexStep::Build {
                    path,
                    is_array,
                    case_insensitive,
                    owner,
                    ..
                },
            ] => {
                assert_eq!(path, "$.tags");
                assert!(*is_array && *case_insensitive);
                assert_eq!(owner, &DocIndexOwner::Named("by_tags".into()));
            }
            other => panic!("expected one named build, got {other:?}"),
        }
    }

    #[test]
    fn a_collection_outside_the_document_engine_owes_nothing() {
        use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

        let mut kv = docs();
        kv.collection_type = nodedb_types::CollectionType::kv(
            StrictSchema::new(vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::nullable("a", ColumnType::String),
            ])
            .expect("schema"),
        );
        assert!(steps_for(None, &kv).is_empty());
    }
}
