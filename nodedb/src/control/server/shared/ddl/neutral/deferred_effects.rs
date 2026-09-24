// SPDX-License-Identifier: BUSL-1.1

//! Run the engine side effects a transaction's index DDL deferred to COMMIT.
//!
//! COMMIT calls [`run_deferred_effects`] once the buffered catalog entries
//! landed, in statement order. Each effect runs through the same function its
//! statement runs in autocommit, so a transactional index ends in the same
//! engine state as an autocommit one.

use crate::control::server::shared::session::ddl_effect::DeferredDdlEffect;
use crate::control::state::SharedState;

use super::super::result::DdlError;
use super::collection::index::build::build_secondary_index;
use super::collection::index::kv_index::drop_kv_index;
use super::collection::index::teardown;
use super::kv_sorted_index::SortedIndexTarget;
use super::kv_sorted_index::dispatch::register_in_engine;
use super::kv_sorted_index::drop_in_engine;

/// Run every effect in order. The first failure stops the run and returns
/// its error: the effects before it applied, and the ones after it did not.
pub(crate) async fn run_deferred_effects(
    state: &SharedState,
    effects: Vec<DeferredDdlEffect>,
) -> crate::Result<()> {
    for effect in effects {
        let collection = effect_collection(&effect).to_string();
        run_one(state, effect)
            .await
            .map_err(|error| effect_error(&collection, error))?;
    }
    Ok(())
}

/// The collection an effect changes.
fn effect_collection(effect: &DeferredDdlEffect) -> &str {
    match effect {
        DeferredDdlEffect::SecondaryIndexBuild(build) => &build.collection,
        DeferredDdlEffect::EngineApply { collection, .. }
        | DeferredDdlEffect::IndexTeardown { collection, .. }
        | DeferredDdlEffect::SortedIndexRegister { collection, .. }
        | DeferredDdlEffect::SortedIndexDrop { collection, .. }
        | DeferredDdlEffect::KvIndexDrop { collection, .. } => collection,
    }
}

async fn run_one(state: &SharedState, effect: DeferredDdlEffect) -> Result<(), DdlError> {
    match effect {
        DeferredDdlEffect::SecondaryIndexBuild(build) => build_secondary_index(state, &build).await,
        DeferredDdlEffect::EngineApply {
            tenant_id,
            database_id,
            collection,
            plan,
            sqlstate,
            context,
        } => {
            crate::control::server::shared::ddl::engine_apply::apply_in_engine(
                state,
                tenant_id,
                database_id,
                &collection,
                plan,
                &sqlstate,
                &context,
            )
            .await
        }
        DeferredDdlEffect::IndexTeardown {
            tenant_id,
            database_id,
            collection,
            plan,
        } => teardown::dispatch(state, tenant_id, database_id, &collection, plan, None).await,
        DeferredDdlEffect::SortedIndexRegister {
            tenant_id,
            database_id,
            collection,
            plan,
        } => {
            let target = SortedIndexTarget {
                tenant_id,
                database_id,
                collection: &collection,
            };
            register_in_engine(state, &target, plan, "CREATE SORTED INDEX")
                .await
                .map(|_| ())
        }
        DeferredDdlEffect::SortedIndexDrop {
            tenant_id,
            database_id,
            collection,
            index_name,
        } => {
            let target = SortedIndexTarget {
                tenant_id,
                database_id,
                collection: &collection,
            };
            drop_in_engine(state, &target, &index_name).await
        }
        DeferredDdlEffect::KvIndexDrop {
            tenant_id,
            database_id,
            collection,
            field,
        } => drop_kv_index(state, tenant_id, database_id, &collection, &field).await,
    }
}

/// The COMMIT error for a failed effect. A UNIQUE violation keeps its class,
/// so the client sees SQLSTATE 23505 as an autocommit `CREATE INDEX` does.
fn effect_error(collection: &str, error: DdlError) -> crate::Error {
    if error.sqlstate == "23505" {
        return crate::Error::RejectedConstraint {
            collection: collection.to_string(),
            constraint: "unique".to_string(),
            detail: error.message,
        };
    }
    crate::Error::Internal {
        detail: format!(
            "index DDL committed but its engine step failed (SQLSTATE {}): {}",
            error.sqlstate, error.message
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_unique_violation_keeps_its_class() {
        let error = effect_error("users", DdlError::new("23505", "duplicate 'a'"));
        assert!(matches!(
            error,
            crate::Error::RejectedConstraint { ref constraint, .. } if constraint == "unique"
        ));
        let other = effect_error("users", DdlError::new("XX000", "core gone"));
        assert!(matches!(other, crate::Error::Internal { .. }));
    }
}
