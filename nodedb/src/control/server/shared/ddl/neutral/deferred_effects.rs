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
use super::collection::index::text_build::build_text_index;
use super::collection::index::text_drop::finish_text_index_drop;
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
        | DeferredDdlEffect::KvIndexDrop { collection, .. }
        | DeferredDdlEffect::TextIndexBuild { collection, .. }
        | DeferredDdlEffect::TextIndexDrop { collection, .. } => collection,
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
            context,
        } => {
            crate::control::server::shared::ddl::engine_apply::apply_in_engine(
                state,
                tenant_id,
                database_id,
                &collection,
                plan,
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
        DeferredDdlEffect::TextIndexBuild {
            tenant_id,
            database_id,
            collection,
            index_name,
        } => build_text_index(state, tenant_id, database_id, &collection, &index_name).await,
        DeferredDdlEffect::TextIndexDrop {
            tenant_id,
            database_id,
            collection,
            index_name,
        } => finish_text_index_drop(state, tenant_id, database_id, &collection, &index_name).await,
    }
}

/// The COMMIT error for a failed effect. It keeps the effect's SQLSTATE,
/// code, details and cause, the class an autocommit statement reports. The
/// message names the collection.
fn effect_error(collection: &str, error: DdlError) -> crate::Error {
    crate::Error::from(error.in_context(&format!(
        "index DDL on '{collection}' committed, but its engine step failed"
    )))
}

#[cfg(test)]
mod tests {
    use nodedb_types::error::{ErrorCode, sqlstate};

    use super::*;
    use crate::control::server::native::dispatch::native_error_fields;
    use crate::control::server::pgwire::types::error_to_sqlstate;

    /// A failed effect reports its own SQLSTATE and code at COMMIT, on
    /// pgwire and native, with the collection in the message.
    #[test]
    fn a_failed_effect_keeps_its_class_at_commit() {
        let refusals = [
            DdlError::from_error(&crate::Error::DataPlane(
                crate::bridge::envelope::ErrorCode::RejectedConstraint {
                    constraint: "unique".into(),
                    detail: "duplicate 'a'".into(),
                },
            )),
            DdlError::from_error(&crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(1),
                resource: "collection 'users'".into(),
            }),
            DdlError::from_error(&crate::Error::CollectionNotFound {
                tenant_id: crate::types::TenantId::new(1),
                collection: "users".into(),
            }),
            DdlError::from_error(&crate::Error::RoleInUse {
                role: "analyst".into(),
                dependents: crate::control::security::role_assignment::RoleDependents::Users(vec![
                    "bob".into(),
                ]),
            }),
            DdlError::from_error(&crate::Error::DeadlineExceeded {
                request_id: crate::types::RequestId::new(1),
            }),
            DdlError::new("42710", "index 'by_email' already exists"),
        ];
        let expected = [
            (sqlstate::UNIQUE_VIOLATION, ErrorCode::CONSTRAINT_VIOLATION),
            (
                sqlstate::INSUFFICIENT_PRIVILEGE,
                ErrorCode::AUTHORIZATION_DENIED,
            ),
            (sqlstate::UNDEFINED_TABLE, ErrorCode::COLLECTION_NOT_FOUND),
            (
                sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
                ErrorCode::DEPENDENT_OBJECTS_EXIST,
            ),
            ("57014", ErrorCode::DEADLINE_EXCEEDED),
            ("42710", ErrorCode::ALREADY_EXISTS),
        ];
        for (refusal, (state, code)) in refusals.into_iter().zip(expected) {
            let error = effect_error("users", refusal);
            let (_, pg_state, message) = error_to_sqlstate(&error);
            assert_eq!(pg_state, state, "{error:?} on pgwire");
            assert!(
                message.starts_with("index DDL on 'users' committed"),
                "{message}"
            );
            let native = native_error_fields(&error);
            assert_eq!(native.sqlstate, state, "{error:?} native SQLSTATE");
            assert_eq!(native.code, code, "{error:?} native code");
        }
    }

    /// An effect that failed with no typed class stays internal.
    #[test]
    fn an_untyped_effect_failure_stays_internal() {
        let error = effect_error("users", DdlError::internal("core gone"));
        assert_eq!(error_to_sqlstate(&error).1, sqlstate::INTERNAL_ERROR);
        assert_eq!(native_error_fields(&error).code, ErrorCode::INTERNAL);
    }
}
