// SPDX-License-Identifier: BUSL-1.1

//! Session-scoped sequence accessor evaluation for the response shaper.
//!
//! A SELECT-list accessor over a relation runs once per output row on the
//! Control Plane, after the Data Plane returns the rows. Each call resolves
//! against the node registry and the calling session's `currval` map with
//! the same semantics the plan-time catalog adapter applies to a constant
//! `SELECT nextval('s')`.

use std::sync::Arc;

use nodedb_types::{DatabaseId, TenantId};

use super::error_map::sequence_error_to_error;
use super::registry::SequenceRegistry;
use super::session_values::SessionSequenceValues;
use crate::control::state::SharedState;

/// Session-scoped sequence accessor evaluation for the response shaper.
pub trait SequenceAccess {
    /// Advance `name` and record the value as this session's `currval`.
    fn nextval(&self, name: &str) -> crate::Result<i64>;
    /// The last value THIS SESSION obtained from `nextval` on `name`.
    fn currval(&self, name: &str) -> crate::Result<i64>;
    /// Position `name` so the next `nextval` returns `value + increment`.
    fn setval(&self, name: &str, value: i64) -> crate::Result<i64>;
}

/// [`SequenceAccess`] over one session's view of the node registry.
///
/// `session` is `None` on a transport with no session state (HTTP): `nextval`
/// still advances the registry, and `currval` reports that this session never
/// called `nextval`, matching the plan-time adapter's behavior for the same
/// caller.
pub struct SessionSequenceAccess<'a> {
    registry: &'a SequenceRegistry,
    session: Option<Arc<SessionSequenceValues>>,
    database_id: DatabaseId,
    tenant_id: TenantId,
}

impl<'a> SessionSequenceAccess<'a> {
    pub fn new(
        registry: &'a SequenceRegistry,
        session: Option<Arc<SessionSequenceValues>>,
        database_id: DatabaseId,
        tenant_id: TenantId,
    ) -> Self {
        Self {
            registry,
            session,
            database_id,
            tenant_id,
        }
    }

    /// Build over `state`'s sequence registry for one request's session,
    /// database, and tenant scope. Every call site (pgwire task/finish,
    /// native `sql_loop`, HTTP materialized/ndjson) derives its accessor
    /// from these same four inputs; this is the one constructor for that.
    pub fn for_session(
        state: &'a SharedState,
        session: Option<Arc<SessionSequenceValues>>,
        database_id: DatabaseId,
        tenant_id: TenantId,
    ) -> Self {
        Self::new(&state.sequence_registry, session, database_id, tenant_id)
    }
}

impl SequenceAccess for SessionSequenceAccess<'_> {
    fn nextval(&self, name: &str) -> crate::Result<i64> {
        let database_id = self.database_id.as_u64();
        let tenant_id = self.tenant_id.as_u64();
        let value = self
            .registry
            .nextval(database_id, tenant_id, name)
            .map_err(|e| sequence_error_to_error(name, e))?;
        if let Some(session) = &self.session {
            session.record(database_id, tenant_id, name, value);
        }
        Ok(value)
    }

    fn currval(&self, name: &str) -> crate::Result<i64> {
        let database_id = self.database_id.as_u64();
        let tenant_id = self.tenant_id.as_u64();
        // The sequence must exist before its absence from the session map can
        // mean "not yet called in this session" rather than "no such object".
        if !self.registry.exists(database_id, tenant_id, name) {
            return Err(crate::Error::UndefinedObject {
                kind: "sequence",
                name: name.to_string(),
            });
        }
        self.session
            .as_ref()
            .and_then(|session| session.last(database_id, tenant_id, name))
            .ok_or_else(|| crate::Error::ObjectNotInPrerequisiteState {
                object: name.to_string(),
                detail: format!(
                    "currval of sequence \"{name}\" is not yet defined in this session"
                ),
            })
    }

    fn setval(&self, name: &str, value: i64) -> crate::Result<i64> {
        self.registry
            .setval(
                self.database_id.as_u64(),
                self.tenant_id.as_u64(),
                name,
                value,
            )
            .map_err(|e| sequence_error_to_error(name, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::sequence_types::StoredSequence;

    const DB: DatabaseId = DatabaseId::DEFAULT;
    const TENANT: TenantId = TenantId::new(7);

    fn registry_with(name: &str) -> SequenceRegistry {
        let registry = SequenceRegistry::new();
        registry
            .create(StoredSequence::new(
                DB.as_u64(),
                TENANT.as_u64(),
                name.to_string(),
                "alice".to_string(),
            ))
            .expect("create sequence");
        registry
    }

    #[test]
    fn nextval_records_currval_for_the_session() {
        let registry = registry_with("s");
        let session = Arc::new(SessionSequenceValues::new());
        let access = SessionSequenceAccess::new(&registry, Some(Arc::clone(&session)), DB, TENANT);
        let first = access.nextval("s").expect("nextval");
        assert_eq!(access.currval("s").expect("currval"), first);
        let second = access.nextval("s").expect("nextval");
        assert!(second > first);
        assert_eq!(access.currval("s").expect("currval"), second);
    }

    #[test]
    fn currval_before_nextval_is_prerequisite_state() {
        let registry = registry_with("s");
        let access = SessionSequenceAccess::new(
            &registry,
            Some(Arc::new(SessionSequenceValues::new())),
            DB,
            TENANT,
        );
        assert!(matches!(
            access.currval("s"),
            Err(crate::Error::ObjectNotInPrerequisiteState { .. })
        ));
    }

    #[test]
    fn unknown_sequence_is_undefined_object() {
        let registry = registry_with("s");
        let access = SessionSequenceAccess::new(&registry, None, DB, TENANT);
        assert!(matches!(
            access.nextval("missing"),
            Err(crate::Error::UndefinedObject {
                kind: "sequence",
                ..
            })
        ));
        assert!(matches!(
            access.currval("missing"),
            Err(crate::Error::UndefinedObject {
                kind: "sequence",
                ..
            })
        ));
        assert!(matches!(
            access.setval("missing", 5),
            Err(crate::Error::UndefinedObject {
                kind: "sequence",
                ..
            })
        ));
    }

    #[test]
    fn setval_positions_the_next_value() {
        let registry = registry_with("s");
        let access = SessionSequenceAccess::new(&registry, None, DB, TENANT);
        access.setval("s", 41).expect("setval");
        assert_eq!(access.nextval("s").expect("nextval"), 42);
    }
}
