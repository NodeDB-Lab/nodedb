use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // Triggers: CREATE [OR REPLACE] [SYNC|DEFERRED] TRIGGER ...
    if upper.starts_with("CREATE TRIGGER ")
        || upper.starts_with("CREATE OR REPLACE TRIGGER ")
        || upper.starts_with("CREATE SYNC TRIGGER ")
        || upper.starts_with("CREATE DEFERRED TRIGGER ")
        || upper.starts_with("CREATE OR REPLACE SYNC TRIGGER ")
        || upper.starts_with("CREATE OR REPLACE DEFERRED TRIGGER ")
    {
        return Some(super::super::trigger::create_trigger(state, identity, sql));
    }
    if upper.starts_with("DROP TRIGGER ") {
        return Some(super::super::trigger::drop_trigger(state, identity, parts));
    }
    if upper.starts_with("ALTER TRIGGER ") {
        return Some(super::super::trigger::alter_trigger(state, identity, parts));
    }
    if upper == "SHOW TRIGGERS" || upper.starts_with("SHOW TRIGGERS") {
        return Some(super::super::trigger::show_triggers(state, identity, parts));
    }

    // Schema introspection.
    if upper.starts_with("DESCRIBE SEQUENCE ") {
        let name = parts.get(2).unwrap_or(&"");
        return Some(super::super::sequence::describe_sequence(
            state, identity, name,
        ));
    }
    if upper.starts_with("DESCRIBE ") || upper.starts_with("\\D ") {
        return Some(super::super::collection::describe_collection(
            state, identity, parts,
        ));
    }

    // Collection management.
    if upper.starts_with("CREATE COLLECTION ") {
        let result = super::super::collection::create_collection(state, identity, parts, sql);
        // If creation succeeded, register the collection's storage mode with the
        // Data Plane so it knows how to encode/decode documents (MessagePack vs
        // Binary Tuple). This must happen after the catalog persist so the collection
        // metadata is durable.
        if result.is_ok() {
            super::super::collection::dispatch_register_if_needed(state, identity, parts, sql)
                .await;
        }
        return Some(result);
    }
    if upper.starts_with("DROP COLLECTION ") {
        return Some(super::super::collection::drop_collection(
            state, identity, parts,
        ));
    }
    if upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
        return Some(super::super::collection::show_collections(state, identity));
    }

    if upper.starts_with("CREATE INDEX ") || upper.starts_with("CREATE UNIQUE INDEX ") {
        return Some(super::super::collection::create_index(
            state, identity, parts, sql,
        ));
    }
    if upper.starts_with("DROP INDEX ") {
        return Some(super::super::collection::drop_index(state, identity, parts));
    }
    if upper.starts_with("SHOW INDEXES") || upper.starts_with("SHOW INDEX") {
        return Some(super::super::collection::show_indexes(
            state, identity, parts,
        ));
    }

    // ALTER TABLE ADD COLUMN — schema modification for strict/columnar collections.
    if upper.starts_with("ALTER TABLE ") && (upper.contains("ADD COLUMN") || upper.contains("ADD "))
    {
        return Some(
            super::super::collection::alter_table_add_column(state, identity, parts, sql).await,
        );
    }

    // ALTER COLLECTION ADD COLUMN — same catalog-generic handler, but only
    // when this isn't a specialised variant (MATERIALIZED_SUM is routed
    // earlier via the collaborative dispatcher).
    if upper.starts_with("ALTER COLLECTION ")
        && upper.contains("ADD COLUMN")
        && !upper.contains("MATERIALIZED_SUM")
    {
        return Some(
            super::super::collection::alter_table_add_column(state, identity, parts, sql).await,
        );
    }

    // ALTER COLLECTION DROP COLUMN — strict-schema column removal.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("DROP COLUMN") {
        return Some(
            super::super::collection::alter_collection_drop_column(state, identity, parts, sql)
                .await,
        );
    }

    // ALTER COLLECTION RENAME COLUMN — metadata-only rename.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("RENAME COLUMN") {
        return Some(
            super::super::collection::alter_collection_rename_column(state, identity, parts, sql)
                .await,
        );
    }

    // ALTER COLLECTION ALTER COLUMN ... TYPE ... — type alias change.
    if upper.starts_with("ALTER COLLECTION ")
        && upper.contains("ALTER COLUMN")
        && upper.contains(" TYPE ")
    {
        return Some(
            super::super::collection::alter_collection_alter_column_type(
                state, identity, parts, sql,
            )
            .await,
        );
    }

    // RLS policies.
    if upper.starts_with("CREATE RLS POLICY ") {
        return Some(super::super::rls::create_rls_policy(state, identity, parts));
    }
    if upper.starts_with("DROP RLS POLICY ") {
        return Some(super::super::rls::drop_rls_policy(state, identity, parts));
    }
    if upper.starts_with("SHOW RLS POLICIES") || upper.starts_with("SHOW RLS POLICY") {
        return Some(super::super::rls::show_rls_policies(state, identity, parts));
    }

    // Ownership transfer.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("OWNER TO") {
        return Some(super::super::ownership::alter_collection_owner(
            state, identity, parts,
        ));
    }

    // Retention and legal hold management.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("SET RETENTION") {
        return Some(super::super::collection::alter_collection_enforcement(
            state,
            identity,
            sql,
            "retention",
        ));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("LAST_VALUE_CACHE") {
        return Some(super::super::collection::alter_collection_enforcement(
            state,
            identity,
            sql,
            "last_value_cache",
        ));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("LEGAL_HOLD") {
        return Some(super::super::collection::alter_collection_enforcement(
            state,
            identity,
            sql,
            "legal_hold",
        ));
    }

    // Set append-only (one-way).
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("SET APPEND_ONLY") {
        return Some(super::super::collection::alter_collection_enforcement(
            state,
            identity,
            sql,
            "append_only",
        ));
    }

    // DEFINE FIELD <name> ON <collection> [TYPE <type>] [DEFAULT <expr>] [VALUE <expr>] [ASSERT <expr>] [READONLY]
    if upper.starts_with("DEFINE FIELD ") {
        return Some(super::super::field_def::define_field(state, identity, sql));
    }

    // DEFINE EVENT <name> ON <collection> WHEN <condition> THEN <action>
    if upper.starts_with("DEFINE EVENT ") {
        return Some(super::super::field_def::define_event(state, identity, sql));
    }

    // CREATE SPATIAL INDEX
    if upper.starts_with("CREATE SPATIAL INDEX ") {
        return Some(super::super::spatial::create_spatial_index(
            state, identity, parts,
        ));
    }

    None
}
