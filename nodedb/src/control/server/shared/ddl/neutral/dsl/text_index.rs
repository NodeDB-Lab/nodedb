// SPDX-License-Identifier: BUSL-1.1

//! `CREATE SEARCH INDEX` / `CREATE FULLTEXT INDEX` DSL handler.
//!
//! The two keywords are documented as equivalents, so they share one
//! implementation rather than two parsers that drift: they previously
//! disagreed on whether the column list is written `(a, b)` or `FIELDS a, b`,
//! on whether a list may name more than one column, and on whether `ANALYZER`
//! exists at all. Both spellings of the column list are accepted here, and the
//! statement is rejected if any token goes unread.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::IndexKind;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::index_registry::{
    IndexRegistration, propose_index_record,
};
use crate::control::server::shared::session::ddl_buffer;
use crate::control::server::shared::session::ddl_effect::DeferredDdlEffect;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::TextOp;

use super::super::super::result::{DdlError, DdlResult};
use super::options::{
    ColumnMode, HeaderSpec, IndexStatement, NameMode, OptionSpec, parse_index_statement,
};
use super::support::ddl_err;

const SEARCH_COMMAND: &str = "CREATE SEARCH INDEX";
const FULLTEXT_COMMAND: &str = "CREATE FULLTEXT INDEX";

const SEARCH_SYNTAX: &str = "CREATE SEARCH INDEX [IF NOT EXISTS] [<name>] ON <collection> \
     (<field> [, <field>]*) [ANALYZER '<name>'] [FUZZY true|false]";
const FULLTEXT_SYNTAX: &str = "CREATE FULLTEXT INDEX [IF NOT EXISTS] [<name>] ON <collection> \
     (<field> [, <field>]*) [ANALYZER '<name>'] [FUZZY true|false]";

const OPTIONS: &[OptionSpec] = &[OptionSpec::quoted("ANALYZER"), OptionSpec::boolean("FUZZY")];

/// `CREATE SEARCH INDEX ...` — see [`create_text_index`].
pub async fn create_search_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    const HEADER: HeaderSpec = HeaderSpec {
        name: NameMode::Optional {
            fallback: "_auto_search",
        },
        columns: ColumnMode::OneOrMore,
        syntax: SEARCH_SYNTAX,
    };
    let stmt = parse_index_statement(
        sql,
        &["CREATE", "SEARCH", "INDEX"],
        &HEADER,
        OPTIONS,
        SEARCH_COMMAND,
    )?;
    create_text_index(state, identity, database_id, stmt, SEARCH_COMMAND).await
}

/// `CREATE FULLTEXT INDEX ...` — the documented alias of
/// [`create_search_index`], dispatching through the same implementation.
pub async fn create_fulltext_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    const HEADER: HeaderSpec = HeaderSpec {
        name: NameMode::Optional {
            fallback: "_auto_fulltext",
        },
        columns: ColumnMode::OneOrMore,
        syntax: FULLTEXT_SYNTAX,
    };
    let stmt = parse_index_statement(
        sql,
        &["CREATE", "FULLTEXT", "INDEX"],
        &HEADER,
        OPTIONS,
        FULLTEXT_COMMAND,
    )?;
    create_text_index(state, identity, database_id, stmt, FULLTEXT_COMMAND).await
}

/// Record ownership for every named field and bind the collection analyzer.
///
/// `ANALYZER '<name>'` binds the collection's per-collection FTS analyzer
/// (`InvertedIndex::set_collection_analyzer`) — the same analyzer-registry
/// lookup that forward indexing (`index_document_in_txn`), the staged-write
/// overlay (`fts_merge` / `fts_score`), and the base search path all resolve
/// through via `InvertedIndex::analyze_for_collection`.
async fn create_text_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    stmt: IndexStatement,
    command: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let collection = stmt.header.collection.to_lowercase();
    let tenant_id = identity.tenant_id;

    // An analyzer name the registry does not know resolves to the standard
    // analyzer at query time, so a typo silently changes how the collection is
    // tokenized. Reject it here, where the user can still fix the statement.
    let analyzer_name = match stmt.options.text("ANALYZER") {
        Some(name) => {
            let lower = name.to_lowercase();
            if !nodedb_fts::index::analyzer_config::analyzer_exists(&lower) {
                return Err(ddl_err(
                    "42704",
                    format!(
                        "{command}: unknown analyzer '{name}'; use 'standard' or a \
                         supported language name (e.g. 'english', 'german', 'japanese')"
                    ),
                ));
            }
            Some(lower)
        }
        None => None,
    };
    let fuzzy_default = stmt.options.boolean("FUZZY");

    // One index, under the name the statement declared. Synthesizing a
    // per-column name and discarding the declared one would leave
    // `DROP INDEX <the name I typed>` unable to match.
    let index_name = resolve_index_name(&stmt, &collection);
    if let Some(taken) = state
        .credentials
        .catalog()
        .get_index_record(database_id.as_u64(), tenant_id.as_u64(), &index_name)
        .map_err(|e| ddl_err("XX000", format!("{command}: read index registry: {e}")))?
    {
        if stmt.header.if_not_exists && taken.kind == IndexKind::FullText {
            return Ok(vec![DdlResult::Status {
                command: command.to_string(),
                rows_affected: None,
            }]);
        }
        return Err(ddl_err(
            "42710",
            format!(
                "{command}: index '{index_name}' already exists on '{}' ({})",
                taken.collection,
                taken.kind.display_type()
            ),
        ));
    }

    propose_index_record(
        state,
        &IndexRegistration {
            database_id,
            tenant_id,
            name: &index_name,
            kind: IndexKind::FullText,
            collection: &collection,
            fields: stmt.header.columns.clone(),
        },
    )?;
    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        IndexKind::FullText.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        &index_name,
        &identity.username,
    )?;
    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "created text index '{index_name}' on '{collection}' ({})",
            stmt.header.columns.join(", ")
        ),
    );

    if analyzer_name.is_some() || fuzzy_default.is_some() {
        let set_config_plan = PhysicalPlan::Text(TextOp::SetTextConfig {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            analyzer_name: analyzer_name.clone(),
            fuzzy_default,
        });
        // Inside an explicit transaction the binding waits for COMMIT, after
        // the buffered index record lands.
        let deferred = ddl_buffer::defer_effect(DeferredDdlEffect::EngineApply {
            tenant_id,
            database_id,
            collection: collection.clone(),
            plan: set_config_plan.clone(),
            sqlstate: "58000".to_string(),
            context: command.to_string(),
        });
        if !deferred {
            crate::control::server::shared::ddl::engine_apply::apply_in_engine(
                state,
                tenant_id,
                database_id,
                &collection,
                set_config_plan,
                "58000",
                command,
            )
            .await?;
        }

        state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!(
                "configured text index on '{collection}' (analyzer={}, fuzzy={})",
                analyzer_name.as_deref().unwrap_or("unchanged"),
                fuzzy_default
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| "unchanged".to_string()),
            ),
        );
    }

    Ok(vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }])
}

/// The name to register this text index under: the one the statement
/// declared, or a per-collection default when the name was omitted.
///
/// The parser substitutes a fixed placeholder for an omitted name; a
/// placeholder would collide across collections, so it is replaced by a name
/// derived from the collection.
fn resolve_index_name(stmt: &IndexStatement, collection: &str) -> String {
    const PLACEHOLDERS: [&str; 2] = ["_auto_search", "_auto_fulltext"];
    if PLACEHOLDERS.contains(&stmt.header.name.as_str()) {
        format!("fts_{collection}")
    } else {
        stmt.header.name.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEARCH_HEADER: HeaderSpec = HeaderSpec {
        name: NameMode::Optional {
            fallback: "_auto_search",
        },
        columns: ColumnMode::OneOrMore,
        syntax: SEARCH_SYNTAX,
    };

    fn parse(sql: &str) -> Result<IndexStatement, DdlError> {
        parse_index_statement(
            sql,
            &["CREATE", "SEARCH", "INDEX"],
            &SEARCH_HEADER,
            OPTIONS,
            SEARCH_COMMAND,
        )
    }

    #[test]
    fn fields_and_paren_lists_are_the_same_statement() {
        let fields = parse("CREATE SEARCH INDEX ON docs FIELDS title, content").unwrap();
        let parens = parse("CREATE SEARCH INDEX ON docs (title, content)").unwrap();
        assert_eq!(fields.header, parens.header);
        assert_eq!(fields.header.columns, ["title", "content"]);
    }

    #[test]
    fn attached_paren_form_names_the_collection_not_the_column() {
        let stmt = parse("CREATE SEARCH INDEX idx ON docs(content)").unwrap();
        assert_eq!(stmt.header.collection, "docs");
        assert_eq!(stmt.header.columns, ["content"]);
        assert_eq!(stmt.header.name, "idx");
    }

    #[test]
    fn analyzer_must_be_quoted() {
        assert_eq!(
            parse("CREATE SEARCH INDEX ON docs (content) ANALYZER 'english'")
                .unwrap()
                .options
                .text("ANALYZER"),
            Some("english")
        );
        assert!(parse("CREATE SEARCH INDEX ON docs (content) ANALYZER english").is_err());
        assert!(parse("CREATE SEARCH INDEX ON docs (content) ANALYZER ''").is_err());
        assert!(parse("CREATE SEARCH INDEX ON docs (content) ANALYZER 'english").is_err());
    }

    #[test]
    fn a_field_list_is_required() {
        assert!(parse("CREATE SEARCH INDEX ON docs").is_err());
    }

    #[test]
    fn unrecognized_trailing_tokens_are_rejected() {
        assert!(parse("CREATE SEARCH INDEX ON docs (content) WITH (analyzer = 'simple')").is_err());
    }
}
