// SPDX-License-Identifier: BUSL-1.1

//! `CREATE SEARCH INDEX` / `CREATE FULLTEXT INDEX` DSL handler.
//!
//! The two keywords are documented as equivalents, so they share one
//! implementation rather than two parsers that drift on the column list
//! syntax, the column count, and the `ANALYZER` clause. Both spellings of the column list are accepted here, and the
//! statement is rejected if any token goes unread.

use crate::control::security::catalog::{IndexKind, IndexRecordState};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::index_registry::{
    IndexRegistration, index_name_holder, propose_text_index_record,
};
use crate::control::server::shared::ddl::neutral::collection::index::text_build::build_text_index;
use crate::control::server::shared::ddl::neutral::collection::text_analysis::analysis_conflict;
use crate::control::server::shared::session::ddl_buffer;
use crate::control::server::shared::session::ddl_effect::DeferredDdlEffect;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

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

/// Register the index with its analyzer and fuzzy default, record its owner,
/// and rebuild the collection's text from its stored rows.
///
/// The record registers `Building` and flips to `Ready` once every node
/// rebuilt. A rebuild error leaves it `Building`. The boot reconcile runs
/// the rebuild again, and a `DROP INDEX` removes it.
///
/// `ANALYZER '<name>'` and `FUZZY <bool>` travel on the record. A Register
/// carries the collection's analysis to every core, and the core installs it
/// where every tokenization reads it: forward indexing, the staged-write
/// overlay, search, and the rebuild (`InvertedIndex::analyze_for_collection`).
/// A second index that names other values is refused.
async fn create_text_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    stmt: IndexStatement,
    command: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    crate::control::server::shared::ddl::neutral::object_permission::require_create_permission(
        state,
        identity,
        database_id,
        "search indexes",
    )?;
    // The header resolves the collection and fields by the SQL identifier rule.
    let collection = stmt.header.collection.clone();
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
    // per-column name and discarding the declared one will leave
    // `DROP INDEX <the name I typed>` unable to match.
    let index_name = resolve_index_name(&stmt, &collection);
    // A `DROP INDEX` of the same name earlier in this transaction frees it.
    // The drop's deferred effect leaves the new record in place.
    if let Some(taken) = index_name_holder(state, database_id, tenant_id, &index_name, command)? {
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

    // One inverted index serves every full-text index of the collection, so
    // they tokenize one way. The listing applies this transaction's buffered
    // DDL, so a record an earlier statement buffered counts. The proposal
    // checks again under the DDL preparation lease.
    let records = state
        .credentials
        .catalog()
        .list_index_records_for_collection(database_id.as_u64(), tenant_id.as_u64(), &collection)
        .map_err(|e| {
            DdlError::from_error_in_context(&format!("{command}: read index registry"), &e)
        })?;
    if let Some(conflict) = analysis_conflict(
        &records,
        &collection,
        analyzer_name.as_deref(),
        fuzzy_default,
    ) {
        return Err(ddl_err("0A000", format!("{command}: {conflict}")));
    }

    // The record carries the analyzer and fuzzy default. Every node's
    // Register delivers them to its cores, so every write, query and rebuild
    // of the collection tokenizes with them.
    propose_text_index_record(
        state,
        &IndexRegistration {
            database_id,
            tenant_id,
            name: &index_name,
            kind: IndexKind::FullText,
            collection: &collection,
            fields: stmt.header.columns.clone(),
            // `Ready` once every node rebuilt the collection's text below.
            state: IndexRecordState::Building,
        },
        analyzer_name.clone(),
        fuzzy_default,
    )
    .await?;
    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        IndexKind::FullText.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        &index_name,
        &identity.username,
    )
    .await?;
    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "created text index '{index_name}' on '{collection}' ({}) (analyzer={}, fuzzy={})",
            stmt.header.columns.join(", "),
            analyzer_name.as_deref().unwrap_or("unnamed"),
            fuzzy_default
                .map(|f| f.to_string())
                .unwrap_or_else(|| "unnamed".to_string()),
        ),
    );

    // Rows stored before the index existed become searchable, tokenized by
    // the record's analyzer. Inside an explicit transaction the rebuild
    // waits for COMMIT, after the buffered record and binding land, so a
    // ROLLBACK leaves nothing.
    let deferred = ddl_buffer::defer_effect(DeferredDdlEffect::TextIndexBuild {
        tenant_id,
        database_id,
        collection: collection.clone(),
        index_name: index_name.clone(),
    });
    if !deferred {
        build_text_index(state, tenant_id, database_id, &collection, &index_name).await?;
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
/// placeholder will collide across collections, so it is replaced by a name
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
