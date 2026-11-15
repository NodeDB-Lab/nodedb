// SPDX-License-Identifier: BUSL-1.1

//! The statements the DDL router runs itself.
//!
//! [`claims`] is the one decision of which statements the router takes. The
//! router reads its parts at each stage: [`string_arm_claims`] gates the
//! string arms, and [`untyped_claim`] picks the untyped arm. The request
//! planner reads [`claims`] to keep a router statement out of `plan_sql`.
//!
//! `claims` is never false for a statement the router takes. It can be true
//! for a statement the router then declines. Examples: a typed statement no
//! typed arm takes, `ALTER TENANT .. IN DATABASE` that falls through the
//! string arm, or an object literal the write parser cannot read.

use nodedb_sql::parser::query_function::parse_query_function;

use super::helpers::{extract_last_value_args, extract_last_values_arg};
use super::route_tokens::RouteTokens;
use super::{
    string_engine_ops, string_introspection, string_schema, string_streaming, string_versioning,
};

/// Whether the DDL router runs `statement` itself.
///
/// True for a statement a string arm takes, one the typed DDL parser claims,
/// `COPY .. FROM`, an object-literal INSERT or UPSERT, `UPSERT .. VALUES`,
/// and a temporal or audit query function.
///
/// The string arms read the trimmed text by prefix. So a leading comment
/// hides a prefix-routed statement from the router, and from this predicate.
pub fn claims(statement: &str) -> bool {
    let upper = statement.trim().to_uppercase();
    string_arm_claims(statement, &upper)
        || nodedb_sql::ddl_ast::parse(statement).is_some()
        || untyped_claim(statement).is_some()
}

/// The untyped arm of a statement the typed DDL parser does not claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UntypedClaim {
    /// `COPY <collection> FROM ..`.
    CopyFrom,
    /// `INSERT INTO <collection> { .. }`.
    ObjectLiteralInsert,
    /// `UPSERT INTO <collection> [(..)] VALUES ..` or
    /// `UPSERT INTO <collection> { .. }`.
    Upsert,
    /// A temporal or audit query function, such as `SELECT BALANCE_AS_OF(..)`.
    QueryFunction,
}

/// The untyped arm of `sql`, in the order the router tries them.
pub(super) fn untyped_claim(sql: &str) -> Option<UntypedClaim> {
    // Only a statement that can open with COPY, INSERT, or UPSERT is
    // tokenized. A plain query skips the tokenizer, whose cost grows with
    // the statement's length.
    if may_lead_with_write_or_copy(sql) {
        let tokens = RouteTokens::new(sql);
        if is_copy_from(&tokens) {
            return Some(UntypedClaim::CopyFrom);
        }
        if is_object_literal_write(&tokens, "INSERT") {
            return Some(UntypedClaim::ObjectLiteralInsert);
        }
        if is_upsert_values(&tokens) || is_object_literal_write(&tokens, "UPSERT") {
            return Some(UntypedClaim::Upsert);
        }
    }
    if parse_query_function(sql).is_some() {
        return Some(UntypedClaim::QueryFunction);
    }
    None
}

/// Whether a string arm of the router can take `sql`.
///
/// `upper` is `sql` trimmed and upper-cased, as the router passes it to
/// the string arms. The router skips every string arm when this is false.
pub(super) fn string_arm_claims(sql: &str, upper: &str) -> bool {
    ARM_PREFIXES.iter().any(|prefix| upper.starts_with(prefix))
        || guarded_arm_claims(sql, upper)
        || string_engine_ops::shape_claims(sql)
        || string_introspection::shape_claims(sql, upper)
        || string_schema::shape_claims(sql)
        || string_streaming::shape_claims(sql)
        || string_versioning::shape_claims(sql)
}

/// The string arms whose prefix alone does not decide. Each reads its
/// arguments first and falls through when they do not parse.
fn guarded_arm_claims(sql: &str, upper: &str) -> bool {
    (upper.starts_with("SELECT LAST_VALUES(") && extract_last_values_arg(sql).is_some())
        || (upper.starts_with("SELECT LAST_VALUE(")
            && !upper.starts_with("SELECT LAST_VALUES(")
            && extract_last_value_args(sql).is_some())
        || (upper.starts_with("SEARCH ")
            && nodedb_sql::ddl_ast::parse_search_using_fusion(sql).is_some())
}

/// Whether `sql` can open with `COPY`, `INSERT`, or `UPSERT`, in any case,
/// once comments are skipped. A leading comment answers `true`: the routing
/// tokens skip it, so the first word is not known without them.
fn may_lead_with_write_or_copy(sql: &str) -> bool {
    let head = sql.trim_start();
    if head.starts_with("--") || head.starts_with("/*") {
        return true;
    }
    let word_end = head
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(head.len());
    let word = &head[..word_end];
    ["COPY", "INSERT", "UPSERT"]
        .iter()
        .any(|verb| word.eq_ignore_ascii_case(verb))
}

/// `COPY <collection> FROM ...`: the clause follows the collection name.
fn is_copy_from(tokens: &RouteTokens) -> bool {
    tokens.leads_with(&["COPY"]) && tokens.phrase_at(tokens.skip_name(1), &["FROM"])
}

/// `<verb> INTO <collection> { ... }`, the object-literal write form.
fn is_object_literal_write(tokens: &RouteTokens, verb: &str) -> bool {
    tokens.leads_with(&[verb, "INTO"]) && tokens.phrase_at(tokens.skip_name(2), &["{"])
}

/// `UPSERT INTO <collection> [(columns)] VALUES (...)`.
fn is_upsert_values(tokens: &RouteTokens) -> bool {
    tokens.leads_with(&["UPSERT", "INTO"]) && tokens.has_word_from(tokens.skip_name(2), "VALUES")
}

/// Every prefix a string arm matches on the trimmed, upper-cased text.
///
/// A unit test reads each string arm's source and checks that this table
/// covers every prefix there. The guarded prefixes live in
/// [`guarded_arm_claims`].
const ARM_PREFIXES: &[&str] = &[
    "ALTER AUTH USER ",
    "ALTER FUNCTION ",
    "ALTER ORG ",
    "ALTER SCOPE ",
    "ALTER SERVICE ACCOUNT ",
    "ALTER SYSTEM ",
    "ALTER TENANT ",
    "ALTER TIMESERIES ",
    "ALTER TYPEGUARD ",
    "ANALYZE ",
    "BLACKLIST ",
    "BLACKLIST AUTH USERS WHERE",
    "CALL ",
    "CLEAR AUDIT",
    "COMMIT OFFSET ",
    "COMMIT OFFSETS ",
    "COMPACT ",
    "COMPACT HISTORY ON ",
    "CRDT MERGE ",
    "CREATE AGGREGATE FUNCTION ",
    "CREATE API KEY ",
    "CREATE AUTH KEY ",
    "CREATE CHECKPOINT ",
    "CREATE FULLTEXT INDEX ",
    "CREATE FUNCTION ",
    "CREATE GRAPH INDEX ",
    "CREATE ORG ",
    "CREATE OR REPLACE AGGREGATE FUNCTION ",
    "CREATE OR REPLACE FUNCTION ",
    "CREATE OR REPLACE PROCEDURE ",
    "CREATE OR REPLACE TYPEGUARD ",
    "CREATE PROCEDURE ",
    "CREATE RESTORE POINT ",
    "CREATE RESTORE POINT",
    "CREATE ROLE ",
    "CREATE SEARCH INDEX ",
    "CREATE SERVICE ACCOUNT ",
    "CREATE SORTED INDEX ",
    "CREATE SPARSE INDEX ",
    "CREATE SPATIAL INDEX ",
    "CREATE TENANT ",
    "CREATE TIMESERIES ",
    "CREATE TOPIC ",
    "CREATE TYPEGUARD ",
    "CREATE VECTOR INDEX ",
    "\\D ",
    "DEACTIVATE AUTH USER ",
    "DEFINE EVENT ",
    "DEFINE FIELD ",
    "DEFINE METERING DIMENSION ",
    "DEFINE QUOTA ",
    "DEFINE SCOPE ",
    "DELEGATE AUTH USER ",
    "DELETE AUDIT",
    "DESCRIBE ",
    "DESCRIBE SEQUENCE",
    "DROP CHECKPOINT ",
    "DROP CONSTRAINT ",
    "DROP FUNCTION ",
    "DROP ORG ",
    "DROP PROCEDURE ",
    "DROP QUOTA ",
    "DROP ROLE ",
    "DROP SCOPE ",
    "DROP SERVICE ACCOUNT ",
    "DROP SORTED INDEX ",
    "DROP TENANT ",
    "DROP TOPIC ",
    "DROP TYPEGUARD ",
    "DROP USER ",
    "EMERGENCY LOCKDOWN",
    "EMERGENCY UNLOCK",
    "EXPLAIN PERMISSION ",
    "EXPLAIN SCOPE ",
    "EXPLAIN TIERS ",
    "EXPORT AUDIT",
    "EXPORT USAGE ",
    "GRANT SCOPE ",
    "IMPERSONATE AUTH USER ",
    "KILL SESSION ",
    "KILL USER SESSIONS ",
    "LIST API KEYS",
    "LIST AUTH KEYS",
    "PUBLISH TO ",
    "PURGE AUTH USERS ",
    "PURGE TENANT ",
    "REBALANCE",
    "REFRESH MATERIALIZED VIEW",
    "REMOVE EVENT ",
    "REMOVE NODE ",
    "RENEW SCOPE ",
    "REQUEUE TRIGGER DLQ",
    "REVOKE API KEY ",
    "REVOKE DELEGATION ",
    "REVOKE SCOPE ",
    "REWRITE PARTITIONS ",
    "ROTATE AUTH KEY ",
    "SELECT CRDT_APPLY (",
    "SELECT CRDT_APPLY(",
    "SELECT CRDT_STATE (",
    "SELECT CRDT_STATE(",
    "SELECT DIFF (",
    "SELECT DIFF(",
    "SELECT ESTIMATE_COUNT (",
    "SELECT ESTIMATE_COUNT(",
    "SELECT * FROM NDB_CHUNK_TEXT (",
    "SELECT * FROM NDB_CHUNK_TEXT(",
    "SELECT * FROM TOPK (",
    "SELECT * FROM TOPK(",
    "SELECT * FROM WEIGHTED_PICK (",
    "SELECT * FROM WEIGHTED_PICK(",
    "SELECT KV_CAS (",
    "SELECT KV_CAS(",
    "SELECT KV_DECR (",
    "SELECT KV_DECR(",
    "SELECT KV_GETSET (",
    "SELECT KV_GETSET(",
    "SELECT KV_INCR (",
    "SELECT KV_INCR(",
    "SELECT KV_INCR_FLOAT (",
    "SELECT KV_INCR_FLOAT(",
    "SELECT NDB_CHUNK_TEXT (",
    "SELECT NDB_CHUNK_TEXT(",
    "SELECT NODEDB_ASSERT_VISIBLE(",
    "SELECT RANK (",
    "SELECT RANK(",
    "SELECT RATE_CHECK (",
    "SELECT RATE_CHECK(",
    "SELECT RATE_REMAINING (",
    "SELECT RATE_REMAINING(",
    "SELECT RATE_RESET (",
    "SELECT RATE_RESET(",
    "SELECT SORTED_COUNT (",
    "SELECT SORTED_COUNT(",
    "SELECT TOPK (",
    "SELECT TOPK(",
    "SELECT TRANSFER (",
    "SELECT TRANSFER(",
    "SELECT TRANSFER_ITEM (",
    "SELECT TRANSFER_ITEM(",
    "SELECT TREE_CHILDREN",
    "SELECT TREE_SUM",
    "SELECT VECTOR_METADATA (",
    "SELECT VECTOR_METADATA(",
    "SHOW ALERT",
    "SHOW ALERT STATUS ",
    "SHOW API KEYS",
    "SHOW AUDIT IN DATABASE",
    "SHOW AUDIT LOG",
    "SHOW AUDIT_LOG",
    "SHOW AUDIT WHERE",
    "SHOW AUTH USERS",
    "SHOW BLACKLIST",
    "SHOW CHANGES ",
    "SHOW CHANGE STREAM",
    "SHOW CLUSTER",
    "SHOW COLLECTIONS",
    "SHOW COMPACTION STATUS ",
    "SHOW COMPACTION STATUS",
    "SHOW CONSTRAINTS ON ",
    "SHOW CONSUMER GROUPS ",
    "SHOW CONTINUOUS AGGREGATE",
    "SHOW DELEGATIONS",
    "SHOW FUNCTIONS",
    "SHOW GRANTS",
    "SHOW INDEX",
    "SHOW INDEXES",
    "SHOW MATERIALIZED VIEW",
    "SHOW MEMBERS OF ORG",
    "SHOW MEMORY ",
    "SHOW MEMORY",
    "SHOW METRICS ",
    "SHOW METRICS",
    "SHOW MIGRATIONS",
    "SHOW MY SCOPES",
    "SHOW NODE ",
    "SHOW NODES",
    "SHOW ORGS",
    "SHOW PARTITIONS ",
    "SHOW PEER HEALTH",
    "SHOW PROCEDURES",
    "SHOW QUOTA ",
    "SHOW QUOTAS",
    "SHOW RAFT GROUP ",
    "SHOW RAFT GROUPS",
    "SHOW RANGES",
    "SHOW RESTORE POINTS",
    "SHOW RETENTION POLIC",
    "SHOW ROLES ",
    "SHOW ROLES",
    "SHOW ROUTING",
    "SHOW SCHEDULE",
    "SHOW SCHEDULE HISTORY ",
    "SHOW SCHEMA VERSION",
    "SHOW SCOPE",
    "SHOW SCOPE GRANTS",
    "SHOW SCOPES FOR ",
    "SHOW SERVER STATS ",
    "SHOW SERVER STATS",
    "SHOW SESSION",
    "SHOW SESSIONS",
    "SHOW STATS ",
    "SHOW STATS",
    "SHOW STORAGE ",
    "SHOW TENANTS",
    "SHOW TOPIC",
    "SHOW TRIGGER DLQ",
    "SHOW TYPEGUARD ON ",
    "SHOW TYPEGUARDS",
    "SHOW USAGE ",
    "SHOW USAGE FOR TENANT ",
    "SHOW USERS",
    "SHOW VECTOR INDEX ",
    "SHOW VECTOR MODELS",
    "SHOW VERSIONS OF ",
    "STOP IMPERSONATION",
    "SUBSCRIBE TO ",
    "TREE_CHILDREN",
    "TREE_SUM",
    "TRUNCATE AUDIT",
    "UNBLACKLIST ",
    "UNDROP COLLECTION ",
    "UNDROP TABLE ",
    "VALIDATE TYPEGUARD ON ",
    "VERIFY AUDIT CHAIN",
];

#[cfg(test)]
#[path = "claims_tests.rs"]
mod tests;
