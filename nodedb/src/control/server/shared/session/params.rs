// SPDX-License-Identifier: BUSL-1.1

//! Session parameter methods (SET/SHOW) on SessionStore.

use std::net::SocketAddr;

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use super::store::SessionStore;

impl SessionStore {
    /// Set a session parameter.
    pub fn set_parameter(&self, addr: &SocketAddr, key: String, value: String) {
        self.write_session(addr, |session| {
            session.parameters.insert(key, value);
        });
    }

    /// Reset one mutable session parameter to its connection default.
    pub fn reset_parameter(&self, addr: &SocketAddr, key: &str) {
        let defaults = super::state::default_parameters();
        let default = defaults
            .into_iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(key));
        self.write_session(addr, |session| {
            session
                .parameters
                .retain(|name, _| !name.eq_ignore_ascii_case(key));
            if let Some((name, value)) = default {
                session.parameters.insert(name, value);
            }
        });
    }

    /// Reset every mutable session parameter and tenant override.
    pub fn reset_all_parameters(&self, addr: &SocketAddr) {
        self.write_session(addr, |session| {
            session.parameters = super::state::default_parameters();
            session.effective_tenant_id = None;
        });
    }

    /// Get a session parameter.
    pub fn get_parameter(&self, addr: &SocketAddr, key: &str) -> Option<String> {
        self.read_session(addr, |session| {
            session.parameters.get(key).cloned().or_else(|| {
                session
                    .parameters
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(key))
                    .map(|(_, value)| value.clone())
            })
        })?
    }

    /// Get all session parameters.
    pub fn all_parameters(&self, addr: &SocketAddr) -> Vec<(String, String)> {
        self.read_session(addr, |s| {
            let mut params: Vec<_> = s
                .parameters
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            params.sort_by(|a, b| a.0.cmp(&b.0));
            params
        })
        .unwrap_or_default()
    }
}

/// Parse a SET command: `SET [SESSION|LOCAL] key = value` or `SET key TO value`.
///
/// Returns (key, value) on success, or None if not a valid SET command.
pub fn parse_set_command(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Strip SET prefix.
    let rest = if upper.starts_with("SET SESSION ") {
        &trimmed[12..]
    } else if upper.starts_with("SET LOCAL ") {
        &trimmed[10..]
    } else if upper.starts_with("SET ") {
        &trimmed[4..]
    } else {
        return None;
    };

    let rest = rest.trim();

    // Split on = or TO.
    let (key, value) = if let Some(eq_pos) = rest.find('=') {
        let k = rest[..eq_pos].trim();
        let v = rest[eq_pos + 1..].trim();
        (k, v)
    } else {
        // Try TO separator.
        let to_pos = find_ascii_case_insensitive(rest, " TO ")?;
        let k = rest[..to_pos].trim();
        let v = rest[to_pos + 4..].trim();
        (k, v)
    };

    if key.is_empty() {
        return None;
    }

    // Strip quotes from value.
    let value = value.trim_matches('\'').trim_matches('"').to_string();

    Some((key.to_lowercase(), value))
}

/// Known PostgreSQL runtime parameters that `SHOW <name>` is allowed to
/// resolve through the session-parameter fallback.
///
/// Any `SHOW <name>` whose lowercased target is in this set, or that was
/// explicitly set via `SET <name> = ...` in the current session, is a
/// runtime-parameter request. Everything else is an administrative SHOW
/// command and must be routed through the DDL / AST router — the
/// session-parameter fallback returns `42704` (`undefined_object`) for
/// unrecognised names instead of silently emitting an empty single-row
/// response (the failure mode behind the `SHOW DATABASES` / `SHOW ROLES`
/// / `SHOW STATS` / `SHOW METRICS` / `SHOW MEMORY` ghost-row bug).
pub const KNOWN_PG_RUNTIME_PARAMETERS: &[&str] = &[
    "all",
    "application_name",
    "client_encoding",
    "client_min_messages",
    "cross_shard_txn",
    "datestyle",
    "default_read_consistency",
    "default_transaction_isolation",
    "default_transaction_read_only",
    "extra_float_digits",
    "integer_datetimes",
    "intervalstyle",
    "is_superuser",
    "lc_collate",
    "lc_ctype",
    "lc_messages",
    "lc_monetary",
    "lc_numeric",
    "lc_time",
    "server_encoding",
    "server_version",
    "server_version_num",
    "search_path",
    "session_authorization",
    "standard_conforming_strings",
    "statement_timeout",
    "timezone",
    "time zone",
    "transaction_isolation",
    "transaction_read_only",
    // NodeDB-specific session knobs settable via SET.
    "nodedb.consistency",
    "nodedb.tenant_id",
    "nodedb.force_shuffle_join",
    "nodedb.shuffle_num_parts",
    "nodedb.force_shuffle_agg",
    "nodedb.shuffle_agg_num_parts",
    "nodedb.broadcast_threshold_bytes",
    "nodedb.shuffle_agg_threshold",
    "rounding_mode",
];

/// Settable runtime parameters (case-insensitive). Subset of
/// [`KNOWN_PG_RUNTIME_PARAMETERS`] — excludes read-only server identity
/// parameters (`server_version`, `server_version_num`, `is_superuser`,
/// `integer_datetimes`, etc.) and includes NodeDB-specific knobs and the
/// identity / security keys handled by their own dispatch branches
/// (`tenant`, `role`, `session_authorization`). `SET <name>` for any name
/// outside this set returns `42704 undefined_object`, mirroring the
/// `SHOW <unknown>` rejection and closing the silent-store class.
pub const SETTABLE_RUNTIME_PARAMETERS: &[&str] = &[
    "application_name",
    "client_encoding",
    "client_min_messages",
    "datestyle",
    "default_transaction_isolation",
    "default_transaction_read_only",
    "extra_float_digits",
    "intervalstyle",
    "lc_collate",
    "lc_ctype",
    "lc_messages",
    "lc_monetary",
    "lc_numeric",
    "lc_time",
    "search_path",
    "standard_conforming_strings",
    "statement_timeout",
    "timezone",
    "time zone",
    "transaction_isolation",
    "transaction_read_only",
    "rounding_mode",
    // Identity / security keys — handled by their own dispatch branches
    // in `handle_set`; listed here so the allowlist accepts them as known
    // names before the dispatcher claims them.
    "tenant",
    "role",
    "session_authorization",
    // NodeDB-specific session knobs settable via SET.
    "nodedb.consistency",
    "default_read_consistency",
    "cross_shard_txn",
    "nodedb.tenant_id",
    "nodedb.auth_session",
    // Distributed shuffle-join override (permanent operator hint; the automatic
    // cost-model default is a separate effort). Read by the routing planner via
    // the session parameter bag.
    "nodedb.force_shuffle_join",
    "nodedb.shuffle_num_parts",
    // Distributed shuffle-aggregate override (FORCE path; cost-model auto-emit
    // is a separate effort). Read by the routing planner via the session
    // parameter bag.
    "nodedb.force_shuffle_agg",
    "nodedb.shuffle_agg_num_parts",
    // Auto-shuffle cost threshold (bytes). Operator/test override of the
    // node's `[tuning.cluster_transport] broadcast_threshold_bytes`. When both
    // join sides' analyzed sizes exceed this, the planner auto-selects shuffle.
    "nodedb.broadcast_threshold_bytes",
    // Auto-shuffle-aggregate cost threshold (distinct-group count). When a GROUP
    // BY's estimated group cardinality (from ANALYZE distinct_count) exceeds
    // this, the planner auto-selects a whole-aggregate shuffle.
    "nodedb.shuffle_agg_threshold",
    // Unprefixed NodeDB session knob — Calvin cross-shard mode (paired
    // SHOW cross_shard_txn). Read by the routing planner via the session
    // parameter bag.
    "cross_shard_txn",
];

/// Returns `true` if `name` (case-insensitive) is a runtime parameter that
/// can be set via `SET`. Used to reject unknown SET keys with `42704`,
/// matching the behavior of `SHOW <unknown>`.
pub fn is_known_settable_runtime_parameter(name: &str) -> bool {
    let lower = name.to_lowercase();
    SETTABLE_RUNTIME_PARAMETERS
        .iter()
        .any(|p| p.eq_ignore_ascii_case(&lower))
}

/// Returns `true` if `name` (case-insensitive) is a known PostgreSQL or
/// NodeDB session parameter.
pub fn is_known_pg_runtime_parameter(name: &str) -> bool {
    let lower = name.to_lowercase();
    KNOWN_PG_RUNTIME_PARAMETERS
        .iter()
        .any(|p| p.eq_ignore_ascii_case(&lower))
}

/// Parse a SHOW command: `SHOW <parameter>` or `SHOW ALL`.
///
/// Returns the parameter name, or "all" for SHOW ALL.
pub fn parse_show_command(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    if !upper.starts_with("SHOW ") {
        return None;
    }

    let param = trimmed[5..].trim().to_lowercase();
    if param.is_empty() {
        return None;
    }

    Some(param)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_parameter_restores_canonical_defaults() {
        let store = SessionStore::new();
        let addr = "127.0.0.1:5000".parse().expect("socket address");
        store.ensure_session(addr);

        store.set_parameter(&addr, "datestyle".into(), "SQL, DMY".into());
        store.reset_parameter(&addr, "datestyle");
        assert_eq!(
            store.get_parameter(&addr, "datestyle"),
            Some("ISO, MDY".into())
        );

        store.set_parameter(&addr, "default_read_consistency".into(), "eventual".into());
        store.reset_parameter(&addr, "default_read_consistency");
        assert_eq!(
            store.get_parameter(&addr, "default_read_consistency"),
            Some("strong".into())
        );

        store.set_parameter(
            &addr,
            "cross_shard_txn".into(),
            "best_effort_non_atomic".into(),
        );
        store.reset_parameter(&addr, "cross_shard_txn");
        assert_eq!(
            store.get_parameter(&addr, "cross_shard_txn"),
            Some("strict".into())
        );
    }

    #[test]
    fn reset_all_parameters_restores_defaults() {
        let store = SessionStore::new();
        let addr = "127.0.0.1:5000".parse().expect("socket address");
        store.ensure_session(addr);
        store.set_parameter(&addr, "application_name".into(), "worker".into());
        store.set_parameter(&addr, "nodedb.consistency".into(), "eventual".into());

        store.reset_all_parameters(&addr);

        assert_eq!(
            store.get_parameter(&addr, "application_name"),
            Some(String::new())
        );
        assert_eq!(
            store.get_parameter(&addr, "nodedb.consistency"),
            Some("strong".into())
        );
    }

    #[test]
    fn parse_set_equals() {
        let (k, v) = parse_set_command("SET client_encoding = 'UTF8'").unwrap();
        assert_eq!(k, "client_encoding");
        assert_eq!(v, "UTF8");
    }

    #[test]
    fn parse_set_to() {
        let (k, v) = parse_set_command("SET search_path TO public").unwrap();
        assert_eq!(k, "search_path");
        assert_eq!(v, "public");
    }

    #[test]
    fn parse_set_to_after_unicode_key_preserves_original_offsets() {
        let (k, v) = parse_set_command("SET custom.ﬀﬀ TO enabled").unwrap();
        assert_eq!(k, "custom.ﬀﬀ");
        assert_eq!(v, "enabled");
    }

    #[test]
    fn parse_set_session() {
        let (k, v) = parse_set_command("SET SESSION nodedb.consistency = 'eventual'").unwrap();
        assert_eq!(k, "nodedb.consistency");
        assert_eq!(v, "eventual");
    }

    #[test]
    fn parse_set_nodedb_tenant() {
        let (k, v) = parse_set_command("SET nodedb.tenant_id = 5").unwrap();
        assert_eq!(k, "nodedb.tenant_id");
        assert_eq!(v, "5");
    }

    #[test]
    fn parse_show() {
        assert_eq!(
            parse_show_command("SHOW client_encoding"),
            Some("client_encoding".into())
        );
        assert_eq!(parse_show_command("SHOW ALL"), Some("all".into()));
        assert_eq!(parse_show_command("SHOW"), None);
    }
}
