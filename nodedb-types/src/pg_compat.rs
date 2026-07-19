// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL wire-compatibility version constants. NodeDB advertises
//! compatibility with a fixed PostgreSQL major so libpq-based clients gate
//! features correctly. These are the single source of truth for `version()`,
//! the `server_version_num` runtime parameter, and startup ParameterStatus.

/// PostgreSQL version NodeDB advertises compatibility with (`server_version` numeric form source).
pub const PG_COMPAT_VERSION: &str = "15.0";

/// `server_version_num` value: MAJOR*10000 + MINOR*100 + PATCH (15.0 -> 150000).
pub const PG_COMPAT_VERSION_NUM: &str = "150000";

/// PostgreSQL-compatible `server_version` value announced during pgwire
/// startup and exposed through runtime settings. libpq parses the leading
/// numeric version; the suffix preserves NodeDB's build identity.
pub fn server_version_string(nodedb_version: &str) -> String {
    format!("{PG_COMPAT_VERSION} (NodeDB {nodedb_version})")
}

/// The string returned by the SQL `version()` function, mirroring
/// PostgreSQL's `"PostgreSQL <ver> on <triple> ..."` shape so clients that
/// parse the leading `"PostgreSQL <major>"` succeed.
pub fn version_string() -> String {
    format!(
        "PostgreSQL {PG_COMPAT_VERSION} (NodeDB) on {}",
        std::env::consts::ARCH
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_version_starts_numeric_and_preserves_nodedb_build() {
        assert_eq!(server_version_string("0.4.0"), "15.0 (NodeDB 0.4.0)");
    }

    #[test]
    fn version_string_starts_with_postgres_major() {
        assert!(version_string().starts_with("PostgreSQL 15"));
    }

    #[test]
    fn version_num_parses_and_meets_minimum() {
        let num: i64 = PG_COMPAT_VERSION_NUM.parse().expect("valid integer");
        assert!(num >= 100000);
    }
}
