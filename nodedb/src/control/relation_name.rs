// SPDX-License-Identifier: BUSL-1.1

//! The rule every new relation name obeys.
//!
//! A collection, array, materialized view, or continuous aggregate name
//! keys storage through `QualifiedCollection`. That form writes
//! `"{database_id}/{name}"` outside the default database. A name holding `/`
//! makes two relations share one key: `"7/x"` in the default database and
//! `x` in database 7. Every DDL path that mints a name refuses one here.

use nodedb_types::CollectionKey;

/// Refuse `name` as the name of a new `kind` when it is empty or holds `/`.
///
/// The error is `BadRequest`, SQLSTATE `42601`, the code an invalid
/// CREATE COLLECTION name takes.
pub fn check_relation_name(kind: &str, name: &str) -> crate::Result<()> {
    if CollectionKey::is_bare_name(name) {
        return Ok(());
    }
    Err(crate::Error::BadRequest {
        detail: format!(
            "invalid {kind} name '{name}': a {kind} name is non-empty and holds no '/'"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_with_a_separator_is_refused() {
        for name in ["7/x", "/x", "x/", ""] {
            let err = check_relation_name("collection", name).expect_err(name);
            assert!(
                matches!(err, crate::Error::BadRequest { .. }),
                "{name}: {err}"
            );
        }
    }

    #[test]
    fn a_bare_name_is_accepted() {
        assert!(check_relation_name("collection", "orders").is_ok());
        assert!(check_relation_name("array", "grid-1").is_ok());
    }
}
