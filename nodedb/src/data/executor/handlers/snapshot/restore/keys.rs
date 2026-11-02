// SPDX-License-Identifier: BUSL-1.1

//! Snapshot-key parsing helpers shared across the per-engine restore paths.

/// The error for a snapshot key that is not `"{db}:{tid}:{collection}"`.
fn malformed_key(key: &str, detail: &str) -> crate::Error {
    crate::Error::Serialization {
        format: "snapshot key".into(),
        detail: format!("key '{key}' is not '{{db}}:{{tid}}:{{collection}}': {detail}"),
    }
}

/// Split a `"{db}:{tid}:{collection}"` snapshot key into its numeric
/// database and tenant ids and the borrowed collection slice.
///
/// Only the first two ':' are structural. The collection can contain ':'.
/// A key with fewer parts, a non-numeric id, or an empty collection fails.
fn split_scoped_key(key: &str) -> crate::Result<(u64, u64, &str)> {
    let mut it = key.splitn(3, ':');
    let (Some(db), Some(tid), Some(collection)) = (it.next(), it.next(), it.next()) else {
        return Err(malformed_key(key, "expected three ':'-separated parts"));
    };
    let db = db
        .parse::<u64>()
        .map_err(|e| malformed_key(key, &format!("database id: {e}")))?;
    let tid = tid
        .parse::<u64>()
        .map_err(|e| malformed_key(key, &format!("tenant id: {e}")))?;
    if collection.is_empty() {
        return Err(malformed_key(key, "empty collection"));
    }
    Ok((db, tid, collection))
}

/// Parse a `"{db}:{tid}:{collection}"` snapshot key into `(database_id,
/// tenant_id, collection)`. The timeseries, flushed-segment, columnar and KV
/// sections all key their entries this way. Any other shape fails.
pub(in crate::data::executor::handlers::snapshot) fn parse_scoped_snapshot_key(
    key: &str,
) -> crate::Result<(u64, u64, String)> {
    let (db, tid, collection) = split_scoped_key(key)?;
    Ok((db, tid, collection.to_string()))
}

/// Parse a vector snapshot key `"{db}:{tid}:{coll_key}"` into
/// `(database_id, tenant_id, coll_key)`. Any other shape fails.
///
/// The tenant comes from the key, so a merged multi-tenant snapshot installs
/// each collection under its own tenant. `coll_key` is a borrowed slice of
/// `key`, so the caller passes it to `restore_vector_collection` without an
/// allocation.
pub(super) fn parse_vector_snapshot_key(key: &str) -> crate::Result<(u64, u64, &str)> {
    split_scoped_key(key)
}

/// Recover the database id encoded in a db-qualified collection name.
///
/// Non-default databases qualify their collections as `"{database_id}/{name}"`.
/// The default database uses the bare name, which maps to
/// `DatabaseId::DEFAULT` (0). A collection name never holds a `/`, so a `/`
/// after a non-numeric prefix marks a malformed name and fails.
///
/// Shared with the KV checkpoint path (`data::executor::kv_checkpoint`), which
/// faces the same problem: `KvEngine` stores the db-qualified name in
/// `hash_to_collection`, and the database id must be recovered from it to
/// rebuild the table key a live read computes.
pub(in crate::data::executor) fn database_id_from_qualified(
    collection: &str,
) -> crate::Result<u64> {
    nodedb_types::QualifiedCollection::database_of_stored(collection)
        .map(|database| database.as_u64())
        .map_err(|e| crate::Error::Serialization {
            format: "qualified collection name".into(),
            detail: e.message().to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A merged multi-tenant snapshot installs each vector collection under
    /// the tenant its key names, not the dispatch tenant.
    #[test]
    fn a_vector_key_carries_its_own_database_and_tenant() {
        assert_eq!(
            parse_vector_snapshot_key("1025:7:1025/emb:field").unwrap(),
            (1025, 7, "1025/emb:field")
        );
    }

    /// A KV section key names its database and tenant, and the collection
    /// keeps its database qualifier.
    #[test]
    fn a_kv_key_splits_into_database_tenant_and_qualified_name() {
        let (db, tid, collection) = parse_scoped_snapshot_key("1025:7:1025/orders").unwrap();
        assert_eq!((db, tid, collection.as_str()), (1025, 7, "1025/orders"));
        assert_eq!(database_id_from_qualified(&collection).unwrap(), db);
        let (db, tid, collection) = parse_scoped_snapshot_key("0:7:orders").unwrap();
        assert_eq!((db, tid, collection.as_str()), (0, 7, "orders"));
        assert_eq!(database_id_from_qualified(&collection).unwrap(), db);
    }

    /// A `/` after a non-numeric prefix names no database and fails.
    #[test]
    fn a_non_numeric_qualifier_is_refused() {
        assert!(matches!(
            database_id_from_qualified("abc/orders"),
            Err(crate::Error::Serialization { .. })
        ));
    }

    /// A two-part `"{tid}:{collection}"` key and a bare key fail to parse.
    #[test]
    fn an_old_shape_key_is_refused() {
        for key in ["7:orders", "orders", "3:emb"] {
            assert!(
                matches!(
                    parse_scoped_snapshot_key(key),
                    Err(crate::Error::Serialization { .. })
                ),
                "scoped key '{key}' must be refused"
            );
            assert!(
                matches!(
                    parse_vector_snapshot_key(key),
                    Err(crate::Error::Serialization { .. })
                ),
                "vector key '{key}' must be refused"
            );
        }
    }

    /// A non-numeric id or an empty collection fails to parse.
    #[test]
    fn a_malformed_three_part_key_is_refused() {
        for key in ["x:7:orders", "0:y:orders", "0:7:"] {
            assert!(
                parse_scoped_snapshot_key(key).is_err(),
                "key '{key}' must be refused"
            );
        }
    }
}
