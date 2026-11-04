// SPDX-License-Identifier: BUSL-1.1

//! The bare collection name a client request names.

use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection};

/// The bare collection name a caller sent, in the session's database.
///
/// The caller names the collection bare, or with this database's own
/// qualifier. Another database's qualifier, a stray `/`, or an empty name is
/// a `BadRequest` (SQLSTATE `42601`). Every plan builder then qualifies the
/// bare name with `QualifiedCollection::new`, so a request never reaches a
/// collection of another database.
pub(crate) fn request_collection(
    database_id: DatabaseId,
    caller_form: &str,
) -> crate::Result<String> {
    let qualified = QualifiedCollection::from_request(database_id, &caller_form.to_lowercase())?;
    Ok(CollectionKey::from_qualified(database_id, &qualified)?
        .name()
        .to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_or_own_qualified_name_resolves_bare() {
        let db = DatabaseId::new(7);
        assert_eq!(request_collection(db, "Orders").expect("bare"), "orders");
        assert_eq!(request_collection(db, "7/orders").expect("own"), "orders");
    }

    #[test]
    fn another_database_qualifier_is_a_bad_request() {
        let err = request_collection(DatabaseId::DEFAULT, "7/orders").expect_err("foreign");
        assert!(matches!(err, crate::Error::BadRequest { .. }), "{err}");
        let err = request_collection(DatabaseId::new(7), "a/b").expect_err("stray");
        assert!(matches!(err, crate::Error::BadRequest { .. }), "{err}");
    }
}
