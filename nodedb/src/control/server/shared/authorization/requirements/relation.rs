//! The grant-store name of a requirement's relation.

use nodedb_types::{CollectionKey, CollectionKeyError, DatabaseId};

/// The grant-store name of a relation a requirement names in `database_id`.
///
/// A plan collection carries its database's qualifier. A provider relation,
/// such as a catalog table or a continuous aggregate, is named bare. Both
/// resolve through [`CollectionKey::from_relation_name`].
pub fn relation_grant_name(
    database_id: DatabaseId,
    name: &str,
) -> Result<&str, CollectionKeyError> {
    CollectionKey::from_relation_name(database_id, name).map(|key| key.name())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plan_collection_drops_its_qualifier() {
        let db = DatabaseId::new(7);
        assert_eq!(relation_grant_name(db, "7/orders").expect("own"), "orders");
    }

    #[test]
    fn a_provider_relation_stays_bare() {
        let db = DatabaseId::new(7);
        assert_eq!(
            relation_grant_name(db, "_system.audit_log").expect("bare"),
            "_system.audit_log"
        );
    }

    #[test]
    fn a_foreign_name_is_refused() {
        assert!(relation_grant_name(DatabaseId::new(7), "8/orders").is_err());
    }
}
