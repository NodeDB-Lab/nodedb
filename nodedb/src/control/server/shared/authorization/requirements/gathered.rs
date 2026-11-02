//! The sources a coordinator-gathered child hands its embedded rows.

use nodedb_physical::physical_plan::GatheredSource;
use nodedb_types::{DatabaseId, QualifiedCollection};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::Permission;
use crate::control::target_identity::bare_collection_name;

use super::AuthorizationRequirement;
use super::collect::plan_requirements;

/// The requirements of `child`, as the sources of the `ProviderScan` that
/// replaces it with its gathered rows. The authorization of the plan that
/// embeds the rows then demands what `child` demanded.
///
/// A gathered child is a read, so every requirement it has is Read. A child
/// that requires any other permission is refused: its rows cannot carry
/// that requirement on.
///
/// Each collection is named qualified for `database_id`, the database the
/// read runs in, the form a plan's own collections take. A plan names a
/// provider relation bare; it is qualified here too, so the redaction rules
/// keyed on the qualified name match the gathered rows.
pub fn gathered_sources(
    child: &PhysicalPlan,
    database_id: DatabaseId,
) -> crate::Result<Vec<GatheredSource>> {
    plan_requirements(child)
        .into_iter()
        .map(|requirement| match requirement {
            AuthorizationRequirement::Collection {
                collection,
                permission: Permission::Read,
            } => Ok(GatheredSource::Collection(
                QualifiedCollection::new(
                    database_id,
                    &bare_collection_name(database_id, &collection),
                )
                .as_str()
                .to_owned(),
            )),
            AuthorizationRequirement::Tenant {
                permission: Permission::Read,
            } => Ok(GatheredSource::Tenant),
            AuthorizationRequirement::Collection { permission, .. }
            | AuthorizationRequirement::Tenant { permission } => Err(crate::Error::Internal {
                detail: format!(
                    "a gathered child requires {permission:?}; only a read is gathered \
                     into embedded rows"
                ),
            }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{GatheredSource, QueryOp};
    use nodedb_types::{DatabaseId, QualifiedCollection};

    use super::gathered_sources;
    use crate::bridge::envelope::PhysicalPlan;

    fn join_in(db: DatabaseId) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: QualifiedCollection::new(db, "orders"),
            right_collection: QualifiedCollection::new(db, "users"),
            condition: Vec::new(),
            join_type: "inner".into(),
            limit: 10,
            left_rls_filters: Vec::new(),
            right_rls_filters: Vec::new(),
            left_input: None,
            right_input: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        })
    }

    #[test]
    fn a_join_child_hands_on_both_collections() {
        assert_eq!(
            gathered_sources(&join_in(DatabaseId::DEFAULT), DatabaseId::DEFAULT).expect("a read"),
            vec![
                GatheredSource::Collection("orders".into()),
                GatheredSource::Collection("users".into()),
            ]
        );
    }

    /// Outside the default database every source is named qualified, the
    /// form the plan names its own collections in.
    #[test]
    fn sources_are_qualified_for_the_reads_database() {
        let db = DatabaseId::new(7);
        let qualified = |name: &str| QualifiedCollection::new(db, name).as_str().to_owned();
        assert_eq!(
            gathered_sources(&join_in(db), db).expect("a read"),
            vec![
                GatheredSource::Collection(qualified("orders")),
                GatheredSource::Collection(qualified("users")),
            ]
        );
    }
}
