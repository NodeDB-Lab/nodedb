// SPDX-License-Identifier: BUSL-1.1

//! Apply user-defined CRS catalog entries to `SystemCatalog` redb.
//!
//! A define of a code that is live or dropped leaves the set unchanged. A
//! drop of a code that is not live, or that a catalog definition names,
//! leaves it unchanged too. Every node applies the same log over the same
//! catalog, so every node leaves the set alike, and the statement that
//! proposed the entry reads the set back to report the outcome.

use nodedb_crs::{CrsDef, Srid};

use crate::control::security::catalog::{SystemCatalog, catalog_err};

pub fn define(def: &CrsDef, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .define_custom_crs(def)
        .map(|_| ())
        .map_err(|e| catalog_err(&format!("define spatial_ref_sys {}", def.srid), e))
}

/// Retire `srid` unless a definition names it. The DDL checks the names
/// before it proposes. This check covers a definition that committed
/// between that check and the drop.
pub fn retire(srid: Srid, catalog: &SystemCatalog) -> crate::Result<()> {
    if !catalog.srid_referrers(srid)?.is_empty() {
        return Ok(());
    }
    catalog
        .drop_custom_crs(srid)
        .map(|_| ())
        .map_err(|e| catalog_err(&format!("drop spatial_ref_sys {srid}"), e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::catalog::collection_constraints::CheckConstraintDef;
    use nodedb_crs::{CustomCrsState, parse_proj_string};
    use nodedb_types::DatabaseId;

    /// A collection that names the code committed after the DDL checked:
    /// the apply leaves the code live. With the definition gone, the drop
    /// applies.
    #[test]
    fn a_named_code_stays_live_at_apply() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let catalog = SystemCatalog::open(&tmp.path().join("system.redb")).expect("open");
        let srid = Srid::new(910_077).expect("srid");
        let def =
            parse_proj_string(srid, "tm", "+proj=tmerc +lon_0=101 +ellps=GRS80").expect("parses");
        define(&def, &catalog).expect("define");

        let mut coll = StoredCollection::stamped_for_test(1, "parcels", "admin");
        coll.check_constraints = vec![CheckConstraintDef {
            name: "in_tm".into(),
            check_sql: "ST_X(ST_Transform(NEW.loc, 910077)) > 0".into(),
            has_subquery: false,
        }];
        catalog
            .put_collection(DatabaseId::DEFAULT, &coll)
            .expect("collection");
        retire(srid, &catalog).expect("retire");
        assert!(matches!(
            catalog.custom_crs_set().get(srid),
            Some(CustomCrsState::Live(_))
        ));
        assert_eq!(
            catalog.srid_referrers(srid).expect("scan"),
            vec!["collection 'parcels' (tenant 1, database 0): CHECK constraint 'in_tm'"]
        );

        coll.check_constraints.clear();
        catalog
            .put_collection(DatabaseId::DEFAULT, &coll)
            .expect("collection");
        retire(srid, &catalog).expect("retire");
        assert_eq!(
            catalog.custom_crs_set().get(srid),
            Some(&CustomCrsState::Dropped)
        );
    }
}
