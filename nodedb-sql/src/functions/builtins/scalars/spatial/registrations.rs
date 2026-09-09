// SPDX-License-Identifier: Apache-2.0

//! Spatial scalar function registrations, generated from the shared catalog.
//!
//! Every accepted spelling of every catalog entry is registered, so the
//! plan-time existence gate accepts exactly the names the evaluator can
//! evaluate. This list is not maintained by hand: a capability added to
//! `nodedb_query::geo_functions::GEO_FUNCTIONS` appears here automatically,
//! under both its standard `ST_*` name and its internal `geo_*` one.

use nodedb_query::geo_functions::GEO_FUNCTIONS;

use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::super::helpers::V0_1_0;
use super::arg_shape::{arg_types, return_type};
use super::triggers::search_trigger;

pub(in crate::functions::builtins::scalars) fn spatial_functions() -> Vec<FunctionMeta> {
    let mut registrations = Vec::new();
    for spec in GEO_FUNCTIONS {
        let (min_args, max_args) = spec.args.arity();
        let trigger = search_trigger(spec.canonical);
        for name in spec.names() {
            registrations.push(FunctionMeta {
                name,
                category: Scalar,
                min_args,
                max_args,
                search_trigger: trigger,
                return_type: return_type(spec.returns),
                arg_types: arg_types(spec.args),
                since: V0_1_0,
                volatility: nodedb_types::Volatility::Immutable,
            });
        }
    }
    registrations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::registry::FunctionRegistry;

    /// The failure this generation exists to prevent: a capability that the
    /// evaluator implements but the registry never learned, so the plan-time
    /// gate rejects it with 42883. Every catalog spelling must be registered.
    #[test]
    fn every_catalog_spelling_is_registered() {
        let registry = FunctionRegistry::new();
        for spec in GEO_FUNCTIONS {
            for name in spec.names() {
                assert!(
                    registry.lookup(name).is_some(),
                    "'{name}' is in the geo catalog but not registered for planning"
                );
            }
        }
    }

    /// Standard and internal spellings must be interchangeable at plan time,
    /// not merely both present — same arity, same typing, same routing.
    #[test]
    fn every_spelling_of_a_function_registers_identically() {
        let registry = FunctionRegistry::new();
        for spec in GEO_FUNCTIONS {
            let Some(canonical) = registry.lookup(spec.canonical) else {
                panic!("'{}' must be registered", spec.canonical);
            };
            for alias in spec.aliases {
                let Some(other) = registry.lookup(alias) else {
                    panic!("'{alias}' must be registered");
                };
                assert_eq!(
                    (other.min_args, other.max_args),
                    (canonical.min_args, canonical.max_args),
                    "'{alias}' and '{}' disagree on arity",
                    spec.canonical
                );
                assert_eq!(
                    other.return_type, canonical.return_type,
                    "'{alias}' and '{}' disagree on return type",
                    spec.canonical
                );
                assert_eq!(
                    other.search_trigger, canonical.search_trigger,
                    "'{alias}' and '{}' disagree on search routing",
                    spec.canonical
                );
            }
        }
    }

    /// The accessors from the original report, which had no registration at
    /// all while their `geo_*` twins evaluated fine.
    #[test]
    fn standard_accessor_names_are_registered() {
        let registry = FunctionRegistry::new();
        for name in [
            "st_astext",
            "st_asgeojson",
            "st_x",
            "st_y",
            "st_geometrytype",
            "st_npoints",
            "st_isvalid",
            "st_srid",
            "st_area",
            "st_centroid",
            "st_length",
            "st_perimeter",
        ] {
            assert!(
                registry.lookup(name).is_some(),
                "'{name}' must be registered"
            );
        }
    }

    /// Constructors must be registered as scalar functions too, so they
    /// resolve in a projection and not only in INSERT value position.
    #[test]
    fn constructors_are_registered_as_scalars() {
        let registry = FunctionRegistry::new();
        for name in [
            "st_point",
            "st_makepoint",
            "st_geomfromtext",
            "st_geomfromgeojson",
            "st_geomfromwkb",
            "st_makeline",
            "st_makepolygon",
            "st_makeenvelope",
        ] {
            let Some(meta) = registry.lookup(name) else {
                panic!("'{name}' must be registered");
            };
            assert_eq!(meta.category, Scalar);
            assert_eq!(
                meta.return_type,
                Some(nodedb_types::columnar::ColumnType::Geometry),
                "'{name}' must be typed as returning a geometry"
            );
        }
    }
}
