// SPDX-License-Identifier: Apache-2.0

//! Per-statement CRS resolution.

use core::cell::RefCell;
use std::sync::Arc;

use nodedb_srid::{SridResolver, code_from_name};

use super::builtin::builtin_lookup;
use super::custom::{CustomCrsSet, CustomCrsState};
use crate::def::{CrsDef, CrsKind};
use crate::error::{CrsError, UnknownSrid};
use crate::srid::Srid;
use crate::transform::Transformer;

/// The user-defined set a context resolves against: borrowed for the
/// context's life, or shared through an `Arc`.
#[derive(Debug, Clone)]
enum CustomSource<'a> {
    Borrowed(&'a CustomCrsSet),
    Shared(Arc<CustomCrsSet>),
}

impl CustomSource<'_> {
    fn set(&self) -> &CustomCrsSet {
        match self {
            CustomSource::Borrowed(set) => set,
            CustomSource::Shared(set) => set,
        }
    }
}

/// Resolves SRIDs for one statement: user-defined CRSs first, then the
/// built-in table. Parsed built-ins are memoized for the context's life.
///
/// One context serves one statement. It never crosses a plane.
#[derive(Debug)]
pub struct CrsContext<'a> {
    custom: CustomSource<'a>,
    memo: RefCell<Vec<(Srid, Arc<CrsDef>)>>,
}

impl CrsContext<'static> {
    /// A context with no user-defined CRSs.
    pub fn builtin_only() -> CrsContext<'static> {
        CrsContext::new(CustomCrsSet::empty())
    }

    /// A context that owns a share of `custom`, so it borrows nothing and a
    /// caller holds it across any other borrow.
    pub fn shared(custom: Arc<CustomCrsSet>) -> CrsContext<'static> {
        CrsContext::with_source(CustomSource::Shared(custom))
    }
}

impl<'a> CrsContext<'a> {
    pub fn new(custom: &'a CustomCrsSet) -> Self {
        CrsContext::with_source(CustomSource::Borrowed(custom))
    }

    fn with_source(custom: CustomSource<'a>) -> Self {
        Self {
            custom,
            memo: RefCell::new(Vec::new()),
        }
    }

    /// The user-defined CRSs this context resolves first.
    pub fn custom_set(&self) -> &CustomCrsSet {
        self.custom.set()
    }

    /// A context on the same user-defined set with its own memo. A scan
    /// thread takes a fork, since a context is not `Sync`.
    pub fn fork(&self) -> CrsContext<'a> {
        CrsContext::with_source(self.custom.clone())
    }

    /// The definition of `srid`.
    pub fn resolve(&self, srid: Srid) -> Result<Arc<CrsDef>, UnknownSrid> {
        let code = srid.code();
        if let Some(state) = self.custom_set().get(srid) {
            return match state {
                CustomCrsState::Live(def) => Ok(self.memoize(srid, || (**def).clone())),
                CustomCrsState::Dropped => Err(UnknownSrid::Dropped { code }),
            };
        }
        if let Some(def) = self.memo.borrow().iter().find(|(s, _)| *s == srid) {
            return Ok(Arc::clone(&def.1));
        }
        let entry = builtin_lookup(code).ok_or(UnknownSrid::Undefined { code })?;
        let def = entry
            .definition(srid)
            .map_err(|cause| UnknownSrid::Unsupported { code, cause })?;
        Ok(self.memoize(srid, || def))
    }

    /// The memoized definition of `srid`, built by `make` on first use.
    fn memoize(&self, srid: Srid, make: impl FnOnce() -> CrsDef) -> Arc<CrsDef> {
        let mut memo = self.memo.borrow_mut();
        if let Some((_, def)) = memo.iter().find(|(s, _)| *s == srid) {
            return Arc::clone(def);
        }
        let def = Arc::new(make());
        memo.push((srid, Arc::clone(&def)));
        def
    }

    /// Check `code` names a defined CRS, without parsing it.
    pub fn resolve_code(&self, code: i64) -> Result<Srid, UnknownSrid> {
        let code = u32::try_from(code).map_err(|_| UnknownSrid::OutOfRange { code })?;
        self.resolve_u32(code)
    }

    /// Check `code` names a defined CRS, without parsing it.
    pub fn resolve_u32(&self, code: u32) -> Result<Srid, UnknownSrid> {
        let srid = Srid::new(code).map_err(|_| UnknownSrid::OutOfRange {
            code: i64::from(code),
        })?;
        match self.custom_set().get(srid) {
            Some(CustomCrsState::Live(_)) => Ok(srid),
            Some(CustomCrsState::Dropped) => Err(UnknownSrid::Dropped { code: srid.code() }),
            None => builtin_lookup(srid.code())
                .map(|_| srid)
                .ok_or(UnknownSrid::Undefined { code: srid.code() }),
        }
    }

    /// Resolve a CRS name: `EPSG:n`, `NODEDB:n`, `urn:ogc:def:crs:EPSG:[v]:n`,
    /// `http://www.opengis.net/def/crs/EPSG/0/n`, the OGC CRS84 URN or URL
    /// (WGS 84), or `SR-ORG:6842` (MODIS sinusoidal).
    pub fn resolve_name(&self, name: &str) -> Result<Srid, UnknownSrid> {
        let code =
            code_from_name(name).ok_or_else(|| UnknownSrid::UnknownName { name: name.into() })?;
        self.resolve_code(code)
    }

    /// Whether `srid` is geographic or projected, without parsing a
    /// built-in definition.
    pub fn kind(&self, srid: Srid) -> Result<CrsKind, UnknownSrid> {
        let code = srid.code();
        match self.custom_set().get(srid) {
            Some(CustomCrsState::Live(def)) => Ok(def.kind),
            Some(CustomCrsState::Dropped) => Err(UnknownSrid::Dropped { code }),
            None => builtin_lookup(code)
                .map(|e| e.kind)
                .ok_or(UnknownSrid::Undefined { code }),
        }
    }

    /// A transformer from `from` to `to`. [`Transformer::apply_at`] maps
    /// coordinates at a given coordinate epoch, for time-dependent datum
    /// shifts.
    pub fn transformer(&self, from: Srid, to: Srid) -> Result<Transformer, CrsError> {
        let source = self.resolve(from)?;
        let target = self.resolve(to)?;
        Ok(Transformer::new(source, target))
    }
}

impl SridResolver for CrsContext<'_> {
    fn resolve_u32(&self, code: u32) -> Result<Srid, UnknownSrid> {
        CrsContext::resolve_u32(self, code)
    }

    fn kind(&self, srid: Srid) -> Result<CrsKind, UnknownSrid> {
        CrsContext::kind(self, srid)
    }

    fn resolve_name(&self, name: &str) -> Result<Srid, UnknownSrid> {
        CrsContext::resolve_name(self, name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::method::ProjectionDef;
    use crate::parse::parse_proj_string;

    fn custom_set() -> CustomCrsSet {
        let mut set = CustomCrsSet::new();
        let live = Srid::new(910_001).unwrap();
        set.define(parse_proj_string(live, "user", "+proj=tmerc +lon_0=102").unwrap())
            .unwrap();
        let dropped = Srid::new(910_002).unwrap();
        set.define(parse_proj_string(dropped, "gone", "+proj=longlat").unwrap())
            .unwrap();
        set.drop_crs(dropped).unwrap();
        set
    }

    #[test]
    fn custom_live_and_dropped() {
        let set = custom_set();
        let ctx = CrsContext::new(&set);
        let live = ctx.resolve(Srid::new(910_001).unwrap()).unwrap();
        assert_eq!(&*live.name, "user");
        assert_eq!(
            ctx.resolve(Srid::new(910_002).unwrap()),
            Err(UnknownSrid::Dropped { code: 910_002 })
        );
        assert_eq!(
            ctx.resolve_code(910_002),
            Err(UnknownSrid::Dropped { code: 910_002 })
        );
        assert_eq!(ctx.resolve_code(910_001).map(Srid::code), Ok(910_001));
        assert_eq!(
            ctx.kind(Srid::new(910_001).unwrap()),
            Ok(CrsKind::Projected)
        );
    }

    #[test]
    fn shared_context_resolves_its_set() {
        let ctx = CrsContext::shared(Arc::new(custom_set()));
        assert_eq!(ctx.resolve_u32(910_001).map(Srid::code), Ok(910_001));
        assert_eq!(
            ctx.resolve_u32(910_002),
            Err(UnknownSrid::Dropped { code: 910_002 })
        );
        let fork = ctx.fork();
        assert_eq!(fork.custom_set().len(), 2);
        let resolver: &dyn SridResolver = &ctx;
        assert_eq!(resolver.resolve_name("EPSG:4326"), Ok(Srid::WGS84));
        assert_eq!(
            resolver.kind(Srid::new(910_001).unwrap()),
            Ok(CrsKind::Projected)
        );
    }

    #[test]
    fn memo_returns_the_same_arc() {
        let set = custom_set();
        let ctx = CrsContext::new(&set);
        let srid = Srid::new(910_001).unwrap();
        let a = ctx.resolve(srid).unwrap();
        let b = ctx.resolve(srid).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn codes_out_of_range_and_undefined() {
        let ctx = CrsContext::builtin_only();
        assert_eq!(
            ctx.resolve_code(0),
            Err(UnknownSrid::OutOfRange { code: 0 })
        );
        assert_eq!(
            ctx.resolve_code(-4326),
            Err(UnknownSrid::OutOfRange { code: -4326 })
        );
        assert_eq!(
            ctx.resolve_code(i64::from(Srid::MAX_CODE) + 1),
            Err(UnknownSrid::OutOfRange {
                code: i64::from(Srid::MAX_CODE) + 1
            })
        );
        assert_eq!(
            ctx.resolve_code(910_777),
            Err(UnknownSrid::Undefined { code: 910_777 })
        );
        assert!(matches!(
            ctx.resolve_name("ESRI:102100"),
            Err(UnknownSrid::UnknownName { .. })
        ));
    }

    #[test]
    fn builtin_wgs84_and_names() {
        let ctx = CrsContext::builtin_only();
        let def = ctx.resolve(Srid::WGS84).unwrap();
        assert!(def.is_geographic());
        assert_eq!(
            ctx.resolve_name("urn:ogc:def:crs:OGC:1.3:CRS84"),
            Ok(Srid::WGS84)
        );
        assert_eq!(ctx.resolve_name("SR-ORG:6842"), Ok(Srid::MODIS_SINUSOIDAL));
        assert_eq!(ctx.resolve_name("EPSG:32648").map(Srid::code), Ok(32648));
    }

    /// NodeDB's own built-ins resolve: MODIS sinusoidal and Himawari
    /// geostationary.
    #[test]
    fn nodedb_builtins_resolve() {
        let ctx = CrsContext::builtin_only();
        assert_eq!(ctx.kind(Srid::MODIS_SINUSOIDAL), Ok(CrsKind::Projected));
        let modis = ctx.resolve(Srid::MODIS_SINUSOIDAL).unwrap();
        assert!(matches!(
            modis.projection,
            Some(ProjectionDef::Sinusoidal(_))
        ));
        assert!(modis.datum.ellipsoid.is_sphere());
        let himawari = ctx.resolve(Srid::new(990_002).unwrap()).unwrap();
        let Some(ProjectionDef::GeostationarySatellite(geos)) = himawari.projection else {
            panic!("expected geos: {himawari:?}");
        };
        assert_eq!(geos.h, 35_785_863.0);
        assert_eq!(geos.lon0_deg, 140.7);
        // The sub-satellite point maps to the grid origin.
        let t = ctx
            .transformer(Srid::WGS84, Srid::new(990_002).unwrap())
            .unwrap();
        let origin = t.apply([140.7, 0.0, 0.0]).unwrap();
        assert!(
            origin[0].abs() < 1e-6 && origin[1].abs() < 1e-6,
            "{origin:?}"
        );
        let t = ctx
            .transformer(Srid::WGS84, Srid::MODIS_SINUSOIDAL)
            .unwrap();
        let origin = t.apply([0.0, 0.0, 0.0]).unwrap();
        assert!(
            origin[0].abs() < 1e-6 && origin[1].abs() < 1e-6,
            "{origin:?}"
        );
    }

    /// A built-in carries its EPSG datum code, so a projected CRS and its
    /// geographic base share a datum.
    #[test]
    fn builtin_datum_codes() {
        let ctx = CrsContext::builtin_only();
        let gdm2000 = ctx.resolve(Srid::new(4742).unwrap()).unwrap();
        let rso = ctx.resolve(Srid::new(3375).unwrap()).unwrap();
        assert!(gdm2000.datum.epsg_code.is_some());
        assert_eq!(gdm2000.datum.epsg_code, rso.datum.epsg_code);
        assert!(gdm2000.datum.same_datum(&rso.datum));
    }
}
