// SPDX-License-Identifier: Apache-2.0

//! The SRID of an R-tree with the bbox space it fixes.

use nodedb_crs::{CrsContext, UnknownSrid};
use nodedb_types::BboxSpace;
use nodedb_types::geometry::Srid;

/// An SRID and its bbox space, resolved once so the tree's hot insert path
/// never consults a CRS registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TreeSrid {
    pub srid: Srid,
    pub space: BboxSpace,
}

impl TreeSrid {
    /// WGS 84, geographic.
    pub const WGS84: TreeSrid = TreeSrid {
        srid: Srid::WGS84,
        space: BboxSpace::Geographic,
    };

    /// `srid` with the space its kind in `ctx` gives.
    pub fn resolve(srid: Srid, ctx: &CrsContext<'_>) -> Result<TreeSrid, UnknownSrid> {
        Ok(TreeSrid {
            srid,
            space: BboxSpace::of(ctx, srid)?,
        })
    }
}

impl Default for TreeSrid {
    fn default() -> Self {
        TreeSrid::WGS84
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_follows_the_crs_kind() {
        let ctx = CrsContext::builtin_only();
        let utm = Srid::new(32650).unwrap();
        assert_eq!(
            TreeSrid::resolve(utm, &ctx),
            Ok(TreeSrid {
                srid: utm,
                space: BboxSpace::Planar
            })
        );
        let gdm2000 = Srid::new(4742).unwrap();
        assert_eq!(
            TreeSrid::resolve(gdm2000, &ctx).map(|t| t.space),
            Ok(BboxSpace::Geographic)
        );
        assert_eq!(
            TreeSrid::resolve(Srid::new(999).unwrap(), &ctx),
            Err(UnknownSrid::Undefined { code: 999 })
        );
    }
}
