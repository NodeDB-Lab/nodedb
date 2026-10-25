// SPDX-License-Identifier: Apache-2.0

//! `ADOPT_METADATA true`: an attr with no metadata takes the nodata,
//! scale, offset and units the file declares for the band that loads into
//! it. An attr with any metadata set keeps all of it, so a load never
//! overrides a choice the schema made.

use crate::bands::AttrSource;
use crate::plan::RasterPlan;
use crate::source::SourceHeader;
use crate::view::{AdoptedMeta, ArrayView, AttrMeta};

/// The metadata changes adoption makes, one per attr that changes.
pub fn adopted_metadata(
    view: &ArrayView,
    plan: &RasterPlan,
    sources: &[SourceHeader],
) -> Vec<AdoptedMeta> {
    let mut changes = Vec::new();
    for (attr, source) in view.attrs.iter().zip(plan.attrs()) {
        let AttrSource::Band { band, .. } = source else {
            continue;
        };
        if attr.meta != AttrMeta::default() {
            continue;
        }
        let Some(info) = plan
            .band(*band)
            .and_then(|at| sources.get(at.source)?.bands.get(at.band))
        else {
            continue;
        };
        let meta = AttrMeta {
            nodata: info.nodata,
            scale: info.scale,
            offset: info.offset,
            units: info.units.clone(),
        };
        if meta != AttrMeta::default() {
            changes.push(AdoptedMeta {
                attr: attr.name.clone(),
                meta,
            });
        }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::LoadOptions;
    use crate::plan::tests::{georef, source, view};
    use nodedb_raster::{BandInfo, SampleType};

    #[test]
    fn only_attrs_without_metadata_adopt_the_band_metadata() {
        let mut info = BandInfo::plain(SampleType::U16);
        info.nodata = Some(0.0);
        info.scale = Some(2.75e-5);
        info.offset = Some(-0.2);
        info.units = Some("reflectance".into());
        let sources = [source("/B4.TIF", info.clone()), source("/B5.TIF", info)];
        let mut view = view();
        view.attrs[1].meta.scale = Some(1.0);
        let plan =
            RasterPlan::new(&view, &georef(), &sources, &LoadOptions::default()).expect("plans");
        let changes = adopted_metadata(&view, &plan, &sources);
        assert_eq!(changes.len(), 1, "nir has metadata; qa has no band");
        assert_eq!(changes[0].attr, "red");
        let meta = &changes[0].meta;
        assert_eq!(meta.scale, Some(2.75e-5));
        assert_eq!(meta.units.as_deref(), Some("reflectance"));
    }
}
