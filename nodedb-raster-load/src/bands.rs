// SPDX-License-Identifier: Apache-2.0

//! Which array attr each band loads into, and the type rule.
//!
//! The bands of a load are every source's bands in source order: source 1's
//! bands, then source 2's, and so on. `BANDS (a, SKIP, b)` maps those bands
//! in order: band 1 to `a`, band 2 nowhere, band 3 to `b`. Without `BANDS`,
//! band `i` loads into attr `i`. A band loads only into an attr that holds
//! every value of its sample type exactly. An attr no band loads into must
//! be nullable, and every cell holds NULL there.

use nodedb_raster::SampleType;

use crate::error::RasterLoadError;
use crate::options::BandTarget;
use crate::view::{ArrayView, AttrType};

/// Where one attr's value comes from.
#[derive(Debug, Clone, PartialEq)]
pub enum AttrSource {
    /// No band loads into the attr. It is nullable.
    Null,
    /// Band `band` of the load loads into the attr.
    Band {
        band: usize,
        /// Whether the attr holds floats, so integer samples widen to `f64`.
        float: bool,
        nullable: bool,
        name: String,
    },
}

/// The attr sources in schema order, and the bands that load.
#[derive(Debug)]
pub struct BandMap {
    pub attrs: Vec<AttrSource>,
    pub loaded: Vec<usize>,
}

/// Map the load's bands, one sample type each, onto the attrs of `view`.
pub fn map_bands(
    view: &ArrayView,
    samples: &[SampleType],
    bands: Option<&[BandTarget]>,
) -> Result<BandMap, RasterLoadError> {
    let targets = band_targets(view, samples.len(), bands)?;
    let mut attrs = vec![AttrSource::Null; view.attrs.len()];
    let mut loaded = Vec::with_capacity(targets.len());
    for (band, (target, sample)) in targets.into_iter().zip(samples).enumerate() {
        let Some(index) = target else { continue };
        let Some(attr) = view.attrs.get(index) else {
            continue;
        };
        if !widens(*sample, attr.dtype) {
            return Err(RasterLoadError::BandType {
                band: band + 1,
                sample: *sample,
                attr: attr.name.clone(),
                attr_type: attr.dtype.sql_name(),
            });
        }
        if let Some(slot) = attrs.get_mut(index) {
            *slot = AttrSource::Band {
                band,
                float: attr.dtype.is_float(),
                nullable: attr.nullable,
                name: attr.name.clone(),
            };
        }
        loaded.push(band);
    }
    if loaded.is_empty() {
        return Err(RasterLoadError::NoBandLoads);
    }
    for (attr, source) in view.attrs.iter().zip(&attrs) {
        if *source == AttrSource::Null && !attr.nullable {
            return Err(RasterLoadError::UnmappedNotNull {
                attr: attr.name.clone(),
            });
        }
    }
    Ok(BandMap { attrs, loaded })
}

/// The attr index each band loads into, `None` for a skipped band.
fn band_targets(
    view: &ArrayView,
    count: usize,
    bands: Option<&[BandTarget]>,
) -> Result<Vec<Option<usize>>, RasterLoadError> {
    let Some(bands) = bands else {
        if count > view.attrs.len() {
            return Err(RasterLoadError::BandCount {
                array: view.name.clone(),
                bands: count,
                attrs: view.attrs.len(),
            });
        }
        return Ok((0..count).map(Some).collect());
    };
    if bands.len() != count {
        return Err(RasterLoadError::BandListLength {
            listed: bands.len(),
            bands: count,
        });
    }
    bands
        .iter()
        .map(|band| match band {
            BandTarget::Skip => Ok(None),
            BandTarget::Attr(name) => view
                .attrs
                .iter()
                .position(|attr| attr.name.eq_ignore_ascii_case(name))
                .map(Some)
                .ok_or_else(|| RasterLoadError::UnknownAttr {
                    array: view.name.clone(),
                    name: name.clone(),
                }),
        })
        .collect()
}

/// Whether every `sample` value converts to `attr` without loss.
fn widens(sample: SampleType, attr: AttrType) -> bool {
    use AttrType as A;
    match sample {
        SampleType::U8 => matches!(
            attr,
            A::UInt8
                | A::Int16
                | A::UInt16
                | A::Int32
                | A::UInt32
                | A::Int64
                | A::Float32
                | A::Float64
        ),
        SampleType::I8 => matches!(
            attr,
            A::Int8 | A::Int16 | A::Int32 | A::Int64 | A::Float32 | A::Float64
        ),
        SampleType::U16 => matches!(
            attr,
            A::UInt16 | A::Int32 | A::UInt32 | A::Int64 | A::Float32 | A::Float64
        ),
        SampleType::I16 => matches!(
            attr,
            A::Int16 | A::Int32 | A::Int64 | A::Float32 | A::Float64
        ),
        SampleType::U32 => matches!(attr, A::UInt32 | A::Int64 | A::Float64),
        SampleType::I32 => matches!(attr, A::Int32 | A::Int64 | A::Float64),
        SampleType::F32 => matches!(attr, A::Float32 | A::Float64),
        SampleType::F64 => matches!(attr, A::Float64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::view::ViewAttr;

    fn view(attrs: &[(&str, AttrType, bool)]) -> ArrayView {
        ArrayView {
            name: "scene".into(),
            dims: Vec::new(),
            attrs: attrs
                .iter()
                .map(|(name, dtype, nullable)| ViewAttr {
                    name: (*name).into(),
                    dtype: *dtype,
                    nullable: *nullable,
                    meta: Default::default(),
                })
                .collect(),
            georef: None,
        }
    }

    const U16X2: [SampleType; 2] = [SampleType::U16, SampleType::U16];

    #[test]
    fn widening_is_lossless_only() {
        use AttrType as A;
        assert!(widens(SampleType::U8, A::UInt8));
        assert!(widens(SampleType::U16, A::Float32));
        assert!(!widens(SampleType::U16, A::Int16));
        assert!(!widens(SampleType::I16, A::UInt16));
        assert!(!widens(SampleType::U32, A::Float32));
        assert!(!widens(SampleType::F32, A::Int64));
        assert!(!widens(SampleType::F64, A::Float32));
        assert!(!widens(SampleType::U8, A::String));
        assert!(widens(SampleType::I8, A::Int8));
        assert!(!widens(SampleType::I8, A::UInt8));
        assert!(!widens(SampleType::U8, A::Int8));
        assert!(widens(SampleType::I32, A::Int32));
        assert!(!widens(SampleType::I32, A::Float32));
        assert!(!widens(SampleType::I32, A::UInt32));
    }

    #[test]
    fn bands_load_in_order_without_a_list() {
        let v = view(&[
            ("b1", AttrType::UInt16, false),
            ("b2", AttrType::Float64, true),
            ("extra", AttrType::Int64, true),
        ]);
        let map = map_bands(&v, &U16X2, None).expect("maps");
        assert_eq!(map.loaded, vec![0, 1]);
        assert!(matches!(
            map.attrs[1],
            AttrSource::Band {
                band: 1,
                float: true,
                ..
            }
        ));
        assert_eq!(map.attrs[2], AttrSource::Null);
    }

    #[test]
    fn a_band_list_skips_and_reorders() {
        let v = view(&[
            ("red", AttrType::UInt16, true),
            ("nir", AttrType::UInt16, true),
        ]);
        let list = [
            BandTarget::Attr("nir".into()),
            BandTarget::Skip,
            BandTarget::Attr("red".into()),
        ];
        let samples = [SampleType::U16; 3];
        let map = map_bands(&v, &samples, Some(&list)).expect("maps");
        assert_eq!(map.loaded, vec![0, 2]);
        assert!(matches!(map.attrs[0], AttrSource::Band { band: 2, .. }));
        assert!(matches!(map.attrs[1], AttrSource::Band { band: 0, .. }));
    }

    #[test]
    fn each_band_keeps_its_own_sample_type() {
        let v = view(&[
            ("red", AttrType::UInt16, true),
            ("qa", AttrType::UInt8, true),
        ]);
        let mixed = [SampleType::U16, SampleType::U8];
        assert!(map_bands(&v, &mixed, None).is_ok());
        let err = map_bands(&v, &U16X2, None).expect_err("u16 into UINT8");
        assert_eq!(err.sqlstate(), "42804");
        assert!(err.to_string().contains("band 2"), "{err}");
    }

    #[test]
    fn refusals_carry_their_sqlstate() {
        let narrow = view(&[("b1", AttrType::UInt8, true)]);
        let one = [SampleType::U16];
        let err = map_bands(&narrow, &one, None).expect_err("u16 into UINT8");
        assert_eq!(err.sqlstate(), "42804");

        let strict = view(&[
            ("b1", AttrType::UInt16, true),
            ("qa", AttrType::UInt16, false),
        ]);
        let err = map_bands(&strict, &one, None).expect_err("qa unmapped");
        assert_eq!(err.sqlstate(), "23502");

        let single = view(&[("b1", AttrType::UInt16, true)]);
        let err = map_bands(&single, &U16X2, None).expect_err("2 bands, 1 attr");
        assert_eq!(err.sqlstate(), "22023");
        let short = [BandTarget::Attr("b1".into())];
        let err = map_bands(&single, &U16X2, Some(&short)).expect_err("short list");
        assert_eq!(err.sqlstate(), "22023");
        let unknown = [BandTarget::Attr("b9".into())];
        let err = map_bands(&single, &one, Some(&unknown)).expect_err("no attr b9");
        assert_eq!(err.sqlstate(), "42703");
    }
}
