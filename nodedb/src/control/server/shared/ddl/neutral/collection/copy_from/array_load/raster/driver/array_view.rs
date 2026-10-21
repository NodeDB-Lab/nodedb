// SPDX-License-Identifier: BUSL-1.1

//! The catalog's array view in the plain form the load planner reads, and
//! the planner's rows and adopted metadata back in the catalog's form.

use nodedb_raster::GeoTransform;
use nodedb_raster_load::{
    AdoptedMeta, ArrayView, AttrLiteral, AttrMeta, AttrType, CoordLiteral, DimType, DomainBound,
    LoadRow, ViewAttr, ViewDim, ViewGeoref,
};
use nodedb_sql::catalog::ArrayCatalogView;
use nodedb_sql::types_array::{
    ArrayAttrAst, ArrayAttrLiteral, ArrayAttrMetaAlter, ArrayAttrMetaAst, ArrayAttrMetaChange,
    ArrayAttrType, ArrayCoordLiteral, ArrayDimAst, ArrayDimType, ArrayDomainBound, ArrayGeorefAst,
    ArrayInsertRow,
};

/// The planner's view of `view`.
pub(crate) fn array_view(view: &ArrayCatalogView) -> ArrayView {
    ArrayView {
        name: view.name.clone(),
        dims: view.dims.iter().map(view_dim).collect(),
        attrs: view.attrs.iter().map(view_attr).collect(),
        georef: view.georef.as_ref().map(view_georef),
    }
}

/// The planner's form of a georef.
pub(crate) fn view_georef(georef: &ArrayGeorefAst) -> ViewGeoref {
    ViewGeoref {
        srid: georef.srid,
        x_dim: georef.x_dim.clone(),
        y_dim: georef.y_dim.clone(),
        transform: georef.transform.map(GeoTransform),
    }
}

/// A planner row as an array insert row.
pub(crate) fn insert_row(row: LoadRow) -> ArrayInsertRow {
    ArrayInsertRow {
        coords: row
            .coords
            .into_iter()
            .map(|coord| match coord {
                CoordLiteral::Int64(v) => ArrayCoordLiteral::Int64(v),
                CoordLiteral::Float64(v) => ArrayCoordLiteral::Float64(v),
            })
            .collect(),
        attrs: row
            .attrs
            .into_iter()
            .map(|attr| match attr {
                AttrLiteral::Int64(v) => ArrayAttrLiteral::Int64(v),
                AttrLiteral::Float64(v) => ArrayAttrLiteral::Float64(v),
                AttrLiteral::Null => ArrayAttrLiteral::Null,
            })
            .collect(),
    }
}

/// Adopted metadata as the `ALTER ATTR ... SET` it makes.
pub(crate) fn meta_alter(adopted: &AdoptedMeta) -> ArrayAttrMetaAlter {
    let meta = &adopted.meta;
    ArrayAttrMetaAlter {
        attr: adopted.attr.clone(),
        change: ArrayAttrMetaChange::Set(ArrayAttrMetaAst {
            nodata: meta.nodata,
            scale: meta.scale,
            offset: meta.offset,
            units: meta.units.clone(),
        }),
    }
}

fn view_dim(dim: &ArrayDimAst) -> ViewDim {
    ViewDim {
        name: dim.name.clone(),
        dtype: match dim.dtype {
            ArrayDimType::Int64 => DimType::Int64,
            ArrayDimType::Float64 => DimType::Float64,
            ArrayDimType::TimestampMs => DimType::TimestampMs,
            ArrayDimType::String => DimType::String,
        },
        lo: bound(&dim.lo),
        hi: bound(&dim.hi),
    }
}

fn bound(bound: &ArrayDomainBound) -> DomainBound {
    match bound {
        ArrayDomainBound::Int64(v) => DomainBound::Int64(*v),
        ArrayDomainBound::Float64(v) => DomainBound::Float64(*v),
        ArrayDomainBound::TimestampMs(v) => DomainBound::TimestampMs(*v),
        ArrayDomainBound::String(v) => DomainBound::String(v.clone()),
    }
}

fn view_attr(attr: &ArrayAttrAst) -> ViewAttr {
    ViewAttr {
        name: attr.name.clone(),
        dtype: attr_type(attr.dtype),
        nullable: attr.nullable,
        meta: AttrMeta {
            nodata: attr.meta.nodata,
            scale: attr.meta.scale,
            offset: attr.meta.offset,
            units: attr.meta.units.clone(),
        },
    }
}

fn attr_type(dtype: ArrayAttrType) -> AttrType {
    match dtype {
        ArrayAttrType::Int8 => AttrType::Int8,
        ArrayAttrType::UInt8 => AttrType::UInt8,
        ArrayAttrType::Int16 => AttrType::Int16,
        ArrayAttrType::UInt16 => AttrType::UInt16,
        ArrayAttrType::Int32 => AttrType::Int32,
        ArrayAttrType::UInt32 => AttrType::UInt32,
        ArrayAttrType::Int64 => AttrType::Int64,
        ArrayAttrType::Float32 => AttrType::Float32,
        ArrayAttrType::Float64 => AttrType::Float64,
        ArrayAttrType::String => AttrType::String,
        ArrayAttrType::Bytes => AttrType::Bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_view_and_rows_carry_over() {
        let catalog = ArrayCatalogView {
            name: "scene".into(),
            dims: vec![ArrayDimAst {
                name: "t".into(),
                dtype: ArrayDimType::TimestampMs,
                lo: ArrayDomainBound::TimestampMs(0),
                hi: ArrayDomainBound::TimestampMs(9),
            }],
            attrs: vec![ArrayAttrAst {
                name: "red".into(),
                dtype: ArrayAttrType::UInt16,
                nullable: true,
                meta: ArrayAttrMetaAst {
                    nodata: Some(0.0),
                    ..Default::default()
                },
            }],
            tile_extents: vec![8],
            georef: Some(ArrayGeorefAst {
                srid: 32_647,
                x_dim: "col".into(),
                y_dim: "row".into(),
                transform: Some([0.0, 1.0, 0.0, 0.0, 0.0, -1.0]),
            }),
        };
        let view = array_view(&catalog);
        assert_eq!(view.dims[0].dtype, DimType::TimestampMs);
        assert_eq!(view.dims[0].hi, DomainBound::TimestampMs(9));
        assert_eq!(view.attrs[0].dtype, AttrType::UInt16);
        assert_eq!(view.attrs[0].meta.nodata, Some(0.0));
        let georef = view.georef.expect("georef");
        assert_eq!(
            georef.transform,
            Some(GeoTransform([0.0, 1.0, 0.0, 0.0, 0.0, -1.0]))
        );

        let row = insert_row(LoadRow {
            coords: vec![CoordLiteral::Int64(3), CoordLiteral::Float64(1.5)],
            attrs: vec![AttrLiteral::Int64(7), AttrLiteral::Null],
        });
        assert_eq!(
            row.coords,
            vec![ArrayCoordLiteral::Int64(3), ArrayCoordLiteral::Float64(1.5)]
        );
        assert_eq!(
            row.attrs,
            vec![ArrayAttrLiteral::Int64(7), ArrayAttrLiteral::Null]
        );

        let alter = meta_alter(&AdoptedMeta {
            attr: "red".into(),
            meta: AttrMeta {
                scale: Some(2.0),
                ..Default::default()
            },
        });
        assert_eq!(alter.attr, "red");
        assert!(matches!(
            alter.change,
            ArrayAttrMetaChange::Set(ArrayAttrMetaAst {
                scale: Some(2.0),
                ..
            })
        ));
    }
}
