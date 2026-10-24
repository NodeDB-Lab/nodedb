// SPDX-License-Identifier: Apache-2.0

pub mod array_schema;
pub mod attr_meta;
pub mod attr_spec;
pub mod builder;
pub mod cell_check;
pub mod cell_order;
pub mod dim_spec;
pub mod georef;
pub mod validation;

pub use array_schema::ArraySchema;
pub use attr_meta::{AttrMeta, AttrMetaError};
pub use attr_spec::{AttrSpec, AttrType};
pub use builder::ArraySchemaBuilder;
pub use cell_order::{CellOrder, TileOrder};
pub use dim_spec::{DimSpec, DimType};
pub use georef::{ArrayGeoref, GeoTransform};
