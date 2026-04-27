// Structural sparse-tile codec. Dense tiles retain zerompk serialization;
// this module covers sparse tiles only. See tile_encode / tile_decode for
// the entry points used by the segment writer and reader.
pub mod column_codec;
pub mod coord_delta;
pub mod coord_rle;
pub mod limits;
pub mod tag;
pub mod tile_decode;
pub mod tile_encode;

pub use column_codec::{
    decode_attr_col, decode_row_kinds, decode_surrogates, decode_timestamps_col, encode_attr_col,
    encode_row_kinds, encode_surrogates, encode_timestamps_col,
};
pub use coord_delta::{decode_coord_axis, encode_coord_axis};
pub use coord_rle::{decode_coord_axis_rle, encode_coord_axis_rle};
pub use tag::{CodecTag, peek_tag};
pub use tile_decode::decode_sparse_tile;
pub use tile_encode::encode_sparse_tile;
