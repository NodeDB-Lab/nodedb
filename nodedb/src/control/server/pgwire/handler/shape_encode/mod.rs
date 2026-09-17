// SPDX-License-Identifier: BUSL-1.1

//! Encode a protocol-neutral [`ShapedRows`](crate::control::server::response_shape::types::ShapedRows)
//! into pgwire `DataRow`s and a `Response::Query`.

pub mod cell;
pub mod response;

pub(in crate::control::server::pgwire) use cell::encode_cell;
pub(in crate::control::server::pgwire) use response::{encode_shaped_row, shaped_query_response};
