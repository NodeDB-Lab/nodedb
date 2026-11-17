// SPDX-License-Identifier: Apache-2.0

//! CRDT engine operations dispatched to the Data Plane.

pub mod collection;
pub mod fields_body;
pub mod op;
pub mod write_verb;

pub use fields_body::{decode_fields_body, encode_fields_body};
pub use op::CrdtOp;
pub use write_verb::CrdtWriteVerb;
