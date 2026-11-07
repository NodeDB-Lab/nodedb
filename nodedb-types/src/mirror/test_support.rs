// SPDX-License-Identifier: Apache-2.0

//! Round-trip helpers shared by the mirror unit tests.

use serde::{Deserialize, Serialize};

pub(super) fn round_trip_msgpack<T>(val: &T) -> T
where
    T: zerompk::ToMessagePack + for<'a> zerompk::FromMessagePack<'a>,
{
    let bytes = zerompk::to_msgpack_vec(val).expect("serialize");
    zerompk::from_msgpack(&bytes).expect("deserialize")
}

pub(super) fn round_trip_serde<T>(val: &T) -> T
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    let json = sonic_rs::to_string(val).expect("serde serialize");
    sonic_rs::from_str(&json).expect("serde deserialize")
}
