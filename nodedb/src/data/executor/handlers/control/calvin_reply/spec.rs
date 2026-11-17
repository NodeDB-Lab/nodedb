// SPDX-License-Identifier: BUSL-1.1

//! A staged reply as the redo entry carries it, and back.
//!
//! The leader's resolve turns the reply its stage decided into a
//! [`CalvinReplySpec`]. Every replica's install turns that spec back into a
//! [`CalvinReply`] and answers it.

use nodedb_physical::physical_plan::CalvinReplySpec;

use super::reply::CalvinReply;

impl From<&CalvinReply> for CalvinReplySpec {
    fn from(reply: &CalvinReply) -> Self {
        match reply {
            CalvinReply::Count(payload) => Self::Count(payload.clone()),
            CalvinReply::Rows(payload) => Self::Rows(payload.clone()),
            CalvinReply::Frozen => Self::Frozen,
        }
    }
}

impl From<CalvinReplySpec> for CalvinReply {
    fn from(spec: CalvinReplySpec) -> Self {
        match spec {
            CalvinReplySpec::Count(payload) => Self::Count(payload),
            CalvinReplySpec::Rows(payload) => Self::Rows(payload),
            CalvinReplySpec::Frozen => Self::Frozen,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(spec: CalvinReplySpec) {
        let reply = CalvinReply::from(spec.clone());
        assert_eq!(CalvinReplySpec::from(&reply), spec);
    }

    #[test]
    fn every_reply_shape_round_trips_through_its_spec() {
        round_trip(CalvinReplySpec::Count(vec![1, 2]));
        round_trip(CalvinReplySpec::Rows(vec![3]));
        round_trip(CalvinReplySpec::Frozen);
    }
}
