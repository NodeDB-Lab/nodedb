// SPDX-License-Identifier: BUSL-1.1

//! Admission of the native batch opcodes: the tenant quota, the payload
//! decode and the configured `max_batch_size`. Every batch opcode runs these
//! before it writes.

use nodedb_types::protocol::{Limits, NativeResponse, TextFields};

use crate::control::server::native::sqlstate_code::sqlstate_error;

use super::{DispatchCtx, error_to_native, error_to_native_with_sqlstate};

/// The request's `collection` as sent and its `payload` decoded as `B`, after
/// the tenant quota check. A missing field, a refused quota or an undecodable
/// payload is the boxed error response to send.
pub(super) fn decode_batch<'f, B>(
    ctx: &DispatchCtx<'_>,
    op: &str,
    seq: u64,
    fields: &'f TextFields,
) -> Result<(&'f str, B), Box<NativeResponse>>
where
    B: for<'a> zerompk::FromMessagePack<'a>,
{
    let Some(collection) = fields.collection.as_deref() else {
        return Err(Box::new(sqlstate_error(
            seq,
            "42601",
            format!("{op}: missing 'collection' field"),
        )));
    };
    let Some(payload) = fields.payload.as_deref() else {
        return Err(Box::new(sqlstate_error(
            seq,
            "42601",
            format!("{op}: missing 'payload' field"),
        )));
    };
    if let Err(e) = ctx.state.check_tenant_quota(ctx.tenant_id()) {
        return Err(Box::new(error_to_native(seq, &e)));
    }
    let batch = zerompk::from_msgpack(payload).map_err(|e| {
        Box::new(sqlstate_error(
            seq,
            "22P02",
            format!("{op}: payload is not an {op} payload: {e}"),
        ))
    })?;
    Ok((collection, batch))
}

/// The refusal for a request of `count` items over the configured
/// `max_batch_size`. `limit_name` names the items in the error. `None` when
/// no cap is configured or `count` is within it.
pub(super) fn batch_size_refusal(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    limit_name: &'static str,
    count: usize,
) -> Option<NativeResponse> {
    let error = over_batch_limit(&ctx.state.limits, limit_name, count)?;
    Some(error_to_native_with_sqlstate(seq, "0A000", &error))
}

/// The error of `count` items over `limits.max_batch_size`, if they are over.
fn over_batch_limit(
    limits: &Limits,
    limit_name: &'static str,
    count: usize,
) -> Option<crate::Error> {
    let max = u64::from(limits.max_batch_size?);
    let value = count as u64;
    if value <= max {
        return None;
    }
    Some(crate::Error::LimitExceeded {
        limit_name,
        value,
        max,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capped(max: u32) -> Limits {
        Limits {
            max_batch_size: Some(max),
            ..Limits::default()
        }
    }

    #[test]
    fn no_cap_admits_any_count() {
        assert!(over_batch_limit(&Limits::default(), "max_batch_size(cells)", 1 << 40).is_none());
    }

    #[test]
    fn a_count_at_the_cap_is_admitted() {
        assert!(over_batch_limit(&capped(10), "max_batch_size(cells)", 10).is_none());
    }

    #[test]
    fn a_count_over_the_cap_names_the_limit_the_count_and_the_cap() {
        let error = over_batch_limit(&capped(10), "max_batch_size(cells)", 11).expect("over");
        assert!(matches!(
            error,
            crate::Error::LimitExceeded {
                limit_name: "max_batch_size(cells)",
                value: 11,
                max: 10,
            }
        ));
    }
}
