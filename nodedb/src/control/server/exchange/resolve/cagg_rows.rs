// SPDX-License-Identifier: BUSL-1.1

//! The rows of a continuous aggregate read as a relation.
//!
//! Every bucket of an aggregate lives on the core that owns its root
//! source's vShard, the core that flushes the source. The coordinator reads
//! the finalized rows there, through the gateway, so a read on any node of a
//! cluster answers from the vShard's owner.

use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::owner_dispatch::dispatch_to_owner;
use crate::control::server::shared::ddl::neutral::continuous_agg::refresh::resolve_root;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Finalized rows of continuous aggregate `name`, one msgpack map per
/// bucket and group, or `None` when `name` is not a continuous aggregate
/// of the caller's tenant in `database_id`.
///
/// `system_time` is the cut the read sees. Inside a transaction block the
/// planner pins it to the block's snapshot, so every read of the block sees
/// the rows the aggregate held at the snapshot.
pub(super) async fn continuous_aggregate_rows(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    system_time: nodedb_types::SystemTimeScope,
) -> crate::Result<Option<Vec<Vec<u8>>>> {
    let tenant = identity.tenant_id.as_u64();
    let db = database_id.as_u64();
    let catalog = state.credentials.catalog();
    let Some(stored) = catalog.get_continuous_aggregate(db, tenant, name)? else {
        return Ok(None);
    };
    let root = resolve_root(name, stored.source, |agg| {
        catalog
            .get_continuous_aggregate(db, tenant, agg)
            .map(|s| s.map(|s| s.source))
    })?;
    let plan = PhysicalPlan::Timeseries(TimeseriesOp::ReadAggregate {
        collection: nodedb_types::QualifiedCollection::new(database_id, &root),
        aggregate: name.to_string(),
        system_time,
    });
    let payload = dispatch_to_owner(
        state,
        identity.tenant_id,
        database_id,
        &root,
        plan,
        "continuous_aggregate",
    )
    .await?;
    split_rows(name, &payload).map(Some)
}

/// One msgpack value per element of the msgpack array `payload`. An empty
/// payload holds no rows. A payload that is not a whole array is refused.
fn split_rows(name: &str, payload: &[u8]) -> crate::Result<Vec<Vec<u8>>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    let malformed = |detail: &str| crate::Error::Codec {
        detail: format!("rows of continuous aggregate '{name}': {detail}"),
    };
    let Some((count, mut pos)) = nodedb_query::msgpack_scan::array_header(payload, 0) else {
        return Err(malformed("the payload is not a msgpack array"));
    };
    let mut rows = Vec::with_capacity(count.min(payload.len()));
    for _ in 0..count {
        let Some(next) = nodedb_query::msgpack_scan::skip_value(payload, pos) else {
            return Err(malformed("a row is truncated"));
        };
        rows.push(payload[pos..next].to_vec());
        pos = next;
    }
    if pos != payload.len() {
        return Err(malformed("bytes follow the row array"));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_split_into_whole_values() {
        let rows = rmpv::Value::Array(vec![
            rmpv::Value::Map(vec![(rmpv::Value::from("c"), rmpv::Value::from(2))]),
            rmpv::Value::Map(vec![(rmpv::Value::from("c"), rmpv::Value::from(3))]),
        ]);
        let mut payload = Vec::new();
        rmpv::encode::write_value(&mut payload, &rows).expect("encode");
        let split = split_rows("v", &payload).expect("split");
        assert_eq!(split.len(), 2);
        let first = rmpv::decode::read_value(&mut split[0].as_slice()).expect("decode");
        assert_eq!(first, rows.as_array().expect("array")[0]);
        assert!(split_rows("v", &[]).expect("empty").is_empty());
        assert!(split_rows("v", &payload[..payload.len() - 1]).is_err());
    }
}
