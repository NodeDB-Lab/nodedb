// SPDX-License-Identifier: BUSL-1.1

//! String-recognized streaming DDL arms: schedule/alert SHOW, change streams,
//! consumer groups, topics, stream/topic consumption, and pub/sub subscribe.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::alert;
use super::super::change_stream;
use super::super::consumer_group;
use super::super::schedule;
use super::super::stream_select;
use super::super::timeseries;
use super::super::topic;
use super::super::topic_subscribe;
use super::route_tokens::{RouteTokens, mentions};

pub(super) async fn try_string(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    database_id: DatabaseId,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // Schedule SHOW. `SHOW SCHEDULE HISTORY <name>` parses into a typed
    // `AutomationStmt::ShowScheduleHistory` and `SHOW SCHEDULES` into
    // `AutomationStmt::ShowSchedules`, but the router dispatches both from
    // the raw token slice by string prefix, before the parse gate (the
    // `SHOW SCHEDULE` prefix also captures the bare-singular `SHOW SCHEDULE`
    // input, which parses into no typed variant). The name comes from
    // `parts.get(3)`.
    if upper.starts_with("SHOW SCHEDULE HISTORY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let name = parts.get(3).copied().unwrap_or("");
        return Some(schedule::show_schedule_history(
            state,
            identity,
            database_id,
            name,
        ));
    }
    if upper.starts_with("SHOW SCHEDULE") {
        return Some(schedule::show_schedules(state, identity, database_id));
    }

    // Alert SHOW. `SHOW ALERT STATUS <name>` parses into a typed
    // `AutomationStmt::ShowAlertStatus` and `SHOW ALERTS` into
    // `AutomationStmt::ShowAlerts`, but the router dispatches both from the
    // raw token slice by string prefix, before the parse gate (the
    // `SHOW ALERT` prefix also captures the bare-singular `SHOW ALERT` input,
    // which parses into `ShowAlerts`). STATUS is checked first, and the name
    // comes from `parts.get(4)` (name after `ON`).
    if upper.starts_with("SHOW ALERT STATUS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let name = parts.get(4).copied().unwrap_or("");
        return Some(alert::show_alert_status(state, identity, database_id, name));
    }
    if upper.starts_with("SHOW ALERT") {
        return Some(alert::show_alerts(state, identity, database_id));
    }

    // Change streams: `SHOW CHANGE STREAM(S)`. This parses into a typed
    // `StreamViewStmt::ShowChangeStreams`, but the router dispatches it from
    // the raw SQL by string prefix, before the parse gate (the
    // `SHOW CHANGE STREAM` prefix captures both the plural
    // `SHOW CHANGE STREAMS` and the bare-singular input).
    if upper.starts_with("SHOW CHANGE STREAM") {
        return Some(change_stream::show_change_streams(
            state,
            identity,
            database_id,
        ));
    }

    // Consumer groups: `SHOW CONSUMER GROUPS ON <stream>`, `SHOW PARTITIONS ON
    // <stream>`, and `COMMIT OFFSET(S) …`. The router dispatches all four by
    // string prefix from the raw token slice, before the parse gate.
    // `SHOW CONSUMER GROUPS` parses into a typed
    // `StreamViewStmt::ShowConsumerGroups`, but string dispatch claims it
    // before any typed arm runs; `SHOW PARTITIONS` and `COMMIT OFFSET(S)`
    // parse into no typed variant at all.
    if upper.starts_with("SHOW CONSUMER GROUPS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(consumer_group::show_consumer_groups(
            state,
            identity,
            database_id,
            &parts,
        ));
    }
    // `SHOW PARTITIONS` has two grammars, told apart by the token after
    // PARTITIONS: `ON <stream>` lists a stream's consumer partitions, and
    // `FOR <timeseries>` lists a timeseries collection's partitions.
    if upper.starts_with("SHOW PARTITIONS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let form = parts.get(2).copied().unwrap_or_default();
        if form.eq_ignore_ascii_case("ON") {
            return Some(consumer_group::show_partitions(
                state,
                identity,
                database_id,
                &parts,
            ));
        }
        if form.eq_ignore_ascii_case("FOR") {
            return Some(timeseries::show_partitions(state, identity, database_id, &parts).await);
        }
        return Some(Err(DdlError::new(
            "42601",
            "syntax: SHOW PARTITIONS ON <stream> | SHOW PARTITIONS FOR <timeseries>",
        )));
    }
    if upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(consumer_group::commit_offset(state, identity, database_id, &parts).await);
    }

    // Topics: `CREATE TOPIC`, `DROP TOPIC`, `SHOW TOPIC(S)`, and `PUBLISH TO`.
    // None of these parse into any typed AST variant — the router dispatches
    // all four by string prefix from the raw token slice / SQL, before the
    // parse gate. The `SHOW TOPIC` prefix has no trailing space, so it
    // captures both `SHOW TOPICS` and the bare-singular input.
    if upper.starts_with("CREATE TOPIC ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(topic::create_topic(state, identity, database_id, &parts, sql).await);
    }
    if upper.starts_with("DROP TOPIC ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(topic::drop_topic(state, identity, database_id, &parts).await);
    }
    if upper.starts_with("SHOW TOPIC") {
        return Some(topic::show_topics(state, identity, database_id));
    }
    if upper.starts_with("PUBLISH TO ") {
        return Some(topic::handle_publish(state, identity, database_id, sql, txn_ctx).await);
    }

    // Stream consumption: `SELECT * FROM STREAM <name> CONSUMER GROUP <group>
    // [PARTITION <p>] [LIMIT <n>]`. Parses into no typed AST variant — the
    // router recognizes it by its leading words and the clause after the name,
    // before
    // the parse gate.
    if is_consumer_group_read(sql, "STREAM") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(stream_select::select_from_stream(state, identity, database_id, &parts).await);
    }

    // Stream/Topic consumption: `SELECT * FROM TOPIC <name> CONSUMER GROUP
    // <group> [LIMIT <n>]`. Topics use "topic:<name>" buffer keys; the router
    // rewrites the token slice (TOPIC → STREAM, name → "topic:<name>") and
    // delegates to the stream-consume handler, before the parse gate.
    if is_consumer_group_read(sql, "TOPIC") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 8
            || !parts[3].eq_ignore_ascii_case("TOPIC")
            || !parts[5].eq_ignore_ascii_case("CONSUMER")
            || !parts[6].eq_ignore_ascii_case("GROUP")
        {
            return Some(Err(DdlError::new(
                "42601",
                "expected SELECT * FROM TOPIC <topic> CONSUMER GROUP <group>",
            )));
        }
        // The token stays raw. `select_from_stream` decodes the name once,
        // after the `topic:` prefix.
        let prefixed_name = format!("topic:{}", parts[4]);
        let stream_keyword = "STREAM";
        let mut rewritten = Vec::with_capacity(parts.len());
        for (i, &p) in parts.iter().enumerate() {
            match i {
                3 => rewritten.push(stream_keyword),
                4 => rewritten.push(prefixed_name.as_str()),
                _ => rewritten.push(p),
            }
        }
        return Some(
            stream_select::select_from_stream(state, identity, database_id, &rewritten).await,
        );
    }

    // Durable topics: `SUBSCRIBE TO <topic> [GROUP <group>] [SINCE <seq>]`.
    // Parses into no typed AST variant, so route it before the parse gate.
    if upper.starts_with("SUBSCRIBE TO ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(topic_subscribe::subscribe_to(
            state,
            identity,
            database_id,
            sql,
            &parts,
        ));
    }

    None
}

/// `SELECT * FROM <kind> <name> CONSUMER GROUP <group> ...`, where `kind` is
/// `STREAM` or `TOPIC`. The consumer-group clause follows the name.
fn is_consumer_group_read(sql: &str, kind: &str) -> bool {
    if !mentions(sql, "CONSUMER") {
        return false;
    }
    let tokens = RouteTokens::new(sql);
    tokens.leads_with(&["SELECT", "*", "FROM", kind])
        && tokens.phrase_at(tokens.skip_name(4), &["CONSUMER", "GROUP"])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_group_reads_are_routed_by_kind() {
        let stream = "SELECT * FROM STREAM orders_cdc CONSUMER GROUP analytics LIMIT 10";
        assert!(is_consumer_group_read(stream, "STREAM"));
        assert!(!is_consumer_group_read(stream, "TOPIC"));
        assert!(is_consumer_group_read(
            "select * from topic alerts consumer group ops",
            "TOPIC"
        ));
    }

    /// A plain SELECT whose literal or column names the clause is no stream
    /// read.
    #[test]
    fn a_plain_select_is_not_a_consumer_group_read() {
        assert!(!is_consumer_group_read(
            "SELECT * FROM events WHERE note = 'FROM STREAM s CONSUMER GROUP g'",
            "STREAM"
        ));
        assert!(!is_consumer_group_read(
            "SELECT * FROM stream_log WHERE consumer = 'x'",
            "STREAM"
        ));
    }
}
