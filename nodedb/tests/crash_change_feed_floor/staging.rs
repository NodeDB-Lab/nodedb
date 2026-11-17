// SPDX-License-Identifier: BUSL-1.1

//! A crash while a large chunked redo's change events stage, before its
//! entry settles, publishes none of them early, loses none, and doubles
//! none.
//!
//! The commit's events pass the change stream's spill bound, so its pages
//! move to the change journal as they stage. The test arms the gate
//! `change_stream::after_spill::<collection>` with a release file it never
//! creates. The apply parks there once the entry spilled a page, and the
//! gate's arrival marker tells the test so. The feed then holds no event of
//! the collection: an entry is visible only once it settles. The test kills
//! the server. After the restart the entry is delivered again, its events
//! stage again from the WAL at the same positions, and the feed holds each
//! one exactly once. A second restart changes nothing.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::crash_harness::diagnostics;
use crate::feed::{
    CALVIN_READY_TIMEOUT, ROWS_PER_STATEMENT, api_token, chunked_harness, chunked_statements,
    document_collection, poll_changes, spawn_statements,
};

const COLLECTION: &str = "staging_redo";

/// How long the test waits for the apply to park at the gate.
const PARK_DEADLINE: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread")]
async fn a_chunked_redo_crashed_while_it_stages_publishes_once() {
    let mut h = chunked_harness();
    h.spawn();
    h.wait_ready();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    h.exec(&document_collection(COLLECTION)).await;
    let token = api_token(&h).await;

    let release = h.data_dir().join("after-spill-release");
    let mut parked = release.clone().into_os_string();
    parked.push(".parked");
    let parked = std::path::PathBuf::from(parked);
    h.kill_9();
    h.set_env(
        "NODEDB_FAILPOINTS",
        &format!(
            "change_stream::after_spill::{COLLECTION}=wait_file({})",
            release.display()
        ),
    );
    h.reopen();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    let statements = crate::SPILLED_STATEMENTS;
    let mut writer = spawn_statements(&h, chunked_statements(COLLECTION, statements));

    let deadline = Instant::now() + PARK_DEADLINE;
    while !parked.exists() {
        if Instant::now() >= deadline {
            let writer_outcome = if writer.is_finished() {
                format!("{:?}", (&mut writer).await)
            } else {
                "the statements still run".to_owned()
            };
            panic!(
                "the apply never parked after a spilled page; writer: {writer_outcome}\n{}",
                diagnostics::log_tail_section(&h.server_log())
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let early = poll_changes(&h, &token, COLLECTION, 0).await;
    assert!(
        early.is_empty(),
        "an entry that has not settled publishes no event: {early:?}"
    );
    h.kill_9();
    writer.abort();
    h.clear_env("NODEDB_FAILPOINTS");

    let expected = statements * ROWS_PER_STATEMENT;
    for restart in ["first", "second"] {
        h.reopen();
        h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
        let changes = poll_changes(&h, &token, COLLECTION, expected).await;
        let documents: BTreeSet<&str> = changes
            .iter()
            .map(|change| change.document_id.as_str())
            .collect();
        assert_eq!(
            (changes.len(), documents.len()),
            (expected, expected),
            "after the {restart} restart, the feed lost an event or holds one twice\n{}",
            diagnostics::log_tail_section(&h.server_log())
        );
        let positions: BTreeSet<&str> = changes
            .iter()
            .map(|change| change.position.as_str())
            .collect();
        assert_eq!(positions.len(), expected, "each event at its own position");
        h.kill_9();
    }
}
