// SPDX-License-Identifier: BUSL-1.1

//! The database an HTTP request reached admission with.
//!
//! The HTTP failure-count layer wraps each request in [`scope`]. Admission
//! calls [`note`] with the request's database. Outside a scope, `note` does
//! nothing, so non-HTTP transports are unaffected.

use std::cell::Cell;
use std::future::Future;

use nodedb_types::DatabaseId;

tokio::task_local! {
    static ADMITTED: Cell<Option<DatabaseId>>;
}

/// Record the database the current request is admitted against.
pub(crate) fn note(database_id: DatabaseId) {
    let _ = ADMITTED.try_with(|slot| slot.set(Some(database_id)));
}

/// Run `request` and return its output with the database noted during it.
pub(crate) async fn scope<F: Future>(request: F) -> (F::Output, Option<DatabaseId>) {
    ADMITTED
        .scope(Cell::new(None), async {
            let output = request.await;
            (output, ADMITTED.with(Cell::get))
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scope_returns_the_noted_database() {
        let (_, noted) = scope(async { note(DatabaseId::DEFAULT) }).await;
        assert_eq!(noted, Some(DatabaseId::DEFAULT));
    }

    #[tokio::test]
    async fn scope_without_admission_returns_none() {
        let (_, noted) = scope(async {}).await;
        assert_eq!(noted, None);
    }

    #[test]
    fn note_outside_a_scope_is_a_no_op() {
        note(DatabaseId::DEFAULT);
    }
}
