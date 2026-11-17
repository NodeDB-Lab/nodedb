// SPDX-License-Identifier: BUSL-1.1

//! The `RETURNING` columns the statement now running announced to its
//! client.
//!
//! A Calvin write's participant evaluates the list and answers the final
//! typed rows. It needs the columns the client was promised before any row
//! existed: their names and types. The Calvin submit stamps them into the
//! transaction's contract (see `planner::calvin::submit::returning_contract`),
//! below every protocol's signatures, so each protocol holds them for the
//! future that dispatches the statement.

use std::future::Future;

use nodedb_cluster::calvin::types::AnnouncedColumn;

use crate::control::server::response_shape::schema::OutputSchema;

tokio::task_local! {
    static ANNOUNCED: Option<Vec<AnnouncedColumn>>;
}

/// Run `future`, a statement's dispatch, with the columns `projection`
/// announces. A star or an empty list announces none.
pub async fn with_announced_returning<F: Future>(
    projection: Option<&OutputSchema>,
    future: F,
) -> F::Output {
    ANNOUNCED.scope(announced_columns(projection), future).await
}

/// The columns the running statement announced. `None` outside a statement
/// that announced any.
pub fn announced_returning() -> Option<Vec<AnnouncedColumn>> {
    ANNOUNCED.try_with(Clone::clone).ok().flatten()
}

fn announced_columns(projection: Option<&OutputSchema>) -> Option<Vec<AnnouncedColumn>> {
    let schema = projection.filter(|schema| !schema.is_star && !schema.columns.is_empty())?;
    Some(
        schema
            .columns
            .iter()
            .map(|column| AnnouncedColumn {
                name: column.display_name.clone(),
                ty: column.ty.wire_name().to_owned(),
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::response_shape::schema::OutputColumn;
    use crate::control::server::response_shape::types::DdlColType;

    fn schema(is_star: bool) -> OutputSchema {
        OutputSchema {
            columns: vec![OutputColumn {
                display_name: "n".into(),
                lookup_key: "n".into(),
                ty: DdlColType::Int8,
            }],
            is_star,
            cp_computed: Vec::new(),
            declared_key: None,
        }
    }

    #[tokio::test]
    async fn a_named_list_is_announced_inside_its_scope_only() {
        let named = schema(false);
        let inside = with_announced_returning(Some(&named), async { announced_returning() }).await;
        assert_eq!(
            inside,
            Some(vec![AnnouncedColumn {
                name: "n".into(),
                ty: "int8".into(),
            }])
        );
        assert_eq!(announced_returning(), None);
        let star = schema(true);
        assert_eq!(
            with_announced_returning(Some(&star), async { announced_returning() }).await,
            None
        );
    }
}
