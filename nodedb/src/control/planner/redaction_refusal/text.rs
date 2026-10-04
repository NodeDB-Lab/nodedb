// SPDX-License-Identifier: BUSL-1.1

//! Refusal of full-text reads over a redacted column.
//!
//! A text match and a BM25 score are computed in the Data Plane over the
//! stored text, which is never redacted. `WHERE text_match(ssn, '123')`
//! selects rows by the stored `ssn`, and `bm25_score(ssn, '123')` returns a
//! number derived from it, so masking the result rows protects nothing. The
//! whole-document index (`text_match(*, q)`) reads every text field, so any
//! rule on the collection covers it.

use nodedb_physical::physical_plan::{TextOp, TextScoreSpec};
use nodedb_types::QualifiedCollection;

use super::lookup::RefusalCtx;

/// Refuse a text op that reads a redacted column. Exhaustive over [`TextOp`]
/// so a new text operation forces a decision here.
pub(super) fn refuse_text_op(op: &TextOp, ctx: &RefusalCtx<'_>) -> crate::Result<()> {
    match op {
        TextOp::Search {
            collection,
            field,
            scores,
            ..
        }
        | TextOp::PhraseSearch {
            collection,
            field,
            scores,
            ..
        } => {
            refuse_text_field(ctx, collection, field.as_deref(), "text_match")?;
            refuse_scores(ctx, collection, scores)
        }
        TextOp::BM25ScoreScan {
            collection, scores, ..
        } => refuse_scores(ctx, collection, scores),
        TextOp::HybridSearch {
            collection,
            text_field,
            ..
        }
        | TextOp::HybridSearchTriple {
            collection,
            text_field,
            ..
        } => refuse_text_field(ctx, collection, text_field.as_deref(), "the text leg"),
        // Index writes and analyzer configuration return no column values.
        TextOp::FtsIndexDoc { .. } | TextOp::FtsDeleteDoc { .. } | TextOp::SetTextConfig { .. } => {
            Ok(())
        }
    }
}

fn refuse_scores(
    ctx: &RefusalCtx<'_>,
    collection: &QualifiedCollection,
    scores: &[TextScoreSpec],
) -> crate::Result<()> {
    scores.iter().try_for_each(|spec| {
        refuse_text_field(ctx, collection, spec.field.as_deref(), "bm25_score")
    })
}

/// Refuse when `field` is redacted, or when `field` is `None` (the
/// whole-document index) and any rule covers the collection.
fn refuse_text_field(
    ctx: &RefusalCtx<'_>,
    collection: &QualifiedCollection,
    field: Option<&str>,
    reader: &str,
) -> crate::Result<()> {
    let collection = collection.as_str();
    if collection.is_empty() {
        return Ok(());
    }
    match field {
        Some(field) if ctx.field_is_redacted(collection, field) => Err(crate::Error::PlanError {
            detail: format!(
                "column '{field}' on '{collection}' is redacted for this role: {reader} over a \
                 redacted column is not permitted — the match and score are computed over the \
                 stored text, so masking the result would not protect it"
            ),
        }),
        Some(_) => Ok(()),
        None if ctx.collection_is_redacted(collection) => Err(crate::Error::PlanError {
            detail: format!(
                "'{collection}' has redacted columns for this role: {reader} over the whole \
                 document is not permitted — it reads every text column, including the \
                 redacted ones"
            ),
        }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{TextOp, TextScoreSpec};
    use nodedb_types::{DatabaseId, QualifiedCollection};

    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::security::auth_context::AuthContext;
    use crate::control::security::redaction::{
        RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
    };
    use crate::types::TenantId;

    const TENANT: u64 = 1;

    fn store_with_rule(collection: &str, role: &str, field: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: TENANT,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        store
    }

    fn auth_with_role(role: &str) -> AuthContext {
        use crate::control::security::identity::{
            AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
        };
        let identity = AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(TENANT),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        );
        let mut auth = AuthContext::from_identity(&identity, "s_test".into());
        auth.roles = vec![role.to_string()];
        auth
    }

    fn check(plan: &PhysicalPlan, store: &RedactionStore) -> crate::Result<()> {
        super::super::refuse_unredactable_plan(
            plan,
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            &auth_with_role("support"),
            store,
        )
    }

    fn users() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "users")
    }

    fn search(field: Option<&str>, scores: Vec<TextScoreSpec>) -> PhysicalPlan {
        PhysicalPlan::Text(TextOp::Search {
            collection: users(),
            field: field.map(str::to_string),
            query: "123".into(),
            top_k: 10,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: false,
            prefilter: None,
            filters: Vec::new(),
            rls_filters: Vec::new(),
            scores,
        })
    }

    fn score(field: Option<&str>) -> TextScoreSpec {
        TextScoreSpec {
            field: field.map(str::to_string),
            query: "123".into(),
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: false,
            alias: "s".into(),
        }
    }

    fn assert_refused(result: crate::Result<()>) {
        assert!(
            matches!(result, Err(crate::Error::PlanError { .. })),
            "expected a PlanError refusal, got {result:?}"
        );
    }

    /// `WHERE text_match(ssn, …)` selects rows by the stored `ssn`.
    #[test]
    fn text_match_on_a_redacted_column_is_refused() {
        let store = store_with_rule("users", "support", "ssn");
        assert_refused(check(&search(Some("ssn"), Vec::new()), &store));
        assert!(check(&search(Some("bio"), Vec::new()), &store).is_ok());
    }

    /// `bm25_score(ssn, …)` derives a number from the stored `ssn`, in a
    /// search and in a score scan.
    #[test]
    fn bm25_score_on_a_redacted_column_is_refused() {
        let store = store_with_rule("users", "support", "ssn");
        assert_refused(check(
            &search(Some("bio"), vec![score(Some("ssn"))]),
            &store,
        ));
        let scan = PhysicalPlan::Text(TextOp::BM25ScoreScan {
            collection: users(),
            filters: Vec::new(),
            rls_filters: Vec::new(),
            scores: vec![score(Some("ssn"))],
            bound: None,
        });
        assert_refused(check(&scan, &store));
    }

    /// The whole-document index reads every text column.
    #[test]
    fn whole_document_match_is_refused_under_any_rule() {
        let store = store_with_rule("users", "support", "ssn");
        assert_refused(check(&search(None, Vec::new()), &store));
        let phrase = PhysicalPlan::Text(TextOp::PhraseSearch {
            collection: users(),
            field: None,
            terms: vec!["a".into(), "b".into()],
            top_k: 10,
            prefilter: None,
            filters: Vec::new(),
            rls_filters: Vec::new(),
            scores: Vec::new(),
        });
        assert_refused(check(&phrase, &store));
    }

    /// A role the policy does not name reads the column.
    #[test]
    fn a_role_without_the_rule_is_not_refused() {
        let store = store_with_rule("users", "analyst", "ssn");
        assert!(check(&search(Some("ssn"), vec![score(None)]), &store).is_ok());
    }
}
