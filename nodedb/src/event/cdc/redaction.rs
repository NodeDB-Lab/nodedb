// SPDX-License-Identifier: BUSL-1.1

//! Column redaction for the CDC events a change-stream subscriber receives.
//!
//! A [`CdcEvent`] carries the written row itself: `new_value`, the UPDATE
//! pre-image `old_value`, and the per-field diffs computed from the two. None
//! of those reach the named-projection shaping core where a query's rows are
//! redacted, so a subscriber would otherwise read stored columns that every
//! other delivery surface masks. The rules are applied here instead, through
//! the same shared hook, so the mask / hash / null semantics stay defined
//! exactly once.
//!
//! The router decodes the stored row bytes ONCE per `WriteEvent`
//! (`CdcRouter::route_event`), so by the time an event is deliverable these
//! payloads are already decoded `serde_json::Value` maps of stored fields — the
//! flat row map the SELECT path redacts, not the stored MessagePack bytes the
//! device-sync surfaces hand over.
//!
//! The scope a policy is keyed on belongs to the subscriber, never to the
//! event:
//!
//! - Control-Plane readers — the stream SELECT and the HTTP poll / SSE
//!   endpoints — are the authenticated caller, whose roles are resolved per
//!   request.
//! - Event-Plane deliveries — the webhook POST and Kafka publish tasks — have
//!   no request to resolve anything from. Their scope is the one an
//!   authenticated principal's `CREATE CHANGE STREAM` captured onto the
//!   subscription record itself
//!   (`ChangeStreamDef::subscriber_roles`), which the delivery task reads back
//!   from the stream registry. No identity is pulled across the Data→Event bus.
//!
//! A captured scope that carries no roles proves nothing about the destination:
//! the record may simply predate the field. Events of a collection some policy
//! protects are then withheld instead of delivered in the clear, per collection,
//! so a stream over unprotected collections still survives the upgrade.
//!
//! Rules are keyed per collection and a wildcard stream carries events from
//! many, so the resolved inputs are rebuilt only when consecutive events cross
//! a collection boundary.

use std::sync::Arc;

use crate::control::security::redaction::RedactionStore;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::state::SharedState;
use crate::event::cdc::event::CdcEvent;
use crate::event::cdc::redaction_warn;
use crate::event::field_diff::FieldDiff;
use crate::types::{DatabaseId, TenantId};

/// Where a subscriber's roles came from, which decides what an empty role list
/// means.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ScopeOrigin {
    /// Resolved per request from the authenticated caller. No roles then means
    /// the caller genuinely holds none — the same identity the SELECT path
    /// serves unmasked, and the same one the planner's refusal pass lets
    /// through.
    LiveIdentity,
    /// Captured onto the subscription record at `CREATE CHANGE STREAM`. No
    /// roles then may equally mean the record predates the field, so nothing can
    /// be concluded about what the destination is entitled to read.
    CapturedSubscription,
}

/// The subscriber-scoped redaction inputs for one delivery surface.
pub struct CdcSubscriberScope {
    tenant_id: TenantId,
    roles: Vec<String>,
    origin: ScopeOrigin,
    /// The last collection resolved, with its inputs. The collection name is
    /// empty until the first event resolves one, and no collection is named
    /// with the empty string, so the first event always rebuilds.
    resolved: (String, QueryRedaction),
}

impl CdcSubscriberScope {
    /// Build the inputs for an authenticated caller holding `roles` in
    /// `tenant_id`.
    pub fn new(tenant_id: TenantId, roles: Vec<String>) -> Self {
        Self::with_origin(tenant_id, roles, ScopeOrigin::LiveIdentity)
    }

    fn with_origin(tenant_id: TenantId, roles: Vec<String>, origin: ScopeOrigin) -> Self {
        let redaction = QueryRedaction::new(tenant_id, roles.clone(), Vec::new());
        Self {
            tenant_id,
            roles,
            origin,
            resolved: (String::new(), redaction),
        }
    }

    /// The scope a change stream's Event-Plane deliveries run under.
    ///
    /// Resolved from the subscription record itself — the roles the Control
    /// Plane captured onto it when an authenticated principal created the
    /// stream. No identity is resolved here, and nothing is read back across
    /// the Data→Event bus.
    ///
    /// `None` when the stream has no registered definition: there is then no
    /// subscriber to evaluate a policy for, and the caller must deliver
    /// nothing.
    pub fn for_stream(
        state: &SharedState,
        database_id: DatabaseId,
        tenant_id: u64,
        stream_name: &str,
    ) -> Option<Self> {
        let def = state
            .stream_registry
            .get(database_id, tenant_id, stream_name)?;
        Some(Self::with_origin(
            TenantId::new(tenant_id),
            def.subscriber_roles,
            ScopeOrigin::CapturedSubscription,
        ))
    }

    /// The event this subscriber may receive, or `None` when it must be
    /// dropped.
    ///
    /// The returned handle is the SAME event when no rule covers its
    /// collection — the buffered payload is shared by every consumer of the
    /// stream, so a delivery under no policy neither clones nor rewrites it and
    /// the frame stays byte-identical. When a rule does apply, the rewrite
    /// happens on a private copy, leaving the buffered original intact for the
    /// next subscriber, whose rules may differ.
    ///
    /// `None` means the payload would have reached the subscriber in the clear
    /// under a policy that covers it — either because a rule covers the
    /// collection and the payload could not be reached, or because the
    /// subscription carries nothing to evaluate that policy against.
    pub fn apply(
        &mut self,
        store: &RedactionStore,
        event: &Arc<CdcEvent>,
    ) -> Option<Arc<CdcEvent>> {
        if self.entitlement_is_unprovable(store, &event.collection) {
            warn_unprovable_entitlement(event);
            return None;
        }

        let redaction = self.resolved_for(&event.collection);
        if !redaction.has_any_rule(store) {
            return Some(Arc::clone(event));
        }

        let mut redacted = CdcEvent::clone(event);
        if !redact_row_value(redaction, store, &mut redacted.new_value)
            || !redact_row_value(redaction, store, &mut redacted.old_value)
        {
            warn_dropped(event);
            return None;
        }
        redact_field_diffs(redaction, store, redacted.field_diffs.as_mut());
        Some(Arc::new(redacted))
    }

    /// Rewrite a consumed batch in place, keeping only what may be delivered.
    ///
    /// Events [`CdcSubscriberScope::apply`] refuses are dropped from the batch
    /// rather than delivered in the clear. The batch's per-partition offsets
    /// were computed from the events as consumed, so a dropped event still
    /// advances the cursor past itself and is not redelivered forever.
    pub fn retain_deliverable(&mut self, store: &RedactionStore, events: &mut Vec<Arc<CdcEvent>>) {
        let mut deliverable = Vec::with_capacity(events.len());
        for event in events.iter() {
            if let Some(event) = self.apply(store, event) {
                deliverable.push(event);
            }
        }
        *events = deliverable;
    }

    /// Whether this event's collection is protected but the subscriber cannot
    /// be shown to be entitled to any part of it.
    ///
    /// A subscription written before the roles were captured decodes with an
    /// empty list, which is indistinguishable from a creator that held none. No
    /// policy is keyed on the empty list, so every rule on the collection would
    /// silently fail to match and the stored columns would go out in the clear —
    /// to a webhook endpoint or a Kafka topic that no later request can take
    /// them back from.
    ///
    /// The question is asked per collection so the refusal stays where it is
    /// needed: an unprotected collection has nothing to prove entitlement to,
    /// and its stream keeps delivering across the upgrade that introduced the
    /// field.
    fn entitlement_is_unprovable(&self, store: &RedactionStore, collection: &str) -> bool {
        self.origin == ScopeOrigin::CapturedSubscription
            && self.roles.is_empty()
            && store.has_any_rule_for_collection_any_role(self.tenant_id.as_u64(), collection)
    }

    /// The redaction inputs for `collection`, rebuilt only when it changed.
    fn resolved_for(&mut self, collection: &str) -> &QueryRedaction {
        if self.resolved.0 != collection {
            self.resolved = (
                collection.to_string(),
                QueryRedaction::new(
                    self.tenant_id,
                    self.roles.clone(),
                    vec![(String::new(), collection.to_string())],
                ),
            );
        }
        &self.resolved.1
    }
}

/// Log a payload a rule covers but that cannot be rewritten.
///
/// Rate limited per `(tenant, collection)`: the shape of a payload is a
/// property of how the rows are written, so an event that hits this raises it
/// again for every sibling row.
fn warn_dropped(event: &CdcEvent) {
    if !redaction_warn::should_warn(
        event.tenant_id,
        &event.collection,
        redaction_warn::WithheldReason::UnreadablePayload,
    ) {
        return;
    }
    tracing::warn!(
        collection = %event.collection,
        row_id = %event.row_id,
        offset = %event.offset_token(),
        "CDC events dropped: a redaction rule covers this collection but the \
         event payload is not a stored row map"
    );
}

/// Log a subscription that cannot be shown to be entitled to a protected
/// collection, naming the operator action that clears it.
///
/// Rate limited per `(tenant, collection)`: the condition belongs to the
/// subscription record, so it holds for every event the stream carries until an
/// operator acts on it.
fn warn_unprovable_entitlement(event: &CdcEvent) {
    if !redaction_warn::should_warn(
        event.tenant_id,
        &event.collection,
        redaction_warn::WithheldReason::UnprovableEntitlement,
    ) {
        return;
    }
    tracing::warn!(
        collection = %event.collection,
        row_id = %event.row_id,
        offset = %event.offset_token(),
        "CDC events dropped: a redaction policy protects this collection but the \
         change stream captured no subscriber roles, so the destination cannot be \
         shown to be entitled to the stored values — recreate the change stream as \
         the principal that should receive it, or backfill its subscriber roles, to \
         resume delivery"
    );
}

/// Redact one row payload in place, reporting whether it may be delivered.
///
/// A payload a rule covers must be a map of stored fields for the rules, which
/// name columns, to reach anything. Anything else cannot be redacted at all, so
/// it is refused rather than delivered.
///
/// The matching itself goes through [`RedactionStore::apply_flat_row`], the one
/// definition every delivery surface shares. It is reached directly rather than
/// through the `redact_envelope_row` hook because this payload is a stored row
/// map, never a scan envelope: a two-column row whose columns happened to be
/// named `id` and `data` would be mistaken for one, and its own `id` / `data`
/// rules silently skipped.
fn redact_row_value(
    redaction: &QueryRedaction,
    store: &RedactionStore,
    value: &mut Option<serde_json::Value>,
) -> bool {
    match value {
        // A payload the operation does not carry — an INSERT has no pre-image
        // — holds nothing to redact.
        None => true,
        Some(serde_json::Value::Object(fields)) => {
            let ctx = redaction.ctx(store);
            ctx.store
                .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, fields);
            true
        }
        Some(_) => false,
    }
}

/// Redact the per-field diffs of an UPDATE in place.
///
/// A diff restates a changed column's before and after values beside its
/// dot-path, so a diff on a ruled column carries exactly what the rule removed
/// from `old_value` and `new_value`. Each side is rewritten under the column
/// the path is rooted at, which is the name a rule can be written against; the
/// diff list itself — its length, paths and ops — is left intact so the wire
/// shape does not change.
fn redact_field_diffs(
    redaction: &QueryRedaction,
    store: &RedactionStore,
    diffs: Option<&mut Vec<FieldDiff>>,
) {
    let Some(diffs) = diffs else {
        return;
    };
    for diff in diffs {
        let root = diff_root_field(&diff.field).to_string();
        if !redaction.field_has_rule(store, &root) {
            continue;
        }
        redact_diff_side(redaction, store, &root, &mut diff.old_value);
        redact_diff_side(redaction, store, &root, &mut diff.new_value);
    }
}

/// The stored column a diff path is rooted at: `blocks[3].content` → `blocks`.
fn diff_root_field(path: &str) -> &str {
    let end = path.find(['.', '[']).unwrap_or(path.len());
    &path[..end]
}

/// Rewrite one side of a diff as if it were that column's value in a row.
fn redact_diff_side(
    redaction: &QueryRedaction,
    store: &RedactionStore,
    root: &str,
    side: &mut Option<serde_json::Value>,
) {
    let Some(value) = side.take() else {
        return;
    };
    let mut row = serde_json::Map::with_capacity(1);
    row.insert(root.to_string(), value);
    let ctx = redaction.ctx(store);
    ctx.store
        .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, &mut row);
    // A rule rewrites the column's value and never removes the key, so the
    // side is restored; a missing key would mean nothing is left to deliver.
    *side = row.get_mut(root).map(serde_json::Value::take);
}

#[cfg(test)]
mod tests {
    use crate::control::security::redaction::{
        RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
    };
    use crate::event::field_diff::DiffOp;

    use super::*;

    fn store_with(
        collection: &str,
        role: &str,
        field: &str,
        mode: RedactionMode,
    ) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: 1,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode,
            }],
        });
        store
    }

    fn masked(collection: &str, role: &str, field: &str) -> RedactionStore {
        store_with(collection, role, field, RedactionMode::Mask("***".into()))
    }

    fn scope(role: &str) -> CdcSubscriberScope {
        CdcSubscriberScope::new(TenantId::new(1), vec![role.to_string()])
    }

    fn event(
        collection: &str,
        op: &str,
        new_value: Option<serde_json::Value>,
        old_value: Option<serde_json::Value>,
    ) -> Arc<CdcEvent> {
        Arc::new(CdcEvent {
            sequence: 1,
            partition: 0,
            collection: collection.into(),
            op: op.into(),
            row_id: "row-1".into(),
            event_time: 0,
            lsn: 10,
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            new_value,
            old_value,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
            source: crate::event::EventSource::User,
        })
    }

    fn row(email: &str) -> serde_json::Value {
        serde_json::json!({"email": email, "name": "Alice"})
    }

    /// The leak this closes: a CDC subscriber read every stored column of the
    /// written row, whatever the SELECT path would have masked.
    #[test]
    fn delivered_new_value_carries_the_mask_for_a_ruled_role() {
        let store = masked("users", "support", "email");
        let mut scope = scope("support");
        let event = event("users", "INSERT", Some(row("a@b.c")), None);

        let delivered = scope.apply(&store, &event).expect("event is deliverable");

        let new_value = delivered.new_value.as_ref().expect("new value");
        assert_eq!(new_value["email"], "***");
        assert_eq!(new_value["name"], "Alice");
    }

    /// An UPDATE pre-image is the same row one version back — masking only the
    /// post-image would hand the subscriber the cleartext it just removed.
    #[test]
    fn update_old_value_is_redacted_like_the_new_value() {
        let store = masked("users", "support", "email");
        let mut scope = scope("support");
        let event = event(
            "users",
            "UPDATE",
            Some(row("new@b.c")),
            Some(row("old@b.c")),
        );

        let delivered = scope.apply(&store, &event).expect("event is deliverable");

        assert_eq!(delivered.new_value.as_ref().expect("new")["email"], "***");
        assert_eq!(delivered.old_value.as_ref().expect("old")["email"], "***");
    }

    /// A diff restates the changed column's values beside its path, so it
    /// leaks exactly what the rule removed from the two payloads.
    #[test]
    fn field_diffs_of_a_ruled_column_are_redacted_on_both_sides() {
        let store = masked("users", "support", "email");
        let mut scope = scope("support");
        let mut raw = CdcEvent::clone(&event(
            "users",
            "UPDATE",
            Some(row("new@b.c")),
            Some(row("old@b.c")),
        ));
        raw.field_diffs = Some(vec![
            FieldDiff {
                field: "email".into(),
                op: DiffOp::Modified,
                old_value: Some(serde_json::json!("old@b.c")),
                new_value: Some(serde_json::json!("new@b.c")),
            },
            FieldDiff {
                field: "name".into(),
                op: DiffOp::Modified,
                old_value: Some(serde_json::json!("Alice")),
                new_value: Some(serde_json::json!("Alicia")),
            },
        ]);

        let delivered = scope
            .apply(&store, &Arc::new(raw))
            .expect("event is deliverable");

        let diffs = delivered.field_diffs.as_ref().expect("diffs");
        assert_eq!(diffs.len(), 2, "the diff list shape must not change");
        assert_eq!(diffs[0].old_value, Some(serde_json::json!("***")));
        assert_eq!(diffs[0].new_value, Some(serde_json::json!("***")));
        assert_eq!(diffs[1].old_value, Some(serde_json::json!("Alice")));
        assert_eq!(diffs[1].new_value, Some(serde_json::json!("Alicia")));
    }

    /// A nested path is rooted at the column a rule can name, so a rule on the
    /// container also covers the diffs reaching inside it.
    #[test]
    fn nested_diff_path_is_matched_by_its_root_column() {
        let store = masked("docs", "support", "blocks");
        let mut scope = scope("support");
        let mut raw = CdcEvent::clone(&event("docs", "UPDATE", None, None));
        raw.field_diffs = Some(vec![FieldDiff {
            field: "blocks[3].content".into(),
            op: DiffOp::Modified,
            old_value: Some(serde_json::json!("secret")),
            new_value: Some(serde_json::json!("also secret")),
        }]);

        let delivered = scope
            .apply(&store, &Arc::new(raw))
            .expect("event is deliverable");

        // The path is preserved so the subscriber still sees WHICH field
        // changed, but both sides of the value are masked — a diff restates
        // the column's before and after, so leaving either in the clear would
        // hand back exactly what the rule removes.
        let diffs = delivered.field_diffs.as_ref().expect("diffs");
        assert_eq!(diffs[0].field, "blocks[3].content");
        assert_eq!(diffs[0].old_value, Some(serde_json::json!("***")));
        assert_eq!(diffs[0].new_value, Some(serde_json::json!("***")));
    }

    /// A subscriber whose roles no policy names reads the stored values, and
    /// reads the very same buffered event rather than a rewritten copy.
    #[test]
    fn unruled_role_receives_the_stored_values_unchanged() {
        let store = masked("users", "support", "email");
        let mut scope = scope("analyst");
        let event = event("users", "INSERT", Some(row("a@b.c")), None);

        let delivered = scope.apply(&store, &event).expect("event is deliverable");

        assert_eq!(delivered.new_value.as_ref().expect("new")["email"], "a@b.c");
        assert!(Arc::ptr_eq(&event, &delivered));
    }

    /// No policy at all must leave the frame exactly as buffered — the shared
    /// event is handed on without a decode or re-encode.
    #[test]
    fn no_policy_delivers_the_identical_event() {
        let store = RedactionStore::new();
        let mut scope = scope("support");
        let event = event("users", "INSERT", Some(row("a@b.c")), None);

        let delivered = scope.apply(&store, &event).expect("event is deliverable");

        assert!(Arc::ptr_eq(&event, &delivered));
        assert_eq!(delivered.to_json_bytes(), event.to_json_bytes());
    }

    /// A rule on another collection must not reach this event, and the cached
    /// inputs must be rebuilt when a wildcard stream crosses collections.
    #[test]
    fn cached_inputs_are_rebuilt_when_the_collection_changes() {
        let store = masked("users", "support", "email");
        let mut scope = scope("support");

        let ruled = scope
            .apply(&store, &event("users", "INSERT", Some(row("a@b.c")), None))
            .expect("deliverable");
        assert_eq!(ruled.new_value.as_ref().expect("new")["email"], "***");

        let unruled = scope
            .apply(&store, &event("audit", "INSERT", Some(row("d@e.f")), None))
            .expect("deliverable");
        assert_eq!(unruled.new_value.as_ref().expect("new")["email"], "d@e.f");

        let ruled_again = scope
            .apply(&store, &event("users", "INSERT", Some(row("g@h.i")), None))
            .expect("deliverable");
        assert_eq!(ruled_again.new_value.as_ref().expect("new")["email"], "***");
    }

    /// A payload a rule covers but that is not a stored row map cannot be
    /// redacted, so the event is refused rather than delivered in the clear.
    #[test]
    fn unreadable_payload_under_an_applicable_rule_is_dropped() {
        let store = masked("users", "support", "email");
        let mut scope = scope("support");

        assert!(
            scope
                .apply(
                    &store,
                    &event("users", "INSERT", Some(serde_json::json!("opaque")), None)
                )
                .is_none()
        );
        assert!(
            scope
                .apply(
                    &store,
                    &event("users", "DELETE", None, Some(serde_json::json!(7)))
                )
                .is_none(),
            "an unreadable pre-image is refused exactly like a post-image"
        );
    }

    /// The same payload under no applicable rule is delivered untouched — the
    /// refusal is scoped to what a policy actually covers.
    #[test]
    fn unreadable_payload_without_an_applicable_rule_is_delivered() {
        let store = masked("users", "support", "email");
        let mut scope = scope("analyst");

        assert!(
            scope
                .apply(
                    &store,
                    &event("users", "INSERT", Some(serde_json::json!("opaque")), None)
                )
                .is_some()
        );
    }

    /// A `Null` rule keeps the column present and valued null, so the frame a
    /// subscriber parses keeps its shape.
    #[test]
    fn null_rule_keeps_the_column_present() {
        let store = store_with("users", "support", "email", RedactionMode::Null);
        let mut scope = scope("support");

        let delivered = scope
            .apply(&store, &event("users", "INSERT", Some(row("a@b.c")), None))
            .expect("deliverable");

        let new_value = delivered.new_value.as_ref().expect("new value");
        assert_eq!(new_value["email"], serde_json::Value::Null);
        assert_eq!(new_value["name"], "Alice");
    }

    /// A batch keeps its deliverable events in order and loses only the ones
    /// that could not be redacted.
    #[test]
    fn retain_deliverable_keeps_order_and_drops_only_the_refused() {
        let store = masked("users", "support", "email");
        let mut scope = scope("support");
        let mut events = vec![
            event("users", "INSERT", Some(row("a@b.c")), None),
            event("users", "INSERT", Some(serde_json::json!("opaque")), None),
            event("audit", "INSERT", Some(row("d@e.f")), None),
        ];

        scope.retain_deliverable(&store, &mut events);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].new_value.as_ref().expect("new")["email"], "***");
        assert_eq!(events[1].collection, "audit");
    }

    /// An authenticated caller holding no roles matches no policy keyed on one,
    /// exactly as on the SELECT path, so the batch is delivered as buffered.
    #[test]
    fn subscriber_without_roles_delivers_every_event_untouched() {
        let store = masked("users", "support", "email");
        let mut scope = CdcSubscriberScope::new(TenantId::new(1), Vec::new());
        let original = event("users", "INSERT", Some(row("a@b.c")), None);
        let mut events = vec![Arc::clone(&original)];

        scope.retain_deliverable(&store, &mut events);

        assert_eq!(events.len(), 1);
        assert!(Arc::ptr_eq(&original, &events[0]));
    }

    fn legacy_subscription() -> CdcSubscriberScope {
        CdcSubscriberScope::with_origin(
            TenantId::new(1),
            Vec::new(),
            ScopeOrigin::CapturedSubscription,
        )
    }

    /// The leak this closes: a subscription persisted before the roles were
    /// captured decodes roleless, so every rule on the collection failed to
    /// match and the stored columns went to the webhook or Kafka destination in
    /// the clear. Nothing can prove that destination entitled, so the events are
    /// withheld — for ANY role's policy, since none of them can be evaluated.
    #[test]
    fn roleless_subscription_drops_events_of_a_protected_collection() {
        let store = masked("users", "support", "email");
        let mut scope = legacy_subscription();

        assert!(
            scope
                .apply(&store, &event("users", "INSERT", Some(row("a@b.c")), None))
                .is_none()
        );
    }

    /// …and the refusal is per collection, not blanket: a stream over a
    /// collection no policy covers keeps delivering across the upgrade that
    /// introduced the captured roles.
    #[test]
    fn roleless_subscription_still_delivers_an_unprotected_collection() {
        let store = masked("users", "support", "email");
        let mut scope = legacy_subscription();
        let original = event("audit", "INSERT", Some(row("d@e.f")), None);

        let delivered = scope
            .apply(&store, &original)
            .expect("an unprotected collection has no entitlement to prove");

        assert!(Arc::ptr_eq(&original, &delivered));
        assert_eq!(delivered.new_value.as_ref().expect("new")["email"], "d@e.f");
    }

    /// A wildcard subscription mixes both, and each event is decided on its own
    /// collection.
    #[test]
    fn roleless_wildcard_subscription_withholds_only_the_protected_collection() {
        let store = masked("users", "support", "email");
        let mut scope = legacy_subscription();
        let mut events = vec![
            event("users", "INSERT", Some(row("a@b.c")), None),
            event("audit", "INSERT", Some(row("d@e.f")), None),
        ];

        scope.retain_deliverable(&store, &mut events);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].collection, "audit");
    }

    /// A subscription that DID capture roles is evaluated normally — the
    /// refusal keys on the empty list, never on the delivery surface.
    #[test]
    fn subscription_with_captured_roles_is_redacted_not_dropped() {
        let store = masked("users", "support", "email");
        let mut scope = CdcSubscriberScope::with_origin(
            TenantId::new(1),
            vec!["support".to_string()],
            ScopeOrigin::CapturedSubscription,
        );

        let delivered = scope
            .apply(&store, &event("users", "INSERT", Some(row("a@b.c")), None))
            .expect("a captured role resolves a policy, so the event is redacted");

        assert_eq!(delivered.new_value.as_ref().expect("new")["email"], "***");
    }

    /// The Event-Plane delivery tasks resolve their scope from the roles the
    /// Control Plane captured onto the subscription when it was created —
    /// never from a live identity, which does not exist on that plane.
    #[test]
    fn event_plane_scope_comes_from_the_captured_subscription_roles() {
        use crate::event::cdc::stream_def::{
            ChangeStreamDef, CompactionConfig, LateDataPolicy, OpFilter, RetentionConfig,
            StreamFormat,
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        let database_id = DatabaseId::new(7);
        state.redaction.create_policy(RedactionPolicy {
            name: "users_support_email".into(),
            tenant_id: 1,
            collection: "users".into(),
            display_collection: "users".into(),
            for_role: "support".into(),
            rules: vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        state.stream_registry.register(ChangeStreamDef {
            database_id,
            tenant_id: 1,
            name: "orders_stream".into(),
            collection: "*".into(),
            op_filter: OpFilter::all(),
            format: StreamFormat::Json,
            retention: RetentionConfig::default(),
            compaction: CompactionConfig::default(),
            webhook: crate::event::webhook::WebhookConfig::default(),
            late_data: LateDataPolicy::default(),
            kafka: crate::event::kafka::KafkaDeliveryConfig::default(),
            owner: "admin".into(),
            created_at: 0,
            subscriber_roles: vec!["support".into()],
        });

        let mut subscriber =
            CdcSubscriberScope::for_stream(&state, database_id, 1, "orders_stream")
                .expect("a registered stream carries a subscriber scope");
        let delivered = subscriber
            .apply(
                &state.redaction,
                &event("users", "INSERT", Some(row("a@b.c")), None),
            )
            .expect("deliverable");
        assert_eq!(delivered.new_value.as_ref().expect("new")["email"], "***");

        assert!(
            CdcSubscriberScope::for_stream(&state, database_id, 1, "no_such_stream").is_none(),
            "a stream with no subscription record has no scope, so nothing may be delivered"
        );
    }

    #[test]
    fn diff_root_field_reports_the_column_a_rule_can_name() {
        assert_eq!(diff_root_field("email"), "email");
        assert_eq!(diff_root_field("metadata.tags"), "metadata");
        assert_eq!(diff_root_field("blocks[3].content"), "blocks");
    }
}
