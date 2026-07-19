# Offline Draft Sync Patterns

NodeDB-Lite provides full engine capabilities embedded in phones, browsers (WASM), and desktops. CRDT sync via WebSocket keeps local data converged with Origin. This guide covers patterns for handling provisional numbering, state transitions, and offline-first workflows.

## Provisional → Official Numbering Handoff

**Problem:** Official document numbers (invoices, receipts, POs) must be sequential and gap-free per legal requirements. But offline devices can't reserve numbers from the Origin server.

**Pattern:**

1. **Offline (NodeDB-Lite):** Create documents with provisional ULID-based identifiers:

```sql
INSERT INTO invoices (invoice_number, customer_id, amount, status)
VALUES ('DRAFT-01JXQ3...', 'cust-005', 750.00, 'draft');
```

The `DRAFT-` prefix signals this is not an externally-issued number. The app enforces: print/email blocked while `invoice_number LIKE 'DRAFT-%'`.

2. **Sync to Origin:** When the device comes online, CRDT sync pushes the draft to Origin.

3. **Origin assigns official number:** A BEFORE INSERT trigger or application logic on Origin:
   - Validates the customer exists, credit limit is OK
   - Calls `nextval('invoice_seq')` to get the gap-free sequential number
   - Replaces the provisional `DRAFT-*` identifier with the official number (e.g., `INV-26-04-00024`)
   - Posts GL entries atomically

4. **Sync back to device:** The official number propagates back via CRDT sync. The rep can now issue the invoice to the customer.

**Key safeguard:** A state constraint prevents issuing drafts:

```sql
ALTER COLLECTION invoices ADD CONSTRAINT no_draft_issue
    ON COLUMN status TRANSITIONS (
        'draft' -> 'issued'
    );

-- Plus a TRANSITION CHECK ensuring the number is official:
ALTER COLLECTION invoices ADD TRANSITION CHECK official_number_required (
    OLD.status != 'draft' OR NEW.invoice_number NOT LIKE 'DRAFT-%'
);
```

This ensures `draft → issued` only succeeds when the invoice number has been replaced with an official one by Origin.

## State Constraint: draft → issued

The state machine for invoices in an offline-first system:

```sql
ALTER COLLECTION invoices ADD CONSTRAINT invoice_flow
    ON COLUMN status TRANSITIONS (
        'draft'     -> 'submitted',
        'submitted' -> 'approved'    BY ROLE 'manager',
        'submitted' -> 'draft',
        'approved'  -> 'issued'      BY ROLE 'accountant',
        'issued'    -> 'voided'      BY ROLE 'controller'
    );
```

On the **device (NodeDB-Lite):**

- Reps create drafts and submit them
- `draft → submitted` is allowed without a role guard
- `submitted → approved` requires the `manager` role — only available on Origin

On **Origin:**

- Managers approve, accountants issue, controllers void
- Each transition is role-guarded and audited
- The state constraint prevents skipping steps (e.g., `draft → issued` is not in the list)

## Offline Tax Rate Sync

**Problem:** Tax rates change by jurisdiction and effective date. Offline devices need current rates but can't query Origin in real-time.

**Pattern:**

1. **Origin maintains the rate table:**

```sql
CREATE COLLECTION tax_rates (
    id          STRING PRIMARY KEY,
    jurisdiction STRING NOT NULL,
    rate        DECIMAL NOT NULL,
    effective_from STRING NOT NULL
) WITH (engine='document_strict');
```

2. **Sync subset to Lite:** Use CRDT shape subscriptions to sync only relevant jurisdictions. On the device, establish a shape subscription that filters by jurisdiction:

```rust
// Wire-level shape subscription (via NodeDB-Lite client)
// This is a client/sync-level feature, not runnable SQL
client.shape_subscribe("tax_rates", json!({
    "jurisdiction": ["US-CA", "US-NY"]
})).await?;
```

3. **Lookup at invoice time:** Use TEMPORAL_LOOKUP (or equivalent local query) to find the rate effective at the invoice date:

```sql
SELECT * FROM tax_rates
WHERE jurisdiction = 'US-CA'
  AND effective_from <= '2026-04-01'
ORDER BY effective_from DESC
LIMIT 1;
```

4. **Reconcile on sync:** If a rate changed between offline creation and sync, Origin can flag invoices for review. The app decides whether to recalculate or accept the offline-computed tax.

**Key principle:** Tax computation logic lives in the application, not the database. NodeDB provides the primitives (temporal lookup, CRDT sync, decimal arithmetic) — the app decides which rate to apply and when to recalculate.

## Sync Scope

Columnar, Vector, FTS, and Spatial collections now participate in **inbound sync** to NodeDB-Lite (alongside the document and CRDT engines). Schema changes (DDL) broadcast to connected Lite and WASM sessions after the Origin catalog commits, so embedded clients automatically pick up new collections and columns without explicit subscription. This enables seamless schema evolution in offline-first applications.

**Schema announce.** A sync peer announces each collection's schema (`CollectionSchema` message) before sending any shape or delta data. Collections unknown to the receiver are materialized into its catalog and propagate cluster-wide via Raft — create-only, so an existing collection is never clobbered. A Lite client can therefore bootstrap collections on Origin simply by announcing their schema.

**Constraint validation and rejects.** UNIQUE, FK, required-field, and CHECK constraints are validated when deltas apply on Origin. Because CRDT deltas are commutative and already merged on import, enforcement is a post-hoc pass: a violating row is quarantined and the client receives a `DeltaReject` carrying a typed `CompensationHint` (e.g., `UniqueViolation { field, conflicting_value }`, `RetryWithDifferentValue`, `ManualIntervention`) telling it precisely what to fix. A rejected delta does not wedge the stream — later deltas continue to apply. A delta must also carry exactly **one document** — cross-engine identity binds one surrogate per delta — so a delta writing rows outside its frame target is rejected with `RejectedConstraint { constraint: "crdt_single_document_delta" }`.

**Durability.** Acknowledged sync writes are quorum-durable: the delta commits through the data group's Raft log before the ack, so it survives leader failover. Producers carry `(producer_id, epoch, seq)` provenance; replays are acknowledged as duplicates rather than double-applied.

## NodeDB-Lite Usage

### Creating Offline Invoices (Swift/Kotlin via FFI)

```swift
// iOS — via nodedb-lite-ffi
let db = NodeDbLite.open(path: "invoices.db")

// Insert draft with provisional number
db.execute("""
    INSERT INTO invoices (invoice_number, customer_id, amount, status)
    VALUES ('DRAFT-\(ULID())', 'cust-005', 750.00, 'draft')
""")

// Sync when online
db.sync(url: "wss://origin.example.com/sync", token: authToken)
```

### WASM (Browser)

```javascript
import { NodeDbLite } from "nodedb-lite-wasm";

const db = await NodeDbLite.open("invoices");

await db.execute(`
    INSERT INTO invoices (invoice_number, customer_id, amount, status)
    VALUES ('DRAFT-${crypto.randomUUID()}', 'cust-005', 750.00, 'draft')
`);

// Sync via WebSocket
await db.sync("wss://origin.example.com/sync");
```

### Key Points

- All eight engines work locally on Lite (document, KV, columnar, timeseries, spatial, vector, array, plus graph and FTS overlays)
- CRDT sync is transparent — local writes are optimistic, validated at Origin
- Constraint violations on Origin produce compensation hints in the dead-letter queue
- The same Decimal type, query engine, and data model work on both Origin and Lite
- Sub-millisecond local reads with no network dependency
