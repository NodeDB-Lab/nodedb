# Agent Memory Architecture

AI agents need three kinds of memory: episodic (what happened), semantic (what I know), and working (what I'm doing right now). NodeDB provides all three in one database — strict collections for structured memory, vector search for relevance, KV with TTL for ephemeral state, and a cron scheduler for background consolidation.

## Episodic Memory (Short-Term)

Stores raw conversation turns and tool call logs. Searchable by vector similarity (find relevant past interactions) and by time (recent context window).

```sql
-- Schema: every conversation turn is a row
CREATE COLLECTION episodic_memory TYPE strict (
    id         STRING PRIMARY KEY,
    agent_id   STRING NOT NULL,
    session_id STRING NOT NULL,
    role       STRING NOT NULL,
    content    STRING NOT NULL,
    embedding  VECTOR(1536),
    tool_calls STRING,
    created_at DATETIME DEFAULT NOW()
);

CREATE VECTOR INDEX ON episodic_memory FIELDS embedding DIMENSION 1536 METRIC cosine;

-- Insert a conversation turn
INSERT INTO episodic_memory VALUES (
    $turn_id, $agent_id, $session_id, 'user',
    'How do I configure the replication factor?',
    $turn_embedding, NULL, NOW()
);

-- Retrieve: recent turns in this session (context window)
SELECT role, content
FROM episodic_memory
WHERE agent_id = $agent AND session_id = $session
ORDER BY created_at DESC
LIMIT 10;

-- Retrieve: semantically relevant past interactions (any session)
SELECT session_id, role, content, vector_distance() AS relevance
FROM episodic_memory
WHERE agent_id = $agent
  AND embedding <-> $query_embedding
LIMIT 5;
```

## Semantic Memory (Long-Term)

Distilled knowledge — facts, summaries, learned preferences. Higher signal-to-noise than raw episodic memory. Updated by a consolidation process that summarizes episodic memories.

```sql
CREATE COLLECTION semantic_memory TYPE strict (
    id         STRING PRIMARY KEY,
    agent_id   STRING NOT NULL,
    fact       STRING NOT NULL,
    embedding  VECTOR(1536),
    confidence DECIMAL,
    source     STRING,
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW()
);

CREATE VECTOR INDEX ON semantic_memory FIELDS embedding DIMENSION 1536 METRIC cosine;

-- Insert a learned fact
INSERT INTO semantic_memory VALUES (
    $fact_id, $agent_id,
    'User prefers YAML configuration over JSON for Kubernetes deployments',
    $fact_embedding, 0.92, 'session-2025-03-15-001', NOW(), NOW()
);

-- Retrieve relevant knowledge for the current query
SELECT fact, confidence, vector_distance() AS relevance
FROM semantic_memory
WHERE agent_id = $agent
  AND confidence > 0.5
  AND embedding <-> $query_embedding
LIMIT 5;

-- Update confidence when a fact is reconfirmed
UPDATE semantic_memory
SET confidence = LEAST(confidence + 0.05, 1.0), updated_at = NOW()
WHERE id = $fact_id;
```

## Working Memory (Ephemeral)

Current task state — plans, intermediate results, tool outputs. Uses the KV engine with TTL for automatic expiry when the task is done.

```sql
-- Create a KV collection for working memory
CREATE COLLECTION working_memory TYPE kv;

-- Store current plan (expires in 1 hour)
INSERT INTO working_memory {
    key: 'agent-1:current_plan',
    goal: 'deploy v2.3',
    steps: '["build", "test", "deploy"]',
    current_step: 1,
    ttl: 3600
};

-- Read current state
SELECT * FROM working_memory WHERE key = 'agent-1:current_plan';

-- Store intermediate tool output
INSERT INTO working_memory {
    key: 'agent-1:build_output',
    status: 'success',
    artifact: 'v2.3.0-rc1.tar.gz',
    ttl: 3600
};

-- Update progress
UPDATE working_memory SET current_step = 2 WHERE key = 'agent-1:current_plan';

-- When task completes, KV entries expire automatically via TTL.
-- No cleanup needed.
```

## Memory Consolidation (Scheduled)

A background job periodically summarizes old episodic memories into semantic facts. This keeps episodic memory bounded while preserving important knowledge long-term.

```sql
-- Create a scheduled job that runs every 6 hours
CREATE SCHEDULE memory_consolidation
    CRON '0 */6 * * *'
    AS BEGIN
        SELECT id, content FROM episodic_memory
        WHERE agent_id = 'agent-1'
          AND created_at < NOW() - INTERVAL '7 days'
        ORDER BY created_at
        LIMIT 100;
    END;

-- The schedule runs the SQL and your app processes the results:
-- 1. Read old episodic memories from the schedule output
-- 2. Send them to an LLM: "Summarize these interactions into key facts"
-- 3. Insert the summaries as semantic memory
-- 4. Delete the consolidated episodic memories

-- For the deletion step after consolidation:
-- DELETE FROM episodic_memory WHERE id IN (...consolidated_ids...);
```

**Alternative: trigger-based consolidation.** Use a CDC change stream to trigger consolidation when episodic memory exceeds a threshold:

```sql
-- Monitor episodic memory inserts
CREATE CHANGE STREAM episodic_monitor ON episodic_memory;

-- Your app subscribes to the stream and counts inserts per agent.
-- When count exceeds threshold, trigger consolidation for that agent.
SELECT * FROM STREAM episodic_monitor CONSUMER GROUP consolidator LIMIT 100;
```

## Putting It All Together

When an agent receives a query, it assembles context from all three memory layers:

```sql
-- 1. Recent context (episodic, this session)
SELECT content FROM episodic_memory
WHERE agent_id = $agent AND session_id = $session
ORDER BY created_at DESC LIMIT 10;

-- 2. Relevant past interactions (episodic, any session)
SELECT content, session_id FROM episodic_memory
WHERE agent_id = $agent
  AND embedding <-> $query_embedding
LIMIT 5;

-- 3. Relevant knowledge (semantic)
SELECT fact, confidence FROM semantic_memory
WHERE agent_id = $agent
  AND embedding <-> $query_embedding
LIMIT 5;

-- 4. Current task state (working)
SELECT * FROM working_memory WHERE key = 'agent-1:current_plan';
```

Your app assembles these into an LLM prompt:

```
System: You are a deployment assistant.
Working Memory: Currently on step 2 of deploy v2.3 (build succeeded).
Semantic Memory: User prefers YAML config. Uses Kubernetes.
Recent Context: [last 10 messages]
Relevant Past: [5 similar past interactions]
User: {current question}
```
