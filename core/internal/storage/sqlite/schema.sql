-- Spitfire v1 SQLite schema.
-- Applied by the sqlite storage backend (Phase 1). Kept in-repo from Phase 0
-- so the tables the interface implies are reviewable early.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS queues (
    name         TEXT PRIMARY KEY,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    queue           TEXT NOT NULL,
    state           TEXT NOT NULL
                    CHECK (state IN ('pending','leased','succeeded','failed','dead')),
    payload         BLOB NOT NULL,
    idempotency_key TEXT,
    attempt         INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    priority        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    scheduled_at    TEXT NOT NULL,
    leased_until    TEXT,
    lease_owner     TEXT,
    completed_at    TEXT,
    last_error      TEXT,
    FOREIGN KEY (queue) REFERENCES queues(name) ON DELETE RESTRICT
);

-- Idempotent enqueue: one job per (queue, idempotency key) when the key is set.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND idempotency_key != '';

-- Claim path: ready pending jobs for a queue, priority then schedule.
CREATE INDEX IF NOT EXISTS jobs_claim
    ON jobs (queue, state, scheduled_at, priority DESC, id);

-- Lease reclaim: expired leases.
CREATE INDEX IF NOT EXISTS jobs_lease_expiry
    ON jobs (leased_until)
    WHERE state = 'leased';

-- Dashboard filters.
CREATE INDEX IF NOT EXISTS jobs_state_queue
    ON jobs (queue, state, id);

CREATE TABLE IF NOT EXISTS job_history (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id  TEXT NOT NULL,
    type    TEXT NOT NULL,
    detail  TEXT,
    at      TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS job_history_job
    ON job_history (job_id, id);

CREATE TABLE IF NOT EXISTS schedules (
    id            TEXT PRIMARY KEY,
    queue         TEXT NOT NULL,
    cron_expr     TEXT,              -- NULL for one-shot delayed
    payload       BLOB NOT NULL,
    max_attempts  INTEGER NOT NULL DEFAULT 3,
    priority      INTEGER NOT NULL DEFAULT 0,
    next_fire_at  TEXT NOT NULL,
    last_fired_at TEXT,
    created_at    TEXT NOT NULL,
    FOREIGN KEY (queue) REFERENCES queues(name) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS schedules_next
    ON schedules (next_fire_at);
