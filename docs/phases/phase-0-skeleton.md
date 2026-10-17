# Phase 0 — Skeleton

**Status:** complete  
**Date:** 2026-10-17

## What was implemented

| Piece | Location |
|---|---|
| Go module | `go.mod` — `github.com/spitfirehq/spitfire` |
| Storage contract | `core/internal/storage/storage.go` |
| SQLite schema | `core/internal/storage/sqlite/schema.sql` |
| In-memory fake | `core/internal/storage/memory/memory.go` + `memory_test.go` |
| Job state machine doc | `docs/concepts/job-lifecycle.md` |
| CI | `.github/workflows/ci.yml` (vet, race tests, build, golangci-lint) |

Directory layout matches the README (`core/`, `sdks/`, `docs/concepts/`, `docs/phases/`, `.github/`). `schemas/`, `dashboard/`, `benchmarks/`, `examples/` are created as phases need them.

## Key design decisions

### `storage.Storage` is the seam

Every persistence operation the product needs is one interface method:

- `Enqueue` — atomic insert + idempotency + history
- `Claim` — exclusive lease of one ready job; **also reclaims expired leases**
- `RenewLease` / `Complete` / `Fail` — owner-checked lease management
- `Requeue` — manual retry / DLQ reprocess
- `History`, `ListJobs`, `ListQueues` — dashboard reads
- `Notify` — hint channel for live updates (lossy by design)

Phase 1 implements this on SQLite. The in-memory fake exists so interface tests do not depend on SQL.

### Job states (see `docs/concepts/job-lifecycle.md`)

`pending → leased → succeeded | failed | dead`, with retry returning `leased → pending` and `Requeue` leaving any terminal state.

- `failed` = terminal non-retryable / manual fail
- `dead` = DLQ after `max_attempts` exhausted

### Attempt counting

`attempt` increments on `Claim`, not on `Fail`. A claimed-then-expired job still consumed an attempt. This is the anti-hot-loop lever.

### Idempotency

Unique `(queue, idempotency_key)` when the key is set. Duplicate enqueue returns the existing job with `Duplicate=true` — it does not error.

### SQLite schema (not yet wired)

`schema.sql` is the single source of truth for tables/indexes: `queues`, `jobs`, `job_history`, `schedules`. Partial unique index for idempotency; claim and lease-reclaim indexes match the interface's access patterns. Phase 1 applies this DDL and implements `Storage` on SQLite.

### Backoff

The memory fake uses a fixed `50ms * 2^attempt` (cap 5s) so retry tests are fast. Phase 3 makes backoff configurable.

## How to run / poke at it

```bash
go test ./core/internal/storage/memory -race -v
```

That package is the executable spec of the storage contract.

## Tinker & understand (owner gate)

1. Draw the job state machine from memory, then check `docs/concepts/job-lifecycle.md`.
2. Point at the interface method that must be atomic with its history append — why? (`Enqueue` / `Claim` / `Fail` / `Complete`.)
3. Run the memory tests. What do they prove about the *interface* versus the *fake*?
4. Read `schema.sql` and say what each table is for in one sentence.

## Open questions / deliberate omissions

- No SQLite implementation yet (Phase 1).
- No gRPC / proto (Phase 2). Payload is opaque `[]byte` so far.
- Backoff formula is placeholder (Phase 3).
- `Notify` drops events when the buffer is full — documented as hints, not a log.
- `ListJobs.NextID` cursor uses job id; fine for the fake, must match SQLite rowid/id ordering in Phase 1.
- Windows is out of v1 (no `GOOS=windows` in CI).
