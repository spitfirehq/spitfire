# Spitfire — Polyglot Background Job Queue

> **Go module root:** `github.com/spitfirehq/spitfire`

## One-line pitch

Hangfire's developer experience, in Python, TypeScript, and .NET. One binary, one directory, no database.

## Operating model (locked)

**This is a fully AI-coded project delivered in phases.** The owner does not write the implementation. The owner reviews each phase, experiments with it, breaks it, and builds a working mental model of how the system works.

That flips the constraint:

| | Code-along learning project | AI-coded phased project |
|---|---|---|
| Bottleneck | Hours to write code | Hours to *understand* code |
| Phase size | What one person can build | What one person can *absorb* |
| Definition of done | Tests pass | Tests pass **and** owner can explain the moving parts |
| Custom hard things (WAL, Raft, wire protocol) | Attractive *because* they're hard to write | Attractive *only if* they're readable and interrogable |

**Phase gate (every phase):** green tests + phase documentation written + a short "tinker & understand" checklist the owner completes before the next phase starts. Comprehension is the acceptance criteria, not just CI.

**Phase documentation (every phase):** at the end of each phase, write `docs/phases/phase-<N>-<slug>.md` covering what was implemented, key design decisions and why, interfaces/APIs introduced, how to run and poke at it, and open questions or deliberate omissions. This is a hard deliverable — the phase is not done without it.

## Positioning

A durable background job queue and scheduler that:

- Ships as a **single binary with a single data directory** — no Postgres, Redis, or SQL Server required
- Covers multiple language ecosystems (v1: .NET, Python; v1.1: TypeScript)
- Offers Hangfire-class dashboard polish and developer ergonomics (polish lands late — see phases)
- Sets up a future expansion into low-latency / financial domains via a later C++ SDK

**Differentiation vs existing tools:**

- Hangfire: .NET-only → this is polyglot
- Hatchet: Postgres-required, no .NET/C++ → this needs no database and covers more languages
- Temporal: heavy workflow-engine programming model → this is a queue with embedded-deploy DX
- Faktory: stalled, no first-class .NET → this targets that audience directly

## Goals

**Technical:**

- Production-grade durable queue with **at-least-once** delivery semantics
- Sub-10ms enqueue latency in batch mode on commodity NVMe
- Sustained throughput target: 10k+ jobs/sec single-node, single queue
- Dashboard with live updates (read-only first, actions later)
- Single-node v1; multi-node HA is an explicit v2 (see Deferred)

**Learning (what the owner should understand deeply):**

- Queue semantics: at-least-once, leases, visibility timeouts, idempotency keys, DLQ, backoff
- SQLite as a storage engine: transactions, WAL mode, `PRAGMA synchronous`, crash behavior, what durability actually costs
- gRPC + proto3 service design and schema-first SDKs
- Go service architecture (process model, concurrency, graceful shutdown) — read and interrogate, even though AI writes it
- Honest benchmarking: methodology, fault injection, what a number actually claims

## v1 scope

**In scope:**

- Go core (single binary)
- SQLite-backed storage behind a small storage interface (no custom WAL)
- .NET + Python SDKs with idiomatic APIs (TypeScript in v1.1)
- gRPC transport + proto3 job schemas
- Cron + delayed scheduling
- Retries with backoff, dead-letter queues
- Worker heartbeats, leases, visibility timeouts
- Idempotent enqueue (dedupe keys)
- Three durability modes (per-queue), mapped onto SQLite PRAGMAs
- Minimal read-only dashboard SPA bundled into the binary (SSE live updates)
- Token auth on the wire and on the dashboard — day one
- Operator metrics (queue depth, oldest job age, lease expiries) + structured logs

**Deferred (explicitly out of v1):**

| Item | When | Notes |
|---|---|---|
| Multi-node HA / Raft | v2 | Different design problem with SQLite — see Deferred |
| Custom QUIC + binary wire protocol | spike / v2 | gRPC is the v1 transport |
| TypeScript SDK | v1.1 | Third SDK after .NET + Python feel good |
| Custom WAL storage engine | optional appendix | Learning spike only, not production path |
| C++ SDK | v1.2 | Fintech push |
| Postgres / Redis / SQLite-file adapters | v1.x | Storage interface is designed to allow them |
| S3 snapshot/restore | v1.x | |
| Dashboard polish (actions, batches, charts) | after v1 | Read-only first |
| OpenTelemetry traces | after metrics | Metrics matter more for a queue |
| Multi-user / RBAC | v2 | |

## Architecture

```
SDKs (.NET, Python → TypeScript)
        │  gRPC (proto3)
        ▼
Go core (single binary)
  ├── gRPC API (enqueue, claim, ack, fail, admin)
  ├── Worker protocol (leases, heartbeats, server-streamed jobs)
  ├── Scheduler (cron, delayed)
  ├── Auth (token)
  └── Dashboard (embedded SPA, SSE)
        │
        ▼
Storage interface (enqueue, claim, lease, terminal states, history, notify)
        │
        ▼
SQLite engine (single file, WAL mode)
```

Storage is a narrow Go interface. v1 backend is SQLite. An in-memory fake exists for tests and validates the interface doesn't leak SQL specifics. Postgres/Redis adapters are post-v1 and only if there's demand.

## Locked design decisions

### Storage: SQLite, not a custom WAL

State lives in a single SQLite database in the data directory. Jobs, queues, schedules, and history are tables. Durability comes from SQLite's own WAL + `PRAGMA synchronous` settings. Backup is `cp data/spitfire.db*` (or `VACUUM INTO`).

**Rationale (updated for AI-coded model):**

- The learning goal is *understanding durability*, not *reimplementing it*. SQLite is readable, documented, and experimentally probeable (crash a process, flip PRAGMAs, inspect WAL files). An AI-written custom WAL is high cognitive load and high subtle-bug risk.
- Ops story is unchanged: single binary, single directory, no server database.
- Crash recovery, fsync cost, and group-commit effects are still visible — they're SQLite's, and you can measure and reason about them honestly.

**Durability modes (per-queue):**

| Mode | SQLite mapping | Tradeoff |
|---|---|---|
| `sync` | `PRAGMA synchronous=FULL` | Strictest, slowest |
| `group` (default) | `PRAGMA synchronous=NORMAL` + WAL | Throughput with near-strict durability; matches SQLite's normal WAL group-commit behavior |
| `async` | `PRAGMA synchronous=OFF` | Fastest; accepts data loss on crash |

### Transport: gRPC + proto3

gRPC gives polyglot codegen, streaming (server-pushed work), and mature clients in every target language. Job payloads are proto3 messages declared in `.proto` files; codegen produces types per SDK.

Custom QUIC + binary framing is deferred: it's a poor fit for enterprise .NET networks, hard to debug, and not required for the product thesis. It remains a good *spike* if/when we want low-latency transport — it must not gate v1.

### Concurrency: claim-based workers, SQLite as the serializer

Workers pull jobs over gRPC (long-poll / server stream). Claims are short SQLite write transactions. No shared-memory writer goroutines across languages — the database is the single source of truth. Throughput target is met with WAL mode + batched claims + prepared statements. Vertical scale is one big box; multi-node is v2.

### Job schemas: proto3 DSL with codegen

Jobs declared in `.proto` files. Codegen produces idiomatic *types* in each SDK. Idiomatic ergonomics (decorators, DI attributes) are **hand-written thin wrappers** over generated types — not generated. Don't build a codegen product.

### SDK ergonomics (idiomatic, not translated)

- **.NET (first):** attribute-based registration (`[Job]`), DI integration, .NET 8+. This is the product wedge — target Hangfire users for migration.
- **Python (second):** async + sync APIs, `@job` decorator, asyncio-native, optional Pydantic integration. Fastest ad-hoc scripting loop once the wedge is validated.
- **TypeScript (v1.1):** Node + Bun support, strong type inference for job payloads, decorator-based registration.

Order is market-wedge first (.NET/Hangfire), then scripting ergonomics (Python), then coverage (TS).

### Dashboard: embedded SPA, read-only first

React + Vite bundled via `go:embed`. SSE for live updates. v1 is read-only job/queue views + search. Retry/requeue actions and batch views land after v1, once the core is solid. Hangfire-class polish is a product surface, not a phase-1 feature.

### Auth: tokens from day one

gRPC metadata bearer token for SDKs; same token (or a separate admin token) for the dashboard. No unauthenticated-by-default dashboards. Multi-user/RBAC is v2.

### HA: deferred, and the future design is already sketched

Multi-node is **not** v1. With SQLite as the engine, HA is a different design problem (replicate ops, not the file). When we do it, the plan is:

- Replicate *logical queue operations* via `hashicorp/raft` (log = op log, not a second file WAL)
- Each node applies ops to its local SQLite as the FSM/state
- Avoid double-logging: the raft log is the replication source of truth; SQLite is local applied state

This is locked so v2 doesn't fight v1's architecture. Rolling our own consensus remains out of scope.

### Naming + identity (locked)

Bare `spitfire` is taken on npm (abandoned), PyPI (abandoned), and crates.io (active) — ship under a branded prefix (same pattern as Temporal / Hatchet).

| Surface | Name | Status |
|---|---|---|
| GitHub org | `spitfirehq` | ✅ claimed |
| Domain | `spitfire.sh` | planned — purchase after first milestone |
| Go module | `github.com/spitfirehq/spitfire` | follows from org |
| npm SDK | `@spitfirehq/sdk` (scope `@spitfirehq` free) | ✅ claimed |
| PyPI SDK | `spitfire-client` (free) | org request made, waiting for confirmation |
| NuGet | `Spitfire` (bare is free) + `Spitfire.Client` | ✅ claimed |

## Phased roadmap

Each phase is a comprehension-sized chunk. The AI implements; the owner completes the **Tinker & understand** list before the next phase starts.

### Phase 0 — Skeleton
**Builds:** Go module, directory layout, CI (`go test`, lint), SQLite schema (jobs, queues, schedules, history), storage interface + in-memory fake.

**Concepts:** what the tables mean; what the interface promises (atomic enqueue, claim, lease renew, terminal write, history).

**Tinker & understand:**
- Draw the job state machine (`pending → leased → succeeded | failed | dead`)
- Point at the interface method that must be atomic and explain why
- Run the in-memory fake tests and say what they prove about the interface

**Done when:** CI green; owner can describe every table's role.

### Phase 1 — Queue engine
**Builds:** enqueue (with idempotency key), claim-next, ack/fail, visibility timeout, history append. SQLite transactions + WAL mode + durability modes.

**Concepts:** at-least-once vs exactly-once; why claim + lease beats "mark in-flight"; what `PRAGMA synchronous` actually does; durability mode tradeoffs.

**Tinker & understand:**
- Kill `-9` a process mid-enqueue in `sync` vs `async` mode; observe what's in the DB
- `PRAGMA synchronous` matrix: measure enqueue latency in each mode
- Run a double-enqueue with the same idempotency key; prove dedupe
- Explain what happens if a worker dies between claim and ack

**Done when:** property tests cover claim/ack/timeout races; owner can explain a lease expiry without looking at code.

### Phase 2 — gRPC worker protocol
**Builds:** proto3 service (Enqueue, Ack, Fail, StreamJobs / long-poll claim, RenewLease, admin RPCs), token auth interceptor, graceful shutdown.

**Concepts:** gRPC unary vs streaming; how auth lands in metadata; why leases need renewal; server-push vs pull for work.

**Tinker & understand:**
- Hit the API with `grpcurl` (auth on and off)
- Block a worker past the visibility timeout; watch the job go redelivered
- Packet-dump or log-trace one claim round-trip

**Done when:** two concurrent workers never double-claim under lease expiry; owner can diagram one full job lifecycle on the wire.

### Phase 3 — Reliability
**Builds:** retries with exponential backoff + jitter, max attempts, DLQ, poison-message handling, dedupe-key docs.

**Concepts:** why at-least-once needs idempotent handlers; backoff design; what belongs in DLQ vs retry forever.

**Tinker & understand:**
- Enqueue a job that always fails; watch backoff timing and DLQ landing
- Craft a poison payload; prove it doesn't hot-loop
- Break an idempotent handler; show redelivery is safe

**Done when:** retry/DLQ behavior is observable in the dashboard or CLI; owner can write a correct idempotent handler from memory.

### Phase 4 — Scheduling
**Builds:** delayed jobs, cron schedules, catch-up/missed-fire policy, timezone handling.

**Concepts:** wall clock vs monotonic time; what "missed cron fire" means; storing next-fire-time as the schedule's source of truth.

**Tinker & understand:**
- Schedule a job 5s out; kill the process at 2s; restart; prove it fires once
- Change system time across a cron boundary; observe catch-up policy
- Two nodes is out of scope — but ask: what *would* break? (answer gate for phase)

**Done when:** delayed/cron jobs survive restart with no double-fire in the happy path.

### Phase 5 — .NET SDK (Hangfire wedge)
**Builds:** `Spitfire` + `Spitfire.Client` NuGet packages, `[Job]` attributes, DI integration, worker host (generic host), typed payloads from proto codegen, retries config surface.

**Concepts:** .NET generic host + DI; how Hangfire users would migrate; async/await job handlers; codegen types behind idiomatic wrappers.

**Tinker & understand:**
- Minimal ASP.NET or worker-service app: enqueue 100 jobs, process them, print timings
- Side-by-side: same job in Hangfire vs Spitfire (naming, retries, dashboard)
- Attribute registration vs manual; see what codegen generated vs wrapper
- Break the server; see client reconnect/backoff behavior
- Explain in one paragraph why a Hangfire user would switch

**Done when:** owner can migrate a toy Hangfire app and articulate the tradeoffs.

### Phase 6 — Dashboard + ops surface
**Builds:** embedded React SPA (read-only): queues, jobs (list/filter/detail), history, live SSE updates, auth. Operator metrics endpoint (queue depth, oldest job age, lease expiries, enqueue/complete rates). `spitfire` CLI (status, enqueue, purge DLQ).

**Concepts:** SSE vs websockets; which metrics an operator actually needs; embedded asset serving.

**Tinker & understand:**
- Run the binary, open the dashboard, enqueue jobs from the SDK, watch live
- Saturate a queue; read the metrics and say which one would page you
- Try dashboard without a token; prove it's denied

**Done when:** owner can diagnose "jobs are stuck" using only the dashboard + metrics.

### Phase 7 — Benchmarks + hardening
**Builds:** reproducible benchmark harness (fixed payloads, durability modes, queue counts), fault-injection tests (kill, disk full, corrupt file), honest results doc.

**Concepts:** what a benchmark number claims; methodology; how to lie with benchmarks.

**Tinker & understand:**
- Run the harness in all three durability modes; record numbers
- Corrupt the SQLite file mid-run; observe failure mode (and that we don't corrupt silently)
- Red-team the 10k/sec claim: under what config is it true? Write that config down.

**Done when:** results doc states payload size, durability mode, hardware, and methodology; owner can defend the numbers.

### Phase 8 — Python SDK
**Builds:** `spitfire-client` (sync + async), `@job` decorator, worker loop, typed payloads from proto codegen, retries config surface, README examples.

**Concepts:** how codegen types flow into idiomatic APIs; background worker lifecycle; structured failure reporting; contrast with the .NET surface.

**Tinker & understand:**
- 15-line script: enqueue 100 jobs, process them, print timings
- Decorator registration vs manual; see what codegen generated vs wrapper
- Break the server; see client reconnect/backoff behavior

**Done when:** owner can write a small real worker (e.g. resize an image, send an email) without reading SDK source.

### Phase 9 — v1 polish
**Builds:** docs site (quickstart, concepts, migration-from-Hangfire), examples/, error-message audit, versioning/upgrade notes for the SQLite schema, release pipeline.

**Concepts:** documentation as product; semver for a queue; schema migration story.

**Tinker & understand:**
- Follow the quickstart cold (no memory of the code); note every stumble
- Upgrade across a schema version bump; prove no data loss

**Done when:** a stranger can go from zero to processing jobs in 10 minutes using only the docs.

---

## Deferred, in detail

**Multi-node HA (v2).** SQLite doesn't replicate. HA means replicating *operations* via Raft and applying them to local SQLite — not sharing a file, not a second WAL. Spec this properly before any clustering code. Membership, split-brain, rolling upgrades, and snapshot shipping are a project on their own.

**Custom QUIC transport (spike/v2).** Valuable as a learning spike for wire-protocol design and for future low-latency use cases. Never blocks v1. When done, it's a *parallel* transport behind the same RPC surface, not a rewrite.

**Custom WAL (appendix, not product).** Optional later spike: implement a tiny WAL + crash recovery *to understand what SQLite does for you*. Results go in `docs/spikes/`. It is not a production storage path unless it ever beats SQLite on a measured goal.

**Postgres/Redis adapters.** Only if users demand shared-server deployments. The storage interface is the bet that keeps this possible.

## Tech stack

- **Core language:** Go (latest stable)
- **Storage:** SQLite via `modernc.org/sqlite` (pure Go, no cgo) in WAL mode
- **Transport:** gRPC + protobuf (`google.golang.org/grpc`, `google.golang.org/protobuf`)
- **Schemas:** proto3; `protoc`/Buf codegen per SDK; hand-written idiomatic wrappers
- **Observability:** `log/slog` + Prometheus-style metrics endpoint
- **Dashboard:** React + Vite, bundled via `embed`
- **Build:** `go build`, cross-compile linux/darwin (windows deferred)
- **CI:** GitHub Actions — test, lint, benchmark smoke

## Repository layout

```
/
├── core/               # Go binary (server, storage, scheduler, dashboard embed)
│   └── internal/
│       ├── storage/    # interface + sqlite + memory
│       ├── api/        # gRPC
│       ├── scheduler/
│       └── auth/
├── sdks/
│   ├── python/
│   ├── dotnet/
│   └── typescript/     # v1.1
├── dashboard/          # React + Vite SPA
├── schemas/            # proto3 definitions + codegen scripts
├── benchmarks/
├── docs/
│   ├── phases/         # end-of-phase write-ups (one per phase, required)
│   ├── concepts/       # leases, durability modes, idempotency, ...
│   └── spikes/         # optional WAL spike, QUIC spike
└── examples/
```

## Conventions

- **License:** Apache 2.0
- **Versioning:** SemVer; pre-1.0, breaking changes expected
- **Commits:** Conventional Commits
- **Branching:** trunk-based, short-lived feature branches
- **Phase gates:** AI implements a phase → writes `docs/phases/phase-<N>-<slug>.md` → owner completes Tinker & understand → next phase. Do not start phase N+1 on an "almost" phase N.

## Constraints + context

- **Owner:** senior fullstack (TypeScript, .NET, Python, Azure, AI integration). Systems learning is the point; Go is a read-and-interrogate language here, not the primary craft target.
- **Delivery model:** AI-coded in phases. Owner time goes to review, experiments, and understanding (budget ~10–20 hrs/week).
- **Timeline:** phases are sized for comprehension, not coding hours. Expect to spend 1–3 weeks of owner attention per phase. A phase that needs more than that is too big — split it.
- **Supporting tech being lifted, not deeply learned:** DevOps/IaC, security/cryptography, mobile, data engineering.

## Immediate next actions

1. **Phase 0** — repo bootstrap under `github.com/spitfirehq/spitfire`, CI, SQLite schema, storage interface + memory fake
2. **Lock proto3 job + RPC schemas** for Phases 1–2 (even if implementation lands later)
3. **Write the job state machine doc** (`docs/concepts/job-lifecycle.md`) — the one diagram every phase leans on
