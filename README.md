# Spitfire — Polyglot Background Job Queue

> **GitHub org:** [`spitfirehq`](https://github.com/spitfirehq) (claimed)
> **Domain:** `spitfire.sh` (planned — purchase after first milestone)
> **Go module root:** `github.com/spitfirehq/spitfire`

## One-line pitch

Hangfire's developer experience, in Python, TypeScript, and .NET. One binary, one directory, no database.

## Positioning

A durable background job queue and scheduler that:

- Ships as a **single binary with a single data directory** — no Postgres, Redis, or SQL Server required
- Covers multiple language ecosystems (v1: Python, TypeScript, .NET; v1.1+: Go; v1.2+: C++)
- Offers Hangfire-class dashboard polish and developer ergonomics
- Sets up a future expansion into low-latency / financial domains via the C++ SDK

**Differentiation vs existing tools:**

- Hangfire: .NET-only → this is polyglot
- Hatchet: Postgres-required, no .NET/C++ → this needs no database and covers more languages
- Temporal: heavy workflow-engine programming model → this is a queue with embedded-deploy DX
- Faktory: stalled, no first-class .NET → this targets that audience directly

## Goals

**Technical:**

- Production-grade durable queue with at-least-once delivery semantics
- Sub-10ms enqueue latency in group-commit mode on commodity NVMe
- Sustained throughput target: 10k+ jobs/sec single-node, single queue
- HA via Raft consensus (multi-node clustering)
- Dashboard with live updates, polish comparable to Hangfire's

**Learning (why this project exists):**

- Go to expert level (idiomatic concurrency, runtime internals, profiling)
- WAL design + crash recovery (fsync semantics, group commit, log compaction)
- Raft consensus via `hashicorp/raft` integration
- QUIC + custom binary wire protocol design
- Polyglot SDK ergonomics and schema-driven codegen
- Honest distributed-systems benchmarking with fault injection

## v1 scope

**In scope:**

- Go core (single binary)
- File-based custom storage engine (WAL + memory index, no DB dependency)
- Python, TypeScript, .NET SDKs with idiomatic APIs
- QUIC transport via `quic-go` + custom binary application protocol
- `hashicorp/raft`-backed HA (Raft replicates WAL entries across cluster)
- Cron + delayed scheduling
- Retries with backoff, dead-letter queues
- Worker heartbeats, leases, visibility timeouts
- Schema-first job definitions (proto3) with codegen for all 3 SDKs
- Three durability modes (per-queue): `sync`, `group` (default), `async`
- Dashboard SPA bundled into the binary, SSE-based live updates
- OpenTelemetry distributed tracing across job chains

**Out of v1, planned for later:**

- Postgres backend adapter — v1.1
- Redis backend adapter — v1.2
- SQLite backend adapter — on demand
- Go SDK — v1.1
- C++ SDK — v1.2 (fintech push)
- S3-compatible snapshot/restore — v1.x
- Multi-user / RBAC dashboard features — v2

## Architecture

```
SDKs (Python, TypeScript, .NET)
        │  QUIC + custom binary protocol
        ▼
Go core (single binary)
  ├── Protocol layer (QUIC server)
  ├── Scheduler (cron, delayed)
  ├── Worker registry (heartbeats, leases)
  └── Raft coordination (HA)
        │
        ▼
Storage trait (atomic enqueue, leases, notify, history)
        │
        ▼
File-based engine (WAL + memory index, no DB)
```

Storage is exposed via a Go interface. The default and only v1 backend is the file-based engine. An in-memory test backend exists to validate the interface doesn't leak file-specific assumptions. Postgres/Redis/SQLite adapters are post-v1.

## Locked design decisions

### Storage: custom WAL + memory index, file-based, no DB

State (jobs, queues, schedules) lives in memory. Every state-changing operation serializes a record, appends to a WAL file, fsyncs, then updates memory and acks. A background compactor periodically writes a snapshot and truncates the WAL. On crash, load the latest snapshot and replay WAL forward.

Directory layout:

```
data/
  wal/
    000001.log
    000002.log
  snapshots/
    snap-00042.bin
  meta/
    config.json
```

**Rationale:** Depth-maxed learning (real WAL/recovery/group-commit work), zero database dependency for ops simplicity, sharp differentiation from competitors that require Postgres or Redis. Backup is `cp -r data/`.

### Concurrency: single-writer per queue, sharded

One writer goroutine per queue, lock-free between queues. Same pattern BullMQ uses. Sufficient for the throughput target; vertical-scale path is sharding more queues. Multi-node scale comes via Raft clustering.

### Durability modes (per-queue, user-configurable)

- `sync` — fsync per write, strictest, slowest
- `group` — group-commit batches concurrent writes into one fsync (**default**), best throughput with near-strict durability
- `async` — timer-based fsync, fastest, accepts seconds of data loss on crash (for low-stakes jobs)

### HA: `hashicorp/raft`, not rolled from scratch

`hashicorp/raft` for consensus — battle-tested in Consul, Vault, and Nomad, with a clean `LogStore` / `FSM` abstraction that maps directly onto our WAL. (`etcd/raft` is the more flexible alternative; revisit if hashicorp's API constrains us.) Integrating teaches Raft thoroughly without blowing up timeline. Rolling from scratch is a separate 6+ month project — explicitly out of scope.

### Transport: QUIC via `quic-go` + custom application protocol

QUIC handles multiplexing, connection migration, 0-RTT, and congestion control. The custom application protocol on top — length-prefixed binary frames, request/response over stream IDs, server push for work notifications, ack-based reliability — is the design work where wire-protocol learning lands. Deliberately easy to implement in C++ later (no gRPC dependency mess).

### Job schemas: proto3 DSL with codegen

Jobs declared in `.proto` files. Codegen produces idiomatic types in each SDK. Prevents type drift across SDKs and gives compile-time payload validation — a known weak spot for Temporal.

### SDK ergonomics (idiomatic, not translated)

- **Python:** async + sync APIs, `@job` decorator, asyncio-native, optional Pydantic integration
- **TypeScript:** Node + Bun support, strong type inference for job payloads, decorator-based registration
- **.NET:** attribute-based registration (`[Job]`), DI integration, .NET 8+, target Hangfire users for migration

### Dashboard: SPA bundled into binary, SSE for live updates

Single-binary deploy. No separate dashboard server. Live updates via Server-Sent Events (well-trodden territory).

## Roadmap (12 months, ~15 hrs/week)

### Months 1–3 — Foundation

- Storage trait paper-design (~2 weeks before any code)
- Custom WAL + memory index, single-node
- Job state machine: enqueued → reserved → running → succeeded/failed/retrying/dead
- Group-commit implementation
- Crash recovery, snapshot, compaction loop
- In-memory test backend (validates the trait)

### Months 4–6 — Protocol + Python SDK + Dashboard

- QUIC server via `quic-go`
- Custom binary application protocol
- Schema codegen pipeline (proto3 → Python types)
- Python SDK: `@job` decorator, sync + async APIs
- Dashboard SPA + SSE telemetry
- First end-to-end use case running

### Months 7–9 — TypeScript SDK + Raft + Benchmarks

- TypeScript SDK (Node + Bun)
- `hashicorp/raft` integration, multi-node clustering
- Jepsen-style fault injection testing
- Cron + delayed scheduling
- First public benchmarks vs Hatchet/Sidekiq/BullMQ (honest methodology, published)

### Months 10–12 — .NET SDK + Polish + Launch

- .NET SDK with `[Job]` attribute + DI
- OpenTelemetry tracing across job chains
- Production hardening, deployment guides, code examples
- Docs site
- Launch post

### Post-v1

- Months 13–15: Go SDK + Postgres backend adapter (v1.1)
- Months 16–18: Redis backend (v1.2)
- Months 18–20: C++ SDK (v1.2, fintech push)

## Tech stack

- **Core language:** Go (latest stable), goroutines + channels for concurrency
- **Transport:** `quic-go` (QUIC) + custom binary framing
- **Consensus:** `hashicorp/raft`
- **Storage:** custom WAL, no external DB
- **Schemas:** proto3 with `google.golang.org/protobuf` for Go; per-SDK codegen
- **Observability:** `log/slog` + OpenTelemetry (`go.opentelemetry.io/otel`)
- **Dashboard:** React + Vite, bundled via Go's built-in `embed`
- **Build:** `go build`, cross-compile via `GOOS`/`GOARCH` for linux/darwin/windows on amd64 and arm64

## Repository layout

Monorepo:

```
/
├── core/              # Go binary
├── sdks/
│   ├── python/
│   ├── typescript/
│   └── dotnet/
├── dashboard/         # React + Vite SPA
├── schemas/           # proto3 schema definitions + codegen tooling
├── benchmarks/
├── docs/
└── examples/
```

## Conventions

- **License:** Apache 2.0
- **Versioning:** SemVer; pre-1.0, breaking changes expected
- **Commits:** Conventional Commits
- **Branching:** trunk-based, short-lived feature branches

## Constraints + context

- **Solo developer**, 10–20 hrs/week, 12-month v1 target
- **Background:** senior fullstack (TypeScript, .NET, Python, Azure, AI integration). Go is a deliberate learning goal but a much gentler ramp than Rust — depth comes from the systems work (WAL, Raft, QUIC, wire protocol), not from fighting the language.
- **Supporting tech being lifted, not deeply learned:** DevOps/IaC, security/cryptography, mobile, data engineering. These appear in the project but aren't the core focus.

## Naming + identity (locked)

Bare `spitfire` is taken on npm (abandoned), PyPI (abandoned), and crates.io (active) — so we ship under a branded prefix, same pattern Temporal (`@temporalio/...`, `temporalio`) and Hatchet (`@hatchet-dev/...`, `hatchet-sdk`) use.

| Surface | Name | Status |
|---|---|---|
| GitHub org | `spitfirehq` | ✅ claimed |
| Domain | `spitfire.sh` | planned — purchase after first milestone |
| Go module | `github.com/spitfirehq/spitfire` | follows from org |
| npm SDK | `@spitfirehq/sdk` (scope `@spitfirehq` free) | reserve before publishing |
| PyPI SDK | `spitfire-client` (free) | reserve at first publish |
| NuGet | `Spitfire` (bare is free) + `Spitfire.Client` | reserve before publishing |

## Immediate next decisions

1. **Go SDK position** — with the core in Go, the Go SDK is nearly free. Consider promoting it from v1.1 into v1 (so launch covers Python, TypeScript, .NET, Go).
2. **Storage interface (Go)** — paper-design before any Go code is written. Must support: atomic enqueue with idempotency, claim-next-ready-from-queue with visibility timeout, lease renewal, terminal state writes, history append, range scans for dashboard, pub/sub-style notify
3. **WAL binary format + record layout** — define record types, framing, magic bytes, version field, checksum strategy
4. **Crash-recovery algorithm** — corner cases: torn writes, partial snapshots, version mismatches, WAL/snapshot ordering
5. **Repo bootstrap + CI** — Go module layout under `github.com/spitfirehq/spitfire`, `golangci-lint`, test workflow
