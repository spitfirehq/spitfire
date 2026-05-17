# Go Primer for Spitfire

Calibrated for someone with deep TypeScript / .NET / Python experience, no
prior systems-programming or Go background, building a durable WAL-backed,
Raft-replicated, QUIC-served job queue over ~12 months part-time.

The good news: Go's language curve is much shallower than Rust's. You'll be
writing useful code in week one. The depth in this project comes from the
**systems work** (WAL design, crash recovery, Raft, QUIC, wire protocol) and
that's where your hours should go. Treat Go itself as a vehicle to learn well
enough that it gets out of your way, not as the primary subject of study.

---

## 1. Mental model shifts coming from C# / TS / Python

- **No classes, no inheritance.** Composition via struct embedding. Polymorphism via interfaces. If you reach for an abstract base class, stop and rethink as an interface.
- **Interfaces are satisfied structurally.** No `implements` keyword. If a type has the right methods, it *is* the interface. This makes mocking trivial and dependency inversion natural.
- **Zero values are first-class.** A `struct{}` with no fields initialized is valid and usable. `var b bytes.Buffer` works without `new()`. Design types so the zero value is meaningful — this is idiomatic Go.
- **Errors are values, not exceptions.** Every function that can fail returns `(T, error)`. No `try`/`catch`. You will type `if err != nil { return err }` thousands of times. Embrace it.
- **No generics for a long time, now they exist (since 1.18).** Use them sparingly. Most Go code does not need them. Interfaces still cover ~90% of polymorphism needs.
- **The build system is the language.** `go.mod` is the manifest, `go build`/`go test`/`go vet` are universal. No npm-vs-pnpm-vs-yarn debates. No `tsconfig` zoo.
- **There is one formatter.** `gofmt` is non-negotiable; nobody debates style. Run it on save.
- **Concurrency is in the language, not a library.** Goroutines and channels are first-class. `async`/`await` doesn't exist — every function call is "blocking" from the caller's perspective, but the runtime multiplexes goroutines onto OS threads for you.

---

## 2. Goroutines, channels, context — the concurrency primitives

This is the part you'll use every day, and the part where bugs hide.

### Goroutines

`go someFunc()` spawns a goroutine. Cheap: ~2KB initial stack, millions per process. *Never* spawn a goroutine without knowing how it will exit. Leaked goroutines are the #1 Go memory bug.

```go
go worker(ctx)  // goroutine respects ctx.Done() to exit cleanly
```

### Channels

Typed pipes between goroutines. `make(chan T)` is unbuffered (synchronous handoff). `make(chan T, N)` is buffered.

- **Send on closed channel panics.** Only the sender should close.
- **Receive on closed channel returns zero value immediately.** Use `v, ok := <-ch` to detect close.
- **`select`** is `switch` for channels — first ready case wins, or `default` for non-blocking.

```go
select {
case job := <-queue:
    process(job)
case <-ctx.Done():
    return ctx.Err()
}
```

### `context.Context`

The standard way to thread cancellation, deadlines, and request-scoped values through a call stack. **Every long-running function takes `ctx context.Context` as its first parameter.** Cancel propagates downward — when a parent cancels, all children unblock from `ctx.Done()`.

Rules of thumb:
- Pass `ctx` explicitly. Never store it in a struct. Never pass `nil` — pass `context.Background()` or `context.TODO()`.
- Functions that block must respect `ctx.Done()`.
- `context.WithTimeout` and `context.WithCancel` return a `cancel` func; always `defer cancel()`.

### `sync` package

- `sync.Mutex` / `sync.RWMutex` — when shared state can't be channel-coordinated. Be honest: locks are often the right answer; "don't communicate by sharing memory" is a slogan, not a rule.
- `sync.WaitGroup` — wait for N goroutines to finish.
- `sync.Once` — exactly-once initialization.
- `sync.Pool` — object reuse to reduce GC pressure. You'll use this for byte buffers in the WAL hot path.

### Atomics (`sync/atomic`)

For lock-free counters, sequence numbers, and small flags. The WAL append path probably needs an atomic sequence number — channels and mutexes are overkill for a single integer.

---

## 3. Error handling

The pattern: every fallible function returns `(T, error)`. Callers check.

```go
data, err := os.ReadFile(path)
if err != nil {
    return fmt.Errorf("read %s: %w", path, err)
}
```

Key idioms:
- **`%w` wraps an error.** `errors.Is(err, fs.ErrNotExist)` and `errors.As(err, &target)` peel through wrappers. Use `%w` whenever you want to preserve the cause chain.
- **Sentinel errors.** Package-level `var ErrFoo = errors.New("foo")` for known conditions callers may want to detect.
- **Typed errors.** A struct implementing `Error() string` for errors that carry data (offset, record ID, etc.).
- **`panic` is for programmer errors**, not control flow. Index out of bounds, nil map writes, unrecoverable invariant violations. A library that panics on bad user input is broken.
- **`defer`** runs at function exit. Use it for cleanup: `defer file.Close()`. Note: `defer` has cost in hot loops (~30ns) — fine in normal code, avoid inside tight inner loops.

You will miss exceptions for a week and then never again. Explicit error returns force you to think about every failure mode, which is exactly the discipline a durable system needs.

---

## 4. Interfaces — Go's polymorphism

```go
type Storage interface {
    Append(ctx context.Context, record Record) (offset int64, err error)
    Read(ctx context.Context, offset int64) (Record, error)
    Sync(ctx context.Context) error
}
```

Any type with these three methods *is* a `Storage`. No declaration needed.

Idioms:
- **Keep interfaces small.** `io.Reader`, `io.Writer`, `io.Closer` are one method each. Composition (`io.ReadCloser`) builds bigger contracts. Three methods is a lot.
- **Define interfaces on the consumer side**, not where the implementation lives. Your `service` package declares the `Storage` interface it needs; the `wal` package just implements concrete methods.
- **Empty interface `any` / `interface{}`** is the escape hatch. Use sparingly; prefer typed APIs.
- **`error` is just an interface** with one method. Same with `io.Reader`. The pattern is universal.

Common stdlib interfaces you'll see constantly: `io.Reader`, `io.Writer`, `io.ReadWriter`, `io.Closer`, `fmt.Stringer`, `error`, `context.Context`, `sort.Interface`.

---

## 5. The runtime — GC, scheduler, escape analysis

You don't manage memory directly, but for a 10k jobs/sec target you need to *think* about it.

### GC

- Concurrent, tri-color mark-sweep. Pause times typically <1ms for most workloads.
- Tunable via `GOGC` env var (default 100 = run GC when heap doubles).
- **Allocation pressure is your enemy** in hot paths. Profile with `pprof`; reduce with `sync.Pool` and slice reuse.

### Scheduler

- M:N scheduler — millions of goroutines multiplexed onto `GOMAXPROCS` OS threads (default = CPU count).
- Preemptive since 1.14 — tight CPU loops won't starve other goroutines.
- Work-stealing across threads.

### Escape analysis

The compiler decides whether a value lives on the stack (cheap) or the heap (GC pressure). Pointers escape to the heap more often than values. Run `go build -gcflags="-m"` to see decisions.

Heuristic: in hot paths, pass small structs by value, avoid `interface{}`, don't return pointers to local vars when a value works.

### Profiling

- `runtime/pprof` and `net/http/pprof` give CPU, heap, goroutine, mutex, block profiles.
- `go test -bench` + `-benchmem` for microbenchmarks.
- `go test -race` is mandatory — runs against your code with a race detector. Run it in CI on every PR.

---

## 6. Patterns you'll use constantly in Spitfire

### `defer file.Close()` everywhere

Every file open, every lock acquired, every transaction begun — pair with a `defer` to release. This is Go's RAII.

### Functional options

```go
type Option func(*Config)
func WithSyncMode(m SyncMode) Option { return func(c *Config) { c.SyncMode = m } }
NewWAL(path, WithSyncMode(SyncModeGroup), WithBufSize(8192))
```

Cleaner than huge config structs once you have >3 options.

### Table-driven tests

```go
func TestParse(t *testing.T) {
    cases := []struct{ in string; want int; wantErr bool }{
        {"42", 42, false},
        {"abc", 0, true},
    }
    for _, tc := range cases {
        t.Run(tc.in, func(t *testing.T) { /* ... */ })
    }
}
```

Universal Go test idiom. Use `t.Run` for sub-tests so failures point to specific rows.

### `errgroup` for concurrent fan-out

```go
import "golang.org/x/sync/errgroup"
g, ctx := errgroup.WithContext(ctx)
for _, w := range workers {
    w := w
    g.Go(func() error { return w.Run(ctx) })
}
return g.Wait()  // first error cancels all
```

You'll use this for the worker pool, snapshot stream fan-out, Raft message handling.

### `io.Reader` / `io.Writer` everywhere

The WAL writes via `io.Writer`. Reading via `io.Reader`. Compose with `bufio.Writer` for buffering, `gzip.NewWriter` for compression, `hash.Hash` (also a writer!) for checksums. This composability is one of Go's quiet wins.

### `sync.Pool` for hot-path buffers

```go
var bufPool = sync.Pool{New: func() any { return make([]byte, 4096) }}
buf := bufPool.Get().([]byte)
defer bufPool.Put(buf)
```

Cuts allocator pressure dramatically in encode/decode loops.

### `context` propagation

Every public function in a long-running subsystem takes `ctx` first. Pass it down. Check `ctx.Done()` at any blocking site. Never silently drop it.

---

## 7. Tooling — the unsung Go win

- **`go build`, `go test`, `go run`** — universal, no config.
- **`go test -race`** — race detector. Run always in CI.
- **`go test -cover`** — coverage out of the box.
- **`go fmt` / `gofmt`** — no debates.
- **`go vet`** — catches common bugs (misuse of `Printf`, copying mutexes, shadowed variables).
- **`golangci-lint`** — the de facto meta-linter; configure once in `.golangci.yml`, run in CI.
- **`go doc`** — godoc.org lives in your terminal.
- **`go mod tidy`** — keeps `go.mod` and `go.sum` aligned with imports.
- **`pprof`** — CPU/heap/goroutine profiling via `go tool pprof`.
- **`govulncheck`** — official vulnerability scanner; CI gate.
- **`delve` (`dlv`)** — debugger. Works with VSCode and Goland.

Set up `golangci-lint` and `go test -race` in CI on day one.

---

## 8. Library map for Spitfire specifically

### Core

- **`net`, `os`, `io`, `bufio`, `encoding/binary`** — stdlib, all you need for the WAL writer
- **`hash/crc32`** — checksums (CRC32C / Castagnoli polynomial for WAL records)
- **`sync`, `sync/atomic`** — concurrency primitives
- **`context`** — cancellation
- **`log/slog`** (1.21+) — structured logging; do not use the old `log` package

### QUIC + protocol

- **`github.com/quic-go/quic-go`** — the only mature QUIC library in Go. Used in production by Caddy, Cloudflare, others.
- **`google.golang.org/protobuf`** — proto3 codegen and runtime (the modern v2 API; not the old `golang/protobuf`)

### Raft

- **`github.com/hashicorp/raft`** — battle-tested in Consul, Vault, Nomad. Clean `LogStore` / `FSM` separation that maps onto your WAL.
- Fallback: **`go.etcd.io/raft/v3`** — used by etcd and CockroachDB. More flexible, less hand-holding. Revisit if hashicorp's API constrains you.

### Storage helpers (optional)

- **`golang.org/x/sys/unix`** — for `fadvise`, `O_DIRECT`, raw fsync control if you need to go below stdlib `os.File.Sync()`.
- Avoid pre-built KV stores (BoltDB, BadgerDB, Pebble) for the core WAL — you're building this engine on purpose.

### Observability

- **`go.opentelemetry.io/otel`** — OpenTelemetry for tracing
- **`runtime/pprof`** + **`net/http/pprof`** — profiling endpoints baked into the binary

### Dashboard bundling

- **`embed`** (stdlib, 1.16+) — `//go:embed dashboard/dist/*` embeds the React build into the binary. Replaces what `rust-embed` did in the Rust plan.

### Testing & fault injection

- **`github.com/stretchr/testify`** — `require`/`assert` helpers; not strictly needed but widely used
- **`github.com/google/go-cmp`** — deep equality for tests
- **`testing/quick`** — property-based testing stdlib
- For Jepsen-style chaos in month 7-9, you'll likely write fault injection in Go directly or run external tools (jepsen/elle).

---

## 9. Things that will surprise you

- **Nil interfaces vs nil concrete values.** `var p *MyErr; var e error = p; e != nil` is true — `e` holds a typed nil, which is not equal to an untyped nil. Bites everyone once. Fix: return untyped `nil` for the success path, never a typed nil.
- **Slice aliasing.** Slices share underlying arrays. `b := a[:5]` and writes to `b` mutate `a`. `append` may or may not allocate a new array depending on capacity. Be explicit with `copy` when you need independence.
- **Map iteration order is randomized.** Never rely on it. If you need order, sort the keys.
- **Maps are not safe for concurrent writes.** Use `sync.Map` or a regular map + `sync.RWMutex`. Concurrent writes will *panic*, not silently corrupt.
- **`defer` runs at function return, not block end.** A `defer` inside a loop accumulates until the function returns.
- **Capturing loop variables in closures (pre-1.22).** `for _, x := range xs { go f(x) }` — in Go ≤1.21, all goroutines see the same `x`. Fixed in 1.22+. Pin Go ≥1.22 in `go.mod`.
- **Goroutine leaks are silent.** No supervisor will tell you. `runtime.NumGoroutine()` + pprof goroutine profile are your friends. Test with goroutine leak detectors like `go.uber.org/goleak`.
- **`time.After` allocates.** In tight loops use `time.NewTimer` and reuse.
- **Interface method calls have cost.** ~2ns dispatch overhead. Irrelevant outside very hot loops, but worth knowing.
- **No exhaustiveness checking on `switch`.** Add a default that panics with `fmt.Sprintf("unhandled %T", x)` if you want to be loud.

---

## 10. Resources

### Read first (in order)

1. **A Tour of Go** — https://go.dev/tour. One sitting; gets syntax out of the way.
2. **Effective Go** — https://go.dev/doc/effective_go. The official idioms doc. Re-read after a month.
3. **Go Code Review Comments** — https://go.dev/wiki/CodeReviewComments. Short and dense; the canonical style/idiom checklist.
4. **The Go Memory Model** — https://go.dev/ref/mem. Required reading before writing concurrent code with shared state.

### Read in the first month

- **"100 Go Mistakes and How to Avoid Them"** by Teiva Harsanyi — best single book for the practitioner level; covers exactly the gotchas in section 9 above and many more.
- **"Concurrency in Go"** by Katherine Cox-Buday — short, dense, the right level on goroutines/channels/context.
- **Go blog: "Share Memory By Communicating"** — the philosophical posture.

### Read by month 3

- **Russ Cox: "Codebase Refactoring"** and other Go blog posts — long-form thinking from one of the language's principal designers.
- **The standard library source.** Pick `bufio`, `net/http`, `sync` — read them. The stdlib is the canonical example of idiomatic Go.

### Reference / lookup

- **pkg.go.dev** — package docs
- **go.dev/ref/spec** — the language spec; surprisingly readable
- **gobyexample.com** — small, runnable examples for common tasks

### Talks worth watching

- Rob Pike — "Concurrency is not Parallelism"
- Bryan C. Mills — "Rethinking Classical Concurrency Patterns" (GopherCon 2018)
- Filippo Valsorda — anything on Go cryptography / fuzz testing
- Russ Cox — "Go and Versioned Modules" for `go.mod` mental model

### Specifically for Spitfire's domain

- **etcd source code** — the most-read open-source Raft implementation in Go. Even though we use hashicorp/raft, etcd's code is the better teacher.
- **Caddy source** — production QUIC use of quic-go.
- **NATS server source** — high-throughput Go networking, JetStream is a durable streaming layer with WAL-like semantics. Closest cousin to what we're building.
- **CockroachDB Pebble** — a Go LSM, not WAL-only, but exemplary durable-storage Go code.

### Communities

- **Gophers Slack** — invite link on gophers.slack.com
- **r/golang** — usually decent
- **GopherCon talks (YouTube)** — annual signal-rich source

### Avoid

- Tutorials that show you "Go classes" via embedding to mimic inheritance. Embedding is composition; do not pretend it's inheritance.
- Books published before 1.18 if you're learning generics.
- Anything that calls itself "Go for Java/C# developers" without specifically addressing concurrency and error handling — those are the actual mental shifts.

---

## 11. A learning sequence calibrated to Spitfire

**Week 1** — Tour of Go, write a small CLI (e.g., a file-line word counter). Goal: comfortable with syntax, error returns, slices, maps, interfaces.

**Week 2-3** — Effective Go + first goroutine/channel programs. Build a small worker pool. Get `golangci-lint` + `go test -race` working in your dev loop.

**Week 4-6** — Read Cox-Buday concurrency book. Build a toy in-memory queue with multi-producer/multi-consumer. Add `context` cancellation. Hit your first goroutine leak; fix it with pprof.

**Month 2** — Start the real WAL. Practice with `io.Writer`, `bufio`, `encoding/binary`, fsync semantics, CRC checksums. Profile with pprof. This is where Go fluency consolidates.

**Month 3** — Read hashicorp/raft examples + etcd's raft package as a comparison. Start sketching the `LogStore` / `FSM` adapter for your WAL. Read the Go memory model in full.

**Month 4+** — You should now be writing Go without thinking about Go. Focus shifts to systems work: Raft integration, QUIC protocol design, recovery testing, fault injection. The language is no longer in your way.

---

## 12. Failure modes to watch for in yourself

- **Reaching for inheritance.** Go has none. If you're embedding to fake a base class, you're fighting the language. Compose, or pass dependencies in.
- **Over-engineering interfaces.** Don't define an interface until you have two concrete implementations or a clear testing reason. Premature interfaces are a Java habit that doesn't translate.
- **Using channels for everything.** Channels are great for ownership transfer and signaling. They're often *wrong* for shared state — a mutex is simpler and faster. The Go authors say this explicitly; ignore folklore that says otherwise.
- **Ignoring `go test -race`.** It will save you weeks. Run on every PR.
- **Not reading the stdlib.** Coming from .NET/TS/Python, you'll instinctively reach for third-party libraries. Go's stdlib is unusually good. Check there first.
- **Silently dropping errors.** `_ = doThing()` is a code smell. Either handle the error, wrap it, or log it. Make the choice explicit.
- **Goroutine spawning without exit plan.** Every `go f()` needs an answer to "how does this goroutine exit?" If you don't have one, you have a leak.
- **Mistaking Go's simplicity for shallowness.** The language is small; the runtime, scheduler, GC, and concurrency model are deep. The depth you wanted from Rust is here too — just under the surface, not in the type system.
