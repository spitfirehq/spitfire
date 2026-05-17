# Rust Primer for Polyqueue

A working primer for a senior fullstack dev (TypeScript, .NET, Python) learning Rust deliberately to build the polyqueue core. Not a tutorial — a map of what's different, what bites, and what to read.

Calibrated to what you'll actually hit: WAL storage, async I/O with `tokio`, QUIC via `quinn`, consensus via `openraft`, no-DB systems work. Skim it once; revisit sections when the compiler is yelling at you.

---

## 1. Mental model shifts coming from C#/TS/Python

| Coming from | Rust forces you to make explicit |
|---|---|
| GC managing memory | Who owns this value, who borrows it, and for how long |
| `null` / `undefined` / nullable refs | `Option<T>` — there is no null |
| Exceptions | `Result<T, E>` — errors are values, propagated with `?` |
| Interfaces with reference equality | Traits with no inherent identity; "fat pointers" for dynamic dispatch |
| `async/await` over a managed runtime | `async/await` over a *user-chosen* runtime (you pick `tokio`); futures are inert until polled |
| Method overloading | Doesn't exist — use traits or different function names |
| Inheritance | Doesn't exist — composition + traits |
| `try/catch/finally` cleanup | RAII via `Drop` trait — cleanup is deterministic at scope exit |
| Reflection / dynamic types | Mostly absent — design for compile-time generics, not runtime polymorphism |

The biggest shift: **the compiler is a design partner, not an obstacle.** Most of what feels like fighting the borrow checker is the compiler refusing to let you write a design that wouldn't have worked anyway — you just used to find out at runtime in production.

---

## 2. Ownership, borrowing, lifetimes — the part that hurts first

Three rules:

1. Every value has exactly one **owner**.
2. When the owner goes out of scope, the value is dropped.
3. You can have either **one mutable borrow** OR **any number of immutable borrows** to a value at a time. Never both.

```rust
let s = String::from("hello");    // s owns the String
let t = s;                         // ownership MOVED to t. s is now invalid.
println!("{}", s);                 // compile error: borrow of moved value

let s = String::from("hello");
let t = s.clone();                 // explicit deep copy — both valid
```

Borrowing instead of moving:

```rust
fn len(s: &String) -> usize { s.len() }    // borrows immutably
fn push(s: &mut String) { s.push('!'); }   // borrows mutably

let mut s = String::from("hi");
let r1 = &s;       // OK: immutable borrow
let r2 = &s;       // OK: another immutable borrow
let r3 = &mut s;   // ERROR: can't take mut borrow while immutable borrows exist
```

**Lifetimes** name *how long* a borrow is valid. Most are inferred. You write them explicitly when the compiler can't figure out which input lifetime an output borrow ties to:

```rust
fn longest<'a>(x: &'a str, y: &'a str) -> &'a str {
    if x.len() > y.len() { x } else { y }
}
```

Read `'a` as "some lifetime called a." The function says: the returned reference lives as long as the shorter of `x` and `y`.

**Pragmatic guidance:** when starting out, prefer owned values (`String`, `Vec<T>`) over references in struct fields. Adding `&'a str` to a struct field means every consumer of that struct also carries the lifetime. Save lifetime annotations for genuinely zero-copy hot paths.

**The escape hatches:**
- `Rc<T>` — single-threaded shared ownership, refcounted
- `Arc<T>` — multi-threaded shared ownership, atomic refcount
- `RefCell<T>` — runtime-checked mutable borrow inside an immutable owner (single-threaded)
- `Mutex<T>` / `RwLock<T>` — thread-safe interior mutability
- `Cell<T>` — copy in / copy out for small `Copy` types

In polyqueue you'll mostly see `Arc<Mutex<T>>` or `Arc<RwLock<T>>` for shared state across async tasks.

---

## 3. Error handling

No exceptions. Everything fallible returns `Result<T, E>`:

```rust
fn read_config(path: &Path) -> Result<Config, ConfigError> { ... }

let cfg = match read_config(&path) {
    Ok(c) => c,
    Err(e) => return Err(e.into()),
};

// Or, idiomatic:
let cfg = read_config(&path)?;   // ? propagates Err, unwraps Ok
```

The `?` operator: "if Err, return early converting via `From`; if Ok, unwrap." This is the single most important ergonomic feature for systems code.

Two crates you will use constantly:
- **`thiserror`** — derive macro for *library* error types (concrete, named variants)
- **`anyhow`** — type-erased error for *application* code where you just want "anything that went wrong"

Pattern: storage and protocol layers expose `thiserror`-derived errors; the binary's `main()` returns `anyhow::Result<()>`.

```rust
#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("WAL fsync failed: {0}")]
    Fsync(#[from] io::Error),
    #[error("checksum mismatch at offset {offset}")]
    Checksum { offset: u64 },
}
```

Don't `.unwrap()` in production code paths. Use it in tests, prototypes, and "this is genuinely impossible" cases — then prefer `.expect("clear message about why")`.

---

## 4. Traits — Rust's polymorphism

Traits are like interfaces but more powerful:

```rust
pub trait Storage: Send + Sync + 'static {
    async fn enqueue(&self, queue: &str, payload: &[u8]) -> Result<JobId, StorageError>;
    async fn claim_next(&self, queue: &str) -> Result<Option<Lease>, StorageError>;
}
```

Two ways to use a trait:

**Generic / static dispatch** (zero-cost, monomorphized):
```rust
fn run<S: Storage>(storage: S) { ... }
```

**Trait object / dynamic dispatch** (`dyn`, runtime vtable):
```rust
fn run(storage: Arc<dyn Storage>) { ... }
```

Default to generics. Use `dyn` when you need heterogeneous collections or to hide concrete types behind an API boundary (e.g., the storage trait in polyqueue, where the binary picks an implementation at startup).

**`Send` and `Sync`** are auto-traits the compiler infers:
- `Send` — safe to *move* between threads
- `Sync` — safe to *share* between threads (i.e., `&T` is `Send`)

For async code you'll see `Send + Sync + 'static` bounds *everywhere*. They mean "this thing can live on any executor thread." Learning when these bounds are required and how to satisfy them is the single most common async Rust pain point.

---

## 5. Async Rust — the part that *also* hurts

Rust async is different from C#/TS in three load-bearing ways:

**1. Futures are inert.** `async fn foo()` returns a `Future`. Nothing executes until you `.await` it or hand it to an executor (`tokio::spawn`).

**2. You pick the runtime.** There is no built-in event loop. `tokio` is the de facto choice for servers. You'll add `#[tokio::main]` to `main()`:

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:7233").await?;
    loop {
        let (sock, _) = listener.accept().await?;
        tokio::spawn(handle(sock));
    }
}
```

**3. `Send` bounds and lifetimes get harder.** If a future *holds a reference across an `.await` point*, that reference must be valid for the whole future's lifetime. This is why you'll see lots of `Arc<T>` rather than `&T` in async code — it sidesteps the lifetime problem.

**Things that will bite:**
- Holding a `MutexGuard` across `.await` — the future stops being `Send`. Drop the guard before awaiting, or use `tokio::sync::Mutex` (slower but async-aware).
- Recursive async functions — need `Box::pin(async move { ... })` because the future's size can't be known.
- `&self` async methods captured in `tokio::spawn` — usually means you need `Arc<Self>` instead.

**Tokio's primitives you'll actually use:**
- `tokio::spawn` — spawn a task on the runtime
- `tokio::select!` — race multiple futures, first to complete wins
- `tokio::sync::mpsc` / `oneshot` / `broadcast` — async channels
- `tokio::sync::RwLock` / `Mutex` — async-aware locks
- `tokio::time::sleep` / `interval` — timers
- `tokio::io::AsyncReadExt` / `AsyncWriteExt` — async I/O traits

**Mental model:** treat `tokio::spawn` like `Task.Run` in C# or `Promise` chains in TS, but with explicit lifetime/Send constraints. Channels (`mpsc`) replace shared mutable state in most cases — actor-style design works extremely well in Rust async.

---

## 6. Patterns you'll use constantly in polyqueue

**Builder pattern with `#[derive]` macros:**
```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config { ... }
```

**Newtypes for type safety:**
```rust
pub struct JobId(pub u64);
pub struct QueueName(String);
// Now the compiler stops you from passing a queue name where a job ID is expected
```

**Typed state machines via enums + match:**
```rust
pub enum JobState {
    Enqueued { at: Instant },
    Reserved { worker: WorkerId, lease_until: Instant },
    Running { started_at: Instant },
    Succeeded,
    Failed { reason: String, attempts: u32 },
    Dead,
}
```
Exhaustive `match` means adding a variant forces you to update every consumer. This is *huge* for evolving the job state machine in README.md:152 without losing edge cases.

**Channels over shared state:**
```rust
let (tx, mut rx) = mpsc::channel::<WalRecord>(1024);
tokio::spawn(async move {
    while let Some(record) = rx.recv().await {
        wal.append(record).await?;
    }
});
```
This is the single-writer-per-queue pattern from README.md:114. One task owns the WAL writer; everyone else sends records into the channel.

**`Drop` for cleanup:**
```rust
impl Drop for Lease {
    fn drop(&mut self) {
        // automatically release lease when Lease goes out of scope
    }
}
```
This is RAII. Use it for leases, file locks, connection pools — anything that needs deterministic cleanup.

---

## 7. Tooling — the unsung Rust win

| Tool | Use |
|---|---|
| `cargo` | Build, test, run, dependency management. Lives at the workspace root |
| `cargo check` | Type-check without compiling — fast feedback loop |
| `cargo clippy` | Linter; takes idiomatic Rust style seriously. **Run it constantly** |
| `cargo fmt` | Formatter; configure once, never argue about style again |
| `cargo test` | Built-in test runner. Tests live in `#[cfg(test)] mod tests` blocks alongside code |
| `cargo bench` | Microbenchmarks (with `criterion` crate for serious work) |
| `cargo doc --open` | Generates and opens HTML docs for your crate + all deps |
| `rust-analyzer` | LSP for VS Code / RustRover / Helix. Best-in-class IDE support. Install on day one |
| `cargo expand` | Shows what `#[derive]` and macros expand to. Essential when debugging macro magic |
| `cargo udeps` | Finds unused dependencies |
| `cargo audit` | Security advisories on your dep tree |
| `cargo flamegraph` | Flamegraph profiling for hot paths |

**Workspace layout** you'll want for polyqueue:
```
core/
  Cargo.toml        # workspace root
  crates/
    polyqueue-wal/
    polyqueue-proto/
    polyqueue-net/
    polyqueue-raft/
    polyqueue-server/
```
Splitting into crates speeds compile times and forces module boundaries.

---

## 8. Crate map for polyqueue specifically

| Concern | Crate | Notes |
|---|---|---|
| Async runtime | `tokio` | The choice for servers |
| QUIC | `quinn` | Confirmed in README.md:128 |
| TLS | `rustls` | Used by `quinn` under the hood |
| Consensus | `openraft` | Confirmed in README.md:125 |
| Proto3 | `prost`, `prost-build` | Confirmed in README.md:193 |
| Serialization (non-proto) | `serde`, `bincode`, `rmp-serde` | For internal formats |
| Logging | `tracing`, `tracing-subscriber` | Confirmed in README.md:194 |
| OpenTelemetry | `opentelemetry`, `tracing-opentelemetry` | For README.md:59 |
| Error handling | `thiserror`, `anyhow` | Library vs binary |
| Configuration | `figment` or `config` | Layered config from files/env |
| CLI | `clap` (derive feature) | For the binary's argument parsing |
| Time | `std::time::Instant` for monotonic, `time` or `chrono` for wall-clock | |
| UUIDs | `uuid` | For job IDs (consider monotonic IDs instead) |
| Embed dashboard | `rust-embed` | Confirmed in README.md:195 |
| Atomic / lock-free | `parking_lot`, `crossbeam`, `dashmap` | Faster alternatives to std primitives |
| Property tests | `proptest` or `quickcheck` | Essential for WAL / Raft correctness |
| Fault injection | `madsim` or custom seam | For deterministic simulation testing |
| Benchmarks | `criterion` | Statistically sound microbenchmarks |

**Picking dependencies:** prefer maintained, widely-used crates. Check `crates.io` for download counts, last commit date, open issues. The Rust ecosystem has a long tail of abandoned crates — `cargo audit` and `cargo outdated` help.

---

## 9. Things that will surprise you

- **No null. No exceptions.** Everything is `Option<T>` or `Result<T, E>`. You'll miss them for about a week, then never again.
- **`String` vs `&str`.** Owned vs borrowed. Function parameters almost always want `&str`. Struct fields almost always want `String`. Concatenation is annoying — use `format!()` or write into a buffer.
- **`Vec<T>` vs `&[T]`.** Same owned/borrowed split. APIs take `&[T]`; storage uses `Vec<T>`.
- **Numeric types are explicit.** `i32`, `u64`, `usize`, `f64`. No implicit conversion. `as` for cast.
- **`PartialEq`/`Eq` and `PartialOrd`/`Ord` distinction** — floats are `PartialOrd` only (NaN). Most other things are `Ord`.
- **Macros vs functions.** `println!`, `vec!`, `format!` — the `!` means macro. They can do things functions can't (variadic args, compile-time work).
- **Closures have three flavors.** `Fn`, `FnMut`, `FnOnce` — by which capture mode they use. Move semantics with `move ||` closures are essential for `tokio::spawn`.
- **`?` doesn't work in `main()` unless the return type is `Result<_, _>`.** Use `anyhow::Result<()>`.
- **Module system is *not* path-based the way C# namespaces are.** `mod foo;` declares a module backed by `foo.rs` or `foo/mod.rs`. `use` brings names into scope. Read about modules once and refer back.

---

## 10. Resources

### Read first (in order)

1. **The Rust Book** — *the* canonical free book. https://doc.rust-lang.org/book/
   Read chapters 1-10 cover-to-cover. Chapters 13 (closures, iterators), 15 (smart pointers), 16 (concurrency), 19 (advanced features) — read when needed.

2. **Rust by Example** — runnable snippets paired with the book. https://doc.rust-lang.org/rust-by-example/

3. **Rustlings** — small compile-and-fix exercises. https://github.com/rust-lang/rustlings
   Do all of them before writing real polyqueue code. ~10-15 hours total; pays back immediately.

### Read in the first 1-2 months

4. **Tokio Tutorial** — best async Rust introduction. https://tokio.rs/tokio/tutorial

5. **Async Book** — the model under tokio. Drier but essential. https://rust-lang.github.io/async-book/

6. **The Rustonomicon** — unsafe Rust, layout, FFI. Skim early, return when you need it. https://doc.rust-lang.org/nomicon/

### Read by month 3-4

7. **Programming Rust, 2nd edition** (Blandy, Orendorff, Tindall — O'Reilly). The best paid Rust book by a wide margin. Better systems-programming coverage than the official book.

8. **Rust for Rustaceans** (Jon Gjengset — No Starch). Intermediate-to-advanced. Read after ~3 months of writing Rust. Chapters on `Pin`, `Send`/`Sync`, async internals are gold.

9. **Zero to Production in Rust** (Luca Palmieri). Builds a real web service. Good for tooling/observability/testing patterns even though it's web-focused.

### Reference / lookup

- **`std` docs** — https://doc.rust-lang.org/std/ — the standard library docs are exemplary. Use them constantly.
- **docs.rs** — every crate's docs auto-published. Bookmark `docs.rs/tokio`, `docs.rs/quinn`, `docs.rs/openraft`.
- **Rust API Guidelines** — https://rust-lang.github.io/api-guidelines/ — how to design idiomatic Rust APIs. Read before designing the storage trait.
- **Rust Reference** — language spec, dense. Reference only.

### Talks worth watching

- **"Crust of Rust" series** — Jon Gjengset on YouTube. Live-codes core abstractions (`Vec`, channels, lifetimes, async). 1-2 hour deep dives. Best Rust content on the internet.
- **"The What and How of Futures and async/await"** — Jon Gjengset. Demystifies futures.
- **"Decrusting the tokio crate"** — Jon Gjengset. Read tokio's source with him.
- **Tyler Mandry (Google) on async Rust internals** — various conference talks.

### Specifically for polyqueue's domain

- **`openraft` book** — https://databendlabs.github.io/openraft/ — read in full before integration in month 7+.
- **`quinn` examples** — https://github.com/quinn-rs/quinn/tree/main/quinn/examples — bookmark.
- **TigerBeetle's design docs and talks** — different language but same philosophy of durable systems work. Joran Dirk Greef's talks are worth watching.
- **Phil Eaton's blog** (https://eatonphil.com) — practical posts on WAL design, B-trees, MVCC. Language-agnostic but Rust-friendly.
- **"Files are hard"** by Dan Luu — read once before writing fsync code. https://danluu.com/file-consistency/
- **"Crash consistency"** (Pillai et al, OSDI '14) — the foundational paper on getting fsync right. Read in month 1.
- **Raft paper** (Ongaro & Ousterhout) — read it twice. https://raft.github.io/raft.pdf
- **TLA+ for Raft** — once you've read the paper. Optional but worth knowing it exists.

### Communities

- **Rust Users Forum** — https://users.rust-lang.org/ — friendly, high signal
- **Rust subreddit** — r/rust — news + occasional deep posts
- **`#rust-lang` on Discord (Rust Community server)** — fast help on small questions
- **`tokio` Discord** — `tokio` and async ecosystem help

### Avoid

- Tutorials older than 2-3 years — async Rust evolved fast; outdated examples cause more confusion than they save
- "Learn Rust in X hours" content — Rust does not compress; pick the slower canonical resources instead
- Anything labeled "beginner Rust" that doesn't have you fighting the borrow checker by lesson 3 — that's where the actual learning happens

---

## 10b. Companion materials that compress the timeline

The Rust Book is the spine, but reading it alone is the slow path. Three companions used *alongside* (not after) the book turn ~80 hours of reading into ~50 hours of working knowledge:

- **Rustlings** (https://github.com/rust-lang/rustlings) — ~12-15 hours of small compile-and-fix exercises. **Do them alongside the matching book chapters, not after.** Each exercise targets one concept; the compile-fail-fix loop builds borrow-check intuition faster than passive reading. Skipping Rustlings doesn't save time — it just defers the pain to when you're writing real code under deadline pressure.

- **Rust by Example** (https://doc.rust-lang.org/rust-by-example/) — same material as the Book, different framing. Use it when a Book section doesn't click: read the parallel RBE chapter and the second angle often catches what the first missed. Cheaper than Stack Overflow and more reliable than blog posts.

- **`cargo` + `rust-analyzer` from day one, before Chapter 2.** The IDE feedback loop is part of how you learn the type system. Watching `rust-analyzer` inline the inferred type as you write is one of the fastest ways to internalize how the compiler "sees" your code. Configure `cargo clippy` to run on save — clippy is a free teacher pointing out idiomatic alternatives.

### Things that slow people down

Equally important — the anti-patterns that stretch the timeline:

- **Copy-pasting code samples instead of typing them.** Muscle memory matters more than you'd expect for borrow-check intuition. Type every example by hand.
- **Trying to memorize syntax instead of building intuition.** Rust syntax is dense; let `rust-analyzer` autocomplete it while you focus on the concepts.
- **Reading Chapters 4 (ownership) and 10 (generics/traits/lifetimes) once.** Almost nobody gets these on first pass. Plan two reads minimum.
- **Treating the book as breadth-first homework.** It's a reference. After the first linear pass through ch 1-10, go depth-first into whatever's blocking your real code.
- **Skipping the project chapters (12 and 20).** They're where everything integrates. Type every line.
- **Defaulting to `.unwrap()` and `.clone()` to silence the compiler.** Both work to make code compile, neither teaches you anything. When the compiler complains, *understand why* before reaching for an escape hatch.

The realistic calendar at 15 hrs/week, with companions used correctly: **6-10 weeks for the book**, then phase out to using it as reference while polyqueue prototyping carries the rest of the learning.

---

## 11. A learning sequence calibrated to polyqueue

| Month | What to learn | What to build |
|---|---|---|
| 1 | Rust Book ch 1-10, Rustlings, ownership/borrowing/lifetimes | Toy WAL: append-only file with length-prefixed records, replay on startup |
| 2 | Rust Book ch 11-16, traits in depth, error handling | Storage trait paper-design (README.md:232). Implement in-memory backend |
| 3 | Tokio tutorial, async/await mechanics | Group-commit WAL with `tokio` + `mpsc` channel for writer task |
| 4 | `serde`, `prost`, build scripts, workspaces | Proto3 schema + codegen pipeline. Internal record serialization |
| 5 | `quinn`, QUIC mental model | Echo server over QUIC. Then framed binary protocol on top |
| 6 | `tracing`, observability, testing patterns | First end-to-end enqueue path. Python SDK as a client to validate |

By month 6 you should be writing Rust without constant doc lookups. By month 12 you should be reasoning about lifetimes and `Send`/`Sync` bounds without thinking about them explicitly — they become intuition.

---

## 12. Failure modes to watch for in yourself

- **Re-architecting prematurely** because a borrow check failed. Usually the right fix is local (`.clone()`, `Arc<T>`, restructure a function) — not a whole-design rethink.
- **Reaching for `unsafe`** before exhausting safe options. In a year of polyqueue work, you should write `unsafe` maybe 2-3 times, each in a tightly-scoped audited block (e.g., `mmap` wrappers).
- **Over-abstracting traits early.** Write the concrete file-backed implementation first, then extract the trait once you see the shape.
- **Fighting async lifetimes by piling on `'static` bounds.** Usually the right move is `Arc<T>`, not `&'static T`.
- **Premature optimization with lock-free / unsafe.** `parking_lot::Mutex` is fast enough until proven otherwise.
- **Reading 10 blog posts instead of writing 100 lines.** The borrow checker is a teacher. Sit with it.

The hard part of learning Rust is not the syntax; it's that the language forces you to think about ownership, aliasing, and lifetimes *up front* that other languages let you ignore until they crash in production. That's the trade. Polyqueue is the project where that trade pays off.
