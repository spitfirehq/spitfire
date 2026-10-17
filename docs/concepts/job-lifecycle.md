# Job lifecycle

State machine for every job in Spitfire. Every storage transition and history
event maps to one of these edges.

```
                    enqueue
                       │
                       ▼
                 ┌──────────┐
        ┌───────►│ pending  │◄──────────────┐
        │        └────┬─────┘               │
        │             │ claim               │ Fail(canRetry=true)
        │             ▼                     │ and attempt < max
        │        ┌──────────┐               │
        │        │ leased   │───────────────┘
        │        └──┬───┬───┘
        │           │   │
        │ Complete  │   │ Fail(canRetry=false)
        │           │   │   or (canRetry=true and attempt >= max)
        │           ▼   ▼
        │    ┌──────────┐  ┌──────────┐
        └─── │succeeded │  │  failed  │   (terminal, manual requeue OK)
   Requeue   └──────────┘  └──────────┘
   (admin)         or
                    ▼
               ┌──────────┐
               │   dead   │   (DLQ; Requeue returns to pending)
               └──────────┘
```

## States

| State | Meaning | Terminal? |
|---|---|---|
| `pending` | Waiting to be claimed. Includes delayed jobs before `scheduled_at`. | no |
| `leased` | Claimed by a worker; lease deadline in `leased_until`. | no |
| `succeeded` | Completed successfully. | yes |
| `failed` | Failed permanently (non-retryable or manual fail). | yes |
| `dead` | Dead-letter: retries exhausted or explicitly dead-lettered. | yes |

## Transitions

| Edge | Trigger | Notes |
|---|---|---|
| → `pending` | `Enqueue` / `Requeue` / `Fail(retry)` | `Fail(retry)` sets `scheduled_at` to now+backoff and increments `attempt` on the *next* claim. |
| `pending` → `leased` | `Claim` | Atomic. `attempt++`. Sets `lease_owner`, `leased_until`. |
| `leased` → `leased` | `RenewLease` | Extends `leased_until`. Owner must match. |
| `leased` → `leased` | lease expiry + reclaim | Another `Claim` may take the job after `leased_until`. Emits `lease_expired`. |
| `leased` → `succeeded` | `Complete` | Owner must match. |
| `leased` → `pending` | `Fail(canRetry=true, attempt < max)` | Backoff before re-claim. |
| `leased` → `failed` | `Fail(canRetry=false)` | Terminal. |
| `leased` → `dead` | `Fail(canRetry=true, attempt >= max)` | DLQ. |
| terminal → `pending` | `Requeue` | Manual retry / DLQ reprocess. Not allowed while `leased`. |

## Invariants

1. **At-least-once.** A job may run more than once (lease expiry, crash after work before `Complete`). Handlers must be idempotent.
2. **Claim exclusivity.** A given `pending` job is never handed to two workers at the same time. After lease expiry, it may be claimed again.
3. **Owner check.** `RenewLease`, `Complete`, and `Fail` require the current `lease_owner`.
4. **Idempotent enqueue.** `(queue, idempotency_key)` is unique when set; duplicate enqueue returns the existing job.
5. **History is append-only.** Every transition appends a `HistoryEvent`.

## Attempt counting

`attempt` is incremented on `Claim` (so a claimed-then-expired job still consumed an attempt). `max_attempts` is the number of claims allowed before dead-lettering on retryable failures.
