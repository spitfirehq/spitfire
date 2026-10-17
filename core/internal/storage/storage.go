// Package storage defines the durable job-queue persistence contract.
//
// The interface is intentionally small and is implemented by:
//   - memory: an in-process fake for tests (validates the contract itself)
//   - sqlite: the v1 production backend
//
// Implementations must be safe for concurrent use.
package storage

import (
	"context"
	"errors"
	"time"
)

// JobState is the lifecycle state of a job.
//
//	pending ──claim──► leased ──Complete──► succeeded
//	   ▲                 │
//	   │                 ├──Fail(retry)──► pending   (backoff, attempt++)
//	   │                 └──Fail(final)──► dead      (DLQ)
//	   └── Requeue(succeeded|dead|failed) ──► pending
//
// See docs/concepts/job-lifecycle.md.
type JobState string

const (
	// JobPending is waiting to be claimed (including delayed jobs before their scheduled time).
	JobPending JobState = "pending"
	// JobLeased is claimed by a worker and not yet completed or failed.
	JobLeased JobState = "leased"
	// JobSucceeded is a terminal success.
	JobSucceeded JobState = "succeeded"
	// JobFailed is a terminal failure that was not dead-lettered (manual / non-retryable).
	JobFailed JobState = "failed"
	// JobDead is the dead-letter queue: retries exhausted or explicitly dead-lettered.
	JobDead JobState = "dead"
)

// Terminal reports whether s is a terminal state.
func (s JobState) Terminal() bool {
	return s == JobSucceeded || s == JobFailed || s == JobDead
}

// EventType is a history / notify event kind.
type EventType string

const (
	EventEnqueued  EventType = "enqueued"
	EventClaimed   EventType = "claimed"
	EventRenewed   EventType = "renewed"
	EventCompleted EventType = "completed"
	EventFailed    EventType = "failed"
	EventDeadLettered EventType = "dead_lettered"
	EventRequeued  EventType = "requeued"
	EventExpired   EventType = "lease_expired"
)

// Job is a durable unit of work.
type Job struct {
	ID             string    `json:"id"`
	Queue          string    `json:"queue"`
	State          JobState  `json:"state"`
	Payload        []byte    `json:"payload"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Attempt        int       `json:"attempt"`
	MaxAttempts    int       `json:"max_attempts"`
	Priority       int       `json:"priority"`
	CreatedAt      time.Time `json:"created_at"`
	ScheduledAt    time.Time `json:"scheduled_at"`
	LeasedUntil    time.Time `json:"leased_until,omitempty"`
	LeaseOwner     string    `json:"lease_owner,omitempty"`
	CompletedAt    time.Time `json:"completed_at,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
}

// NewJob is the input to Enqueue.
type NewJob struct {
	Queue          string
	Payload        []byte
	IdempotencyKey string // optional; when set, enqueue is idempotent per key
	MaxAttempts    int    // <=0 means default (3)
	Priority       int    // higher runs first within a queue
	ScheduledAt    time.Time // zero means now
}

// EnqueueResult is the outcome of Enqueue.
type EnqueueResult struct {
	Job       *Job
	Duplicate bool // true when an existing idempotency key was matched
}

// ClaimOptions configures a claim.
type ClaimOptions struct {
	LeaseDuration time.Duration
	LeaseOwner    string
}

// JobQuery is a range scan for the dashboard.
type JobQuery struct {
	Queue   string    // empty = all queues
	State   JobState  // empty = all states
	AfterID string    // exclusive cursor; empty = start
	Limit   int       // <=0 means default (50)
}

// JobPage is one page of a job range scan.
type JobPage struct {
	Jobs     []*Job
	NextID   string // cursor for the next page; empty when exhausted
}

// QueueInfo is dashboard-facing queue metadata.
type QueueInfo struct {
	Name      string `json:"name"`
	Pending   int    `json:"pending"`
	Leased    int    `json:"leased"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	Dead      int    `json:"dead"`
}

// HistoryEvent is an append-only audit record for one job.
type HistoryEvent struct {
	ID        int64     `json:"id"`
	JobID     string    `json:"job_id"`
	Type      EventType `json:"type"`
	Detail    string    `json:"detail,omitempty"`
	At        time.Time `json:"at"`
}

// Event is a storage-level notification (dashboard live updates, worker hints).
type Event struct {
	Type EventType
	Job  *Job
}

// Default values applied by implementations when fields are unset.
const (
	DefaultMaxAttempts   = 3
	DefaultClaimLimit    = 50
	DefaultLeaseDuration = 30 * time.Second
)

// Sentinel errors. Implementations must wrap or return these.
var (
	// ErrNotFound is returned when a job does not exist.
	ErrNotFound = errors.New("storage: not found")
	// ErrNotLeased is returned when a job is not leased, or leased by another owner.
	ErrNotLeased = errors.New("storage: job not leased by this worker")
	// ErrInvalidState is returned on an illegal state transition.
	ErrInvalidState = errors.New("storage: invalid state transition")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("storage: closed")
)

// Storage is the durable queue persistence contract.
//
// Atomicity requirements:
//   - Enqueue must be atomic with its history append and idempotency insert.
//   - Claim must be atomic: the same pending job is never handed to two workers.
//   - Complete / Fail / Requeue must be atomic with history and any retry reschedule.
type Storage interface {
	// Enqueue persists a job in state pending.
	//
	// If NewJob.IdempotencyKey is non-empty and a job with the same
	// (queue, idempotency key) already exists, Enqueue returns that job with
	// Duplicate=true and does not create a second job. Otherwise it inserts
	// and returns Duplicate=false.
	Enqueue(ctx context.Context, job NewJob) (EnqueueResult, error)

	// Claim atomically moves one ready job (state=pending, scheduled_at<=now)
	// to leased and returns it. Ready jobs are selected by priority desc, then
	// scheduled_at asc, then id asc. Returns (nil, nil) when the queue is empty.
	//
	// Jobs whose lease has expired (state=leased, leased_until < now) are
	// eligible again; Claim treats them as reclaimable and may return them.
	Claim(ctx context.Context, queue string, opts ClaimOptions) (*Job, error)

	// RenewLease extends the lease of a job currently leased by opts.LeaseOwner.
	// Returns ErrNotLeased when the lease is missing or owned by someone else.
	RenewLease(ctx context.Context, jobID string, opts ClaimOptions) error

	// Complete marks a leased job as succeeded. The caller must be the lease owner.
	Complete(ctx context.Context, jobID, leaseOwner string) error

	// Fail records a failed attempt on a leased job. The caller must be the lease owner.
	//
	//   - if canRetry and attempt < MaxAttempts: state returns to pending,
	//     scheduled_at is set by the implementation (backoff), attempt is incremented.
	//   - if !canRetry: state becomes failed (terminal).
	//   - if canRetry and attempt >= MaxAttempts: state becomes dead (DLQ).
	//
	// Returns the job's resulting state.
	Fail(ctx context.Context, jobID, leaseOwner, reason string, canRetry bool) (JobState, error)

	// Requeue returns a terminal job to pending (manual retry / DLQ reprocess).
	// Optional scheduledAt <= 0 means now.
	Requeue(ctx context.Context, jobID string, scheduledAt time.Time) error

	// History returns append-only events for a job, oldest first.
	History(ctx context.Context, jobID string) ([]HistoryEvent, error)

	// ListJobs returns one page of jobs matching the query, ordered by id.
	ListJobs(ctx context.Context, q JobQuery) (JobPage, error)

	// ListQueues returns per-queue counts for the dashboard.
	ListQueues(ctx context.Context) ([]QueueInfo, error)

	// Notify returns a channel of storage events. The channel is owned by the
	// implementation and closes on Close. Slow consumers may miss events;
	// notifications are hints, not a durability mechanism.
	Notify() <-chan Event

	// Close releases resources. Subsequent calls return ErrClosed.
	// Close is idempotent.
	Close() error
}
