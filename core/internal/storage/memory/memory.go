// Package memory is an in-process Storage fake used by tests.
//
// It implements the same contract as the SQLite backend without touching disk.
// Tests that pass against memory and sqlite prove the interface does not leak
// backend-specific assumptions.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/spitfirehq/spitfire/core/internal/storage"
)

// Store is a thread-safe in-memory implementation of storage.Storage.
type Store struct {
	mu sync.Mutex

	jobs      map[string]*storage.Job
	byIdem    map[string]string // queue + "\x00" + key → job id
	history   map[string][]storage.HistoryEvent
	queues    map[string]struct{}
	nextHist  int64
	closed    bool
	notifyCh  chan storage.Event
	now       func() time.Time
}

// Option configures a Store.
type Option func(*Store)

// WithClock overrides the time source (tests).
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// New returns an empty in-memory store.
func New(opts ...Option) *Store {
	s := &Store{
		jobs:     make(map[string]*storage.Job),
		byIdem:   make(map[string]string),
		history:  make(map[string][]storage.HistoryEvent),
		queues:   make(map[string]struct{}),
		notifyCh: make(chan storage.Event, 256),
		now:      time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

var _ storage.Storage = (*Store)(nil)

func (s *Store) Enqueue(ctx context.Context, in storage.NewJob) (storage.EnqueueResult, error) {
	if err := ctx.Err(); err != nil {
		return storage.EnqueueResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.EnqueueResult{}, storage.ErrClosed
	}

	if in.Queue == "" {
		return storage.EnqueueResult{}, fmt.Errorf("storage: queue is required")
	}
	s.queues[in.Queue] = struct{}{}

	if in.IdempotencyKey != "" {
		if id, ok := s.byIdem[idemKey(in.Queue, in.IdempotencyKey)]; ok {
			job := s.jobs[id]
			return storage.EnqueueResult{Job: clone(job), Duplicate: true}, nil
		}
	}

	maxAttempts := in.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = storage.DefaultMaxAttempts
	}
	sched := in.ScheduledAt
	if sched.IsZero() {
		sched = s.now()
	}
	created := s.now()

	job := &storage.Job{
		ID:             uuid.NewString(),
		Queue:          in.Queue,
		State:          storage.JobPending,
		Payload:        append([]byte(nil), in.Payload...),
		IdempotencyKey: in.IdempotencyKey,
		Attempt:        0,
		MaxAttempts:    maxAttempts,
		Priority:       in.Priority,
		CreatedAt:      created,
		ScheduledAt:    sched,
	}
	s.jobs[job.ID] = job
	if in.IdempotencyKey != "" {
		s.byIdem[idemKey(in.Queue, in.IdempotencyKey)] = job.ID
	}
	s.appendLocked(job.ID, storage.EventEnqueued, "")
	s.emitLocked(storage.Event{Type: storage.EventEnqueued, Job: clone(job)})

	return storage.EnqueueResult{Job: clone(job)}, nil
}

func (s *Store) Claim(ctx context.Context, queue string, opts storage.ClaimOptions) (*storage.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if queue == "" {
		return nil, fmt.Errorf("storage: queue is required")
	}
	if opts.LeaseOwner == "" {
		return nil, fmt.Errorf("storage: lease owner is required")
	}
	lease := opts.LeaseDuration
	if lease <= 0 {
		lease = storage.DefaultLeaseDuration
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, storage.ErrClosed
	}

	now := s.now()
	deadline := now.Add(lease)

	// Reclaim expired leases so they are claimable again.
	for _, job := range s.jobs {
		if job.State == storage.JobLeased && job.LeasedUntil.Before(now) {
			job.State = storage.JobPending
			job.LeaseOwner = ""
			job.LeasedUntil = time.Time{}
			s.appendLocked(job.ID, storage.EventExpired, "lease expired")
			s.emitLocked(storage.Event{Type: storage.EventExpired, Job: clone(job)})
		}
	}

	var ready []*storage.Job
	for _, job := range s.jobs {
		if job.Queue != queue || job.State != storage.JobPending {
			continue
		}
		if job.ScheduledAt.After(now) {
			continue
		}
		ready = append(ready, job)
	}
	if len(ready) == 0 {
		return nil, nil
	}

	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Priority != ready[j].Priority {
			return ready[i].Priority > ready[j].Priority
		}
		if !ready[i].ScheduledAt.Equal(ready[j].ScheduledAt) {
			return ready[i].ScheduledAt.Before(ready[j].ScheduledAt)
		}
		return ready[i].ID < ready[j].ID
	})

	job := ready[0]
	job.State = storage.JobLeased
	job.LeaseOwner = opts.LeaseOwner
	job.LeasedUntil = deadline
	job.Attempt++
	s.appendLocked(job.ID, storage.EventClaimed, opts.LeaseOwner)
	s.emitLocked(storage.Event{Type: storage.EventClaimed, Job: clone(job)})
	return clone(job), nil
}

func (s *Store) RenewLease(ctx context.Context, jobID string, opts storage.ClaimOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.ErrClosed
	}

	job, ok := s.jobs[jobID]
	if !ok {
		return storage.ErrNotFound
	}
	if job.State != storage.JobLeased || job.LeaseOwner != opts.LeaseOwner {
		return storage.ErrNotLeased
	}
	lease := opts.LeaseDuration
	if lease <= 0 {
		lease = storage.DefaultLeaseDuration
	}
	job.LeasedUntil = s.now().Add(lease)
	s.appendLocked(job.ID, storage.EventRenewed, opts.LeaseOwner)
	return nil
}

func (s *Store) Complete(ctx context.Context, jobID, leaseOwner string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.ErrClosed
	}

	job, ok := s.jobs[jobID]
	if !ok {
		return storage.ErrNotFound
	}
	if job.State != storage.JobLeased || job.LeaseOwner != leaseOwner {
		return storage.ErrNotLeased
	}
	job.State = storage.JobSucceeded
	job.CompletedAt = s.now()
	job.LeaseOwner = ""
	job.LeasedUntil = time.Time{}
	s.appendLocked(job.ID, storage.EventCompleted, "")
	s.emitLocked(storage.Event{Type: storage.EventCompleted, Job: clone(job)})
	return nil
}

func (s *Store) Fail(ctx context.Context, jobID, leaseOwner, reason string, canRetry bool) (storage.JobState, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", storage.ErrClosed
	}

	job, ok := s.jobs[jobID]
	if !ok {
		return "", storage.ErrNotFound
	}
	if job.State != storage.JobLeased || job.LeaseOwner != leaseOwner {
		return "", storage.ErrNotLeased
	}

	job.LastError = reason
	job.LeaseOwner = ""
	job.LeasedUntil = time.Time{}

	switch {
	case !canRetry:
		job.State = storage.JobFailed
		job.CompletedAt = s.now()
		s.appendLocked(job.ID, storage.EventFailed, reason)
		s.emitLocked(storage.Event{Type: storage.EventFailed, Job: clone(job)})
	case job.Attempt >= job.MaxAttempts:
		job.State = storage.JobDead
		job.CompletedAt = s.now()
		s.appendLocked(job.ID, storage.EventDeadLettered, reason)
		s.emitLocked(storage.Event{Type: storage.EventDeadLettered, Job: clone(job)})
	default:
		// Backoff: 50ms * 2^attempt, capped at 5s. Phase 3 makes this configurable.
		backoff := 50 * time.Millisecond * time.Duration(1<<min(job.Attempt, 7))
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
		job.State = storage.JobPending
		job.ScheduledAt = s.now().Add(backoff)
		s.appendLocked(job.ID, storage.EventFailed, reason)
		s.emitLocked(storage.Event{Type: storage.EventFailed, Job: clone(job)})
	}
	return job.State, nil
}

func (s *Store) Requeue(ctx context.Context, jobID string, scheduledAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.ErrClosed
	}

	job, ok := s.jobs[jobID]
	if !ok {
		return storage.ErrNotFound
	}
	if job.State == storage.JobLeased {
		return fmt.Errorf("%w: cannot requeue a leased job", storage.ErrInvalidState)
	}
	if job.State == storage.JobPending {
		return fmt.Errorf("%w: already pending", storage.ErrInvalidState)
	}
	if scheduledAt.IsZero() {
		scheduledAt = s.now()
	}
	job.State = storage.JobPending
	job.ScheduledAt = scheduledAt
	job.CompletedAt = time.Time{}
	job.LeaseOwner = ""
	job.LeasedUntil = time.Time{}
	s.appendLocked(job.ID, storage.EventRequeued, "")
	s.emitLocked(storage.Event{Type: storage.EventRequeued, Job: clone(job)})
	return nil
}

func (s *Store) History(ctx context.Context, jobID string) ([]storage.HistoryEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, storage.ErrClosed
	}
	if _, ok := s.jobs[jobID]; !ok {
		return nil, storage.ErrNotFound
	}
	out := append([]storage.HistoryEvent(nil), s.history[jobID]...)
	return out, nil
}

func (s *Store) ListJobs(ctx context.Context, q storage.JobQuery) (storage.JobPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.JobPage{}, err
	}
	limit := q.Limit
	if limit <= 0 {
		limit = storage.DefaultClaimLimit
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.JobPage{}, storage.ErrClosed
	}

	var ids []string
	for id := range s.jobs {
		if q.AfterID != "" && id <= q.AfterID {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	page := storage.JobPage{}
	for _, id := range ids {
		job := s.jobs[id]
		if q.Queue != "" && job.Queue != q.Queue {
			continue
		}
		if q.State != "" && job.State != q.State {
			continue
		}
		page.Jobs = append(page.Jobs, clone(job))
		if len(page.Jobs) >= limit {
			if len(ids) > 0 {
				page.NextID = id
			}
			break
		}
	}
	return page, nil
}

func (s *Store) ListQueues(ctx context.Context) ([]storage.QueueInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, storage.ErrClosed
	}

	byQueue := map[string]*storage.QueueInfo{}
	for name := range s.queues {
		byQueue[name] = &storage.QueueInfo{Name: name}
	}
	for _, job := range s.jobs {
		info, ok := byQueue[job.Queue]
		if !ok {
			info = &storage.QueueInfo{Name: job.Queue}
			byQueue[job.Queue] = info
		}
		switch job.State {
		case storage.JobPending:
			info.Pending++
		case storage.JobLeased:
			info.Leased++
		case storage.JobSucceeded:
			info.Succeeded++
		case storage.JobFailed:
			info.Failed++
		case storage.JobDead:
			info.Dead++
		}
	}

	out := make([]storage.QueueInfo, 0, len(byQueue))
	for _, info := range byQueue {
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *Store) Notify() <-chan storage.Event {
	return s.notifyCh
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.ErrClosed
	}
	s.closed = true
	close(s.notifyCh)
	return nil
}

// --- helpers ---

func idemKey(queue, key string) string {
	return queue + "\x00" + key
}

func clone(j *storage.Job) *storage.Job {
	if j == nil {
		return nil
	}
	c := *j
	c.Payload = append([]byte(nil), j.Payload...)
	return &c
}

func (s *Store) appendLocked(jobID string, t storage.EventType, detail string) {
	s.nextHist++
	s.history[jobID] = append(s.history[jobID], storage.HistoryEvent{
		ID:     s.nextHist,
		JobID:  jobID,
		Type:   t,
		Detail: detail,
		At:     s.now(),
	})
}

// emitLocked drops events if the buffer is full — notify is a hint, not a log.
func (s *Store) emitLocked(ev storage.Event) {
	select {
	case s.notifyCh <- ev:
	default:
	}
}


