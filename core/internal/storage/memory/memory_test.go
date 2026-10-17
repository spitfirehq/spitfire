package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spitfirehq/spitfire/core/internal/storage"
	"github.com/spitfirehq/spitfire/core/internal/storage/memory"
)

func newStore(t *testing.T) *memory.Store {
	t.Helper()
	s := memory.New()
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustEnqueue(t *testing.T, s storage.Storage, in storage.NewJob) *storage.Job {
	t.Helper()
	res, err := s.Enqueue(context.Background(), in)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if res.Duplicate {
		t.Fatalf("unexpected duplicate")
	}
	return res.Job
}

func TestEnqueueIdempotency(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	in := storage.NewJob{Queue: "default", Payload: []byte("a"), IdempotencyKey: "k1"}
	first, err := s.Enqueue(ctx, in)
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if first.Duplicate {
		t.Fatal("first enqueue reported duplicate")
	}

	second, err := s.Enqueue(ctx, storage.NewJob{Queue: "default", Payload: []byte("b"), IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if !second.Duplicate {
		t.Fatal("second enqueue should be duplicate")
	}
	if second.Job.ID != first.Job.ID {
		t.Fatalf("duplicate returned different job: %s vs %s", second.Job.ID, first.Job.ID)
	}

	// Different key creates a new job.
	third, err := s.Enqueue(ctx, storage.NewJob{Queue: "default", Payload: []byte("c"), IdempotencyKey: "k2"})
	if err != nil {
		t.Fatalf("third enqueue: %v", err)
	}
	if third.Duplicate || third.Job.ID == first.Job.ID {
		t.Fatal("distinct key must create a new job")
	}
}

func TestClaimIsExclusiveAndExpires(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEnqueue(t, s, storage.NewJob{Queue: "q", Payload: []byte("x")})

	owner := storage.ClaimOptions{LeaseOwner: "w1", LeaseDuration: 50 * time.Millisecond}
	job, err := s.Claim(ctx, "q", owner)
	if err != nil || job == nil {
		t.Fatalf("claim: job=%v err=%v", job, err)
	}
	if job.State != storage.JobLeased || job.LeaseOwner != "w1" {
		t.Fatalf("bad lease state: %+v", job)
	}
	if job.Attempt != 1 {
		t.Fatalf("attempt should be 1 after claim, got %d", job.Attempt)
	}

	// Second worker cannot claim the same job while leased.
	job2, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w2", LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if job2 != nil {
		t.Fatalf("second worker claimed a leased job: %+v", job2)
	}

	// Wrong owner cannot complete.
	if err := s.Complete(ctx, job.ID, "w2"); !errors.Is(err, storage.ErrNotLeased) {
		t.Fatalf("complete by wrong owner: %v", err)
	}

	// Lease expires → reclaimed by another worker.
	time.Sleep(60 * time.Millisecond)
	job3, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w2", LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("reclaim claim: %v", err)
	}
	if job3 == nil || job3.LeaseOwner != "w2" {
		t.Fatalf("expected reclaim by w2, got %+v", job3)
	}
}

func TestCompleteAndHistory(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	job := mustEnqueue(t, s, storage.NewJob{Queue: "q", Payload: []byte("x")})

	claimed, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w1", LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.Complete(ctx, claimed.ID, "w1"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	got, err := s.ListJobs(ctx, storage.JobQuery{Queue: "q", State: storage.JobSucceeded})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got.Jobs) != 1 || got.Jobs[0].ID != job.ID {
		t.Fatalf("succeeded job not listed: %+v", got.Jobs)
	}

	hist, err := s.History(ctx, job.ID)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	want := []storage.EventType{storage.EventEnqueued, storage.EventClaimed, storage.EventCompleted}
	if len(hist) != len(want) {
		t.Fatalf("history len=%d want %d: %+v", len(hist), len(want), hist)
	}
	for i, typ := range want {
		if hist[i].Type != typ {
			t.Fatalf("history[%d]=%s want %s", i, hist[i].Type, typ)
		}
	}

	// Terminal job cannot be claimed again.
	again, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w2", LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim after complete: %v", err)
	}
	if again != nil {
		t.Fatalf("claimed a completed job: %+v", again)
	}
}

func TestFailRetryThenDeadLetter(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	job := mustEnqueue(t, s, storage.NewJob{Queue: "q", Payload: []byte("x"), MaxAttempts: 2})

	// Attempt 1 fails with retry → pending again.
	c1, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w", LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim1: %v", err)
	}
	state, err := s.Fail(ctx, c1.ID, "w", "boom", true)
	if err != nil {
		t.Fatalf("fail1: %v", err)
	}
	if state != storage.JobPending {
		t.Fatalf("after retry fail state=%s want pending", state)
	}

	// Backoff is applied — not immediately claimable.
	quick, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w", LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim during backoff: %v", err)
	}
	if quick != nil {
		t.Fatalf("claimed during backoff: %+v", quick)
	}

	// Wait out the short test backoff, then second failure exhausts attempts → dead.
	time.Sleep(120 * time.Millisecond)
	c2, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w", LeaseDuration: time.Second})
	if err != nil || c2 == nil {
		t.Fatalf("claim2: job=%v err=%v", c2, err)
	}
	state, err = s.Fail(ctx, c2.ID, "w", "boom again", true)
	if err != nil {
		t.Fatalf("fail2: %v", err)
	}
	if state != storage.JobDead {
		t.Fatalf("exhausted retries state=%s want dead", state)
	}
	if job.MaxAttempts != 2 {
		t.Fatalf("max attempts not preserved: %d", job.MaxAttempts)
	}

	// Manual requeue from DLQ.
	if err := s.Requeue(ctx, c2.ID, time.Time{}); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	after, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w", LeaseDuration: time.Second})
	if err != nil || after == nil {
		t.Fatalf("claim after requeue: job=%v err=%v", after, err)
	}
}

func TestFailWithoutRetryIsTerminalFailed(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEnqueue(t, s, storage.NewJob{Queue: "q", Payload: []byte("x"), MaxAttempts: 5})

	c, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w", LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	state, err := s.Fail(ctx, c.ID, "w", "invalid payload", false)
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	if state != storage.JobFailed {
		t.Fatalf("state=%s want failed", state)
	}
}

func TestClaimOrderPriorityThenSchedule(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEnqueue(t, s, storage.NewJob{Queue: "q", Payload: []byte("low"), Priority: 0, ScheduledAt: time.Now().Add(-time.Hour)})
	mustEnqueue(t, s, storage.NewJob{Queue: "q", Payload: []byte("high"), Priority: 10, ScheduledAt: time.Now().Add(-time.Hour)})
	mustEnqueue(t, s, storage.NewJob{Queue: "q", Payload: []byte("later"), Priority: 10, ScheduledAt: time.Now().Add(time.Hour)})

	j1, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w", LeaseDuration: time.Second})
	if err != nil || j1 == nil {
		t.Fatalf("claim1: %v %v", j1, err)
	}
	if string(j1.Payload) != "high" {
		t.Fatalf("first claim payload=%s want high", j1.Payload)
	}
	j2, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w", LeaseDuration: time.Second})
	if err != nil || j2 == nil {
		t.Fatalf("claim2: %v %v", j2, err)
	}
	if string(j2.Payload) != "low" {
		t.Fatalf("second claim payload=%s want low", j2.Payload)
	}
	j3, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w", LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim3: %v", err)
	}
	if j3 != nil {
		t.Fatalf("claimed future-scheduled job: %+v", j3)
	}
}

func TestRenewLeaseAndOwnership(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEnqueue(t, s, storage.NewJob{Queue: "q", Payload: []byte("x")})

	j, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w1", LeaseDuration: 30 * time.Millisecond})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Renew before expiry keeps the lease.
	if err := s.RenewLease(ctx, j.ID, storage.ClaimOptions{LeaseOwner: "w1", LeaseDuration: 200 * time.Millisecond}); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := s.RenewLease(ctx, j.ID, storage.ClaimOptions{LeaseOwner: "w2", LeaseDuration: time.Second}); !errors.Is(err, storage.ErrNotLeased) {
		t.Fatalf("renew by wrong owner: %v", err)
	}
	// Still leased after original deadline thanks to renew.
	time.Sleep(50 * time.Millisecond)
	j2, err := s.Claim(ctx, "q", storage.ClaimOptions{LeaseOwner: "w2", LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim2: %v", err)
	}
	if j2 != nil {
		t.Fatalf("job reclaimed despite renew: %+v", j2)
	}
}

func TestListJobsPaginationAndQueues(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		mustEnqueue(t, s, storage.NewJob{Queue: "q", Payload: []byte{byte('a' + i)}})
	}

	page1, err := s.ListJobs(ctx, storage.JobQuery{Queue: "q", Limit: 2})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1.Jobs) != 2 || page1.NextID == "" {
		t.Fatalf("page1: %d jobs next=%q", len(page1.Jobs), page1.NextID)
	}
	page2, err := s.ListJobs(ctx, storage.JobQuery{Queue: "q", Limit: 2, AfterID: page1.NextID})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2.Jobs) != 2 {
		t.Fatalf("page2: %d jobs", len(page2.Jobs))
	}

	infos, err := s.ListQueues(ctx)
	if err != nil {
		t.Fatalf("list queues: %v", err)
	}
	if len(infos) != 1 || infos[0].Name != "q" || infos[0].Pending != 5 {
		t.Fatalf("queue info: %+v", infos)
	}
}

func TestCloseIsIdempotentAndSubsequentCallsFail(t *testing.T) {
	s := memory.New()
	if err := s.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := s.Close(); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("second close: %v", err)
	}
	_, err := s.Enqueue(context.Background(), storage.NewJob{Queue: "q", Payload: []byte("x")})
	if !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("enqueue after close: %v", err)
	}
}
