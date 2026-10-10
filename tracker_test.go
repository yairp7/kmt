package kmt_test

import (
	"context"
	"errors"
	"github.com/yairp7/kmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func pushPending(et *kmt.EventsTrackerImpl, offsets ...int64) {
	for _, o := range offsets {
		et.Push(o, kmt.Event{Status: kmt.StatusPending})
	}
}

func mustMark(t *testing.T, et *kmt.EventsTrackerImpl, offset int64, status kmt.EventsTrackerStatus) {
	t.Helper()
	if err := et.MarkResolved(offset, status); err != nil {
		t.Fatalf("MarkResolved(%d) error: %v", offset, err)
	}
}

func TestTracker_PopResolvedEmpty(t *testing.T) {
	et := kmt.New()
	if got := et.PopResolved(); len(got) != 0 {
		t.Fatalf("PopResolved() = %v, want empty", got)
	}
}

func TestTracker_MarkResolvedMissingOffset(t *testing.T) {
	et := kmt.New()
	if err := et.MarkResolved(42, kmt.StatusSuccess); !errors.Is(err, kmt.ErrMissingOffset) {
		t.Fatalf("err = %v, want ErrMissingOffset", err)
	}
}

func TestTracker_PendingHeadBlocksResolvedBehindIt(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2, 3)
	mustMark(t, et, 2, kmt.StatusSuccess)
	mustMark(t, et, 3, kmt.StatusSuccess)

	if got := et.PopResolved(); len(got) != 0 {
		t.Fatalf("PopResolved() = %v, want empty while head is pending", got)
	}

	// The pending head must not have been lost from the heap.
	mustMark(t, et, 1, kmt.StatusSuccess)
	want := []int64{1, 2, 3}
	if got := et.PopResolved(); !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}
}

func TestTracker_PopsOnlyContiguousResolvedPrefix(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2, 3, 4)
	mustMark(t, et, 1, kmt.StatusSuccess)
	mustMark(t, et, 2, kmt.StatusSuccess)
	mustMark(t, et, 4, kmt.StatusSuccess)

	if got, want := et.PopResolved(), []int64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}

	mustMark(t, et, 3, kmt.StatusSuccess)
	if got, want := et.PopResolved(), []int64{3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}
}

func TestTracker_PopsInOffsetOrderRegardlessOfPushOrder(t *testing.T) {
	et := kmt.New()
	pushPending(et, 3, 1, 2)
	for _, o := range []int64{2, 3, 1} {
		mustMark(t, et, o, kmt.StatusSuccess)
	}

	if got, want := et.PopResolved(), []int64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}
}

func TestTracker_FailedHeadBlocksUntilRemarked(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2)
	mustMark(t, et, 1, kmt.StatusFailed)
	mustMark(t, et, 2, kmt.StatusSuccess)

	if got := et.PopResolved(); len(got) != 0 {
		t.Fatalf("PopResolved() = %v, want empty while head is failed", got)
	}

	mustMark(t, et, 1, kmt.StatusSuccess)
	if got, want := et.PopResolved(), []int64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}
}

func TestTracker_DLQCountsAsResolved(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1)
	mustMark(t, et, 1, kmt.StatusDLQ)

	if got, want := et.PopResolved(), []int64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}
}

func TestTracker_PopResolvedDrainsOnce(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2)
	mustMark(t, et, 1, kmt.StatusSuccess)
	mustMark(t, et, 2, kmt.StatusSuccess)

	if got := et.PopResolved(); len(got) != 2 {
		t.Fatalf("first PopResolved() = %v, want 2 offsets", got)
	}
	if got := et.PopResolved(); len(got) != 0 {
		t.Fatalf("second PopResolved() = %v, want empty", got)
	}
	// Popped offsets are forgotten.
	if err := et.MarkResolved(1, kmt.StatusSuccess); !errors.Is(err, kmt.ErrMissingOffset) {
		t.Fatalf("err = %v, want kmt.ErrMissingOffset for popped offset", err)
	}
}

func TestTracker_DuplicatePushIsIgnored(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1)
	mustMark(t, et, 1, kmt.StatusSuccess)
	// Re-pushing a tracked offset must not reset its status or add a second heap entry.
	et.Push(1, kmt.Event{Status: kmt.StatusPending})

	if got, want := et.PopResolved(), []int64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}
	if got := et.PopResolved(); len(got) != 0 {
		t.Fatalf("second PopResolved() = %v, want empty (no duplicate entry)", got)
	}
}

func TestTracker_DoesNotPopPastOffsetNotYetPushed(t *testing.T) {
	et := kmt.New()
	pushPending(et, 0, 2)
	mustMark(t, et, 0, kmt.StatusSuccess)
	mustMark(t, et, 2, kmt.StatusSuccess)

	if got, want := et.PopResolved(), []int64{0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v while offset 1 is not pushed", got, want)
	}

	pushPending(et, 1)
	mustMark(t, et, 1, kmt.StatusSuccess)
	if got, want := et.PopResolved(), []int64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}
}

func TestTracker_StartOffsetWaitsForFirstOffset(t *testing.T) {
	et := kmt.New(kmt.WithStartOffset(10))
	pushPending(et, 11)
	mustMark(t, et, 11, kmt.StatusSuccess)

	if got := et.PopResolved(); len(got) != 0 {
		t.Fatalf("PopResolved() = %v, want empty until start offset 10 is resolved", got)
	}

	pushPending(et, 10)
	mustMark(t, et, 10, kmt.StatusSuccess)
	if got, want := et.PopResolved(), []int64{10, 11}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}
}

func TestTracker_PushBelowStartOffsetIsIgnored(t *testing.T) {
	et := kmt.New(kmt.WithStartOffset(5))
	pushPending(et, 4)
	if err := et.MarkResolved(4, kmt.StatusSuccess); !errors.Is(err, kmt.ErrMissingOffset) {
		t.Fatalf("err = %v, want ErrMissingOffset for offset below start", err)
	}
}

func TestTracker_LowerPushBeforeFirstPopMovesStart(t *testing.T) {
	et := kmt.New()
	pushPending(et, 2, 1)
	mustMark(t, et, 2, kmt.StatusSuccess)

	if got := et.PopResolved(); len(got) != 0 {
		t.Fatalf("PopResolved() = %v, want empty while offset 1 is pending", got)
	}

	mustMark(t, et, 1, kmt.StatusSuccess)
	if got, want := et.PopResolved(), []int64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}
}

// A redelivered offset that was already popped must not be popped again.
func TestTracker_PushOfPoppedOffsetIsIgnored(t *testing.T) {
	et := kmt.New()
	pushPending(et, 0, 1)
	mustMark(t, et, 0, kmt.StatusSuccess)
	mustMark(t, et, 1, kmt.StatusSuccess)
	et.PopResolved()

	pushPending(et, 1)
	if err := et.MarkResolved(1, kmt.StatusSuccess); !errors.Is(err, kmt.ErrMissingOffset) {
		t.Fatalf("err = %v, want ErrMissingOffset for already popped offset", err)
	}
	if got := et.PopResolved(); len(got) != 0 {
		t.Fatalf("PopResolved() = %v, want empty", got)
	}

	// The tracker still advances normally afterwards.
	pushPending(et, 2)
	mustMark(t, et, 2, kmt.StatusSuccess)
	if got, want := et.PopResolved(), []int64{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PopResolved() = %v, want %v", got, want)
	}
}

// Run with -race: workers that push their own offsets race each other, and
// pops must still come out contiguous.
func TestTracker_ConcurrentPushPopsContiguously(t *testing.T) {
	const n, workers = 50_000, 16
	et := kmt.New(kmt.WithStartOffset(0))

	var next atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				o := next.Add(1) - 1
				if o >= n {
					return
				}
				et.Push(o, kmt.Event{Status: kmt.StatusPending})
				if err := et.MarkResolved(o, kmt.StatusSuccess); err != nil {
					t.Errorf("MarkResolved(%d): %v", o, err)
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	var popped []int64
	for finished := false; !finished; {
		select {
		case <-done:
			finished = true
		default:
		}
		popped = append(popped, et.PopResolved()...)
	}

	if len(popped) != n {
		t.Fatalf("popped %d offsets, want %d", len(popped), n)
	}
	for i, o := range popped {
		if o != int64(i) {
			t.Fatalf("popped[%d] = %d, want %d", i, o, i)
		}
	}
}

// collector gathers committed offsets from the committer goroutine.
type collector struct {
	mu      sync.Mutex
	offsets []int64
	calls   int
}

func (c *collector) commit(offsets []int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offsets = append(c.offsets, offsets...)
	c.calls++
	return nil
}

func (c *collector) snapshot() ([]int64, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int64(nil), c.offsets...), c.calls
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func startCommitter(et *kmt.EventsTrackerImpl, commit func([]int64) error) (cancel func(), done <-chan error) {
	ctx, cancelCtx := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- et.StartCommitter(ctx, time.Millisecond, commit) }()
	return cancelCtx, ch
}

func TestCommitter_CommitsResolvedOffsets(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2, 3)
	c := &collector{}
	cancel, done := startCommitter(et, c.commit)

	mustMark(t, et, 1, kmt.StatusSuccess)
	mustMark(t, et, 2, kmt.StatusDLQ)
	waitFor(t, func() bool { got, _ := c.snapshot(); return len(got) == 2 })

	mustMark(t, et, 3, kmt.StatusSuccess)
	waitFor(t, func() bool { got, _ := c.snapshot(); return len(got) == 3 })

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("StartCommitter err = %v, want context.Canceled", err)
	}
	if got, _ := c.snapshot(); !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatalf("committed = %v, want [1 2 3]", got)
	}
}

func TestCommitter_DoesNotCommitWhileHeadPending(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2)
	c := &collector{}
	cancel, done := startCommitter(et, c.commit)

	mustMark(t, et, 2, kmt.StatusSuccess)
	time.Sleep(20 * time.Millisecond)
	if _, calls := c.snapshot(); calls != 0 {
		t.Fatalf("commit called %d times while head pending, want 0", calls)
	}

	mustMark(t, et, 1, kmt.StatusSuccess)
	waitFor(t, func() bool { got, _ := c.snapshot(); return len(got) == 2 })

	cancel()
	<-done
	if got, _ := c.snapshot(); !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("committed = %v, want [1 2]", got)
	}
}

// failingCollector fails the first `failures` commits, then records like collector.
type failingCollector struct {
	collector
	failures int
	attempts [][]int64
}

func (c *failingCollector) commit(offsets []int64) error {
	c.mu.Lock()
	c.attempts = append(c.attempts, append([]int64(nil), offsets...))
	fail := len(c.attempts) <= c.failures
	c.mu.Unlock()
	if fail {
		return errors.New("broker unavailable")
	}
	return c.collector.commit(offsets)
}

func (c *failingCollector) snapshotAttempts() [][]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]int64(nil), c.attempts...)
}

func TestCommitter_RetriesFailedCommit(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2)
	c := &failingCollector{failures: 3}
	cancel, done := startCommitter(et, c.commit)

	mustMark(t, et, 1, kmt.StatusSuccess)
	mustMark(t, et, 2, kmt.StatusSuccess)
	waitFor(t, func() bool { got, _ := c.snapshot(); return len(got) == 2 })

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("StartCommitter err = %v, want context.Canceled", err)
	}
	if got, _ := c.snapshot(); !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("committed = %v, want [1 2]", got)
	}
	if attempts := c.snapshotAttempts(); len(attempts) != 4 {
		t.Fatalf("commit attempts = %v, want 3 failures then 1 success", attempts)
	}
}

// If the committer stops while a commit is still failing, a restarted
// committer must retry those offsets rather than skip them.
func TestCommitter_RestartRetriesUncommittedOffsets(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2)
	mustMark(t, et, 1, kmt.StatusSuccess)
	mustMark(t, et, 2, kmt.StatusSuccess)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	boom := errors.New("boom")
	if err := et.StartCommitter(ctx, time.Hour, func([]int64) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("first StartCommitter err = %v, want boom", err)
	}

	pushPending(et, 3)
	mustMark(t, et, 3, kmt.StatusSuccess)
	c := &collector{}
	cancel2, done := startCommitter(et, c.commit)
	waitFor(t, func() bool { got, _ := c.snapshot(); return len(got) == 3 })
	cancel2()
	<-done

	if got, _ := c.snapshot(); !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatalf("committed after restart = %v, want [1 2 3]", got)
	}
}

// Offsets resolved while a commit keeps failing are merged into the retried batch.
func TestCommitter_RetryIncludesNewlyResolvedOffsets(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2)
	c := &failingCollector{failures: 1 << 30}
	cancel, done := startCommitter(et, c.commit)

	mustMark(t, et, 1, kmt.StatusSuccess)
	waitFor(t, func() bool { return len(c.snapshotAttempts()) >= 1 })
	mustMark(t, et, 2, kmt.StatusSuccess)
	waitFor(t, func() bool {
		a := c.snapshotAttempts()
		return reflect.DeepEqual(a[len(a)-1], []int64{1, 2})
	})

	c.mu.Lock()
	c.failures = 0
	c.mu.Unlock()
	waitFor(t, func() bool { got, _ := c.snapshot(); return len(got) == 2 })

	cancel()
	<-done
	if got, _ := c.snapshot(); !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("committed = %v, want [1 2]", got)
	}
}

func TestCommitter_FlushesOnCancel(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2)
	mustMark(t, et, 1, kmt.StatusSuccess)
	mustMark(t, et, 2, kmt.StatusSuccess)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &collector{}
	// The interval never elapses, so only the shutdown flush can commit.
	err := et.StartCommitter(ctx, time.Hour, c.commit)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("StartCommitter err = %v, want context.Canceled", err)
	}
	if got, _ := c.snapshot(); !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("committed on cancel = %v, want [1 2]", got)
	}
}

func TestCommitter_FlushErrorIsReturnedOnCancel(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1)
	mustMark(t, et, 1, kmt.StatusSuccess)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	boom := errors.New("boom")
	err := et.StartCommitter(ctx, time.Hour, func([]int64) error { return boom })
	if !errors.Is(err, boom) || !errors.Is(err, context.Canceled) {
		t.Fatalf("StartCommitter err = %v, want both boom and context.Canceled", err)
	}
}

func TestCommitter_KeepsRunningAfterCommit(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2)
	c := &collector{}
	cancel, done := startCommitter(et, c.commit)
	defer cancel()

	mustMark(t, et, 1, kmt.StatusSuccess)
	waitFor(t, func() bool { _, calls := c.snapshot(); return calls == 1 })

	select {
	case err := <-done:
		t.Fatalf("StartCommitter returned %v after a successful commit", err)
	case <-time.After(20 * time.Millisecond):
	}

	mustMark(t, et, 2, kmt.StatusSuccess)
	waitFor(t, func() bool { _, calls := c.snapshot(); return calls == 2 })
}

// Run with -race: concurrent pushes, marks and commits must neither race nor lose offsets.
func TestCommitter_ConcurrentMarksCommitEverythingInOrder(t *testing.T) {
	const n = 500
	et := kmt.New()
	for i := int64(0); i < n; i++ {
		et.Push(i, kmt.Event{})
	}
	c := &collector{}
	cancel, done := startCommitter(et, c.commit)

	var wg sync.WaitGroup
	const workers = 8
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := int64(w); i < n; i += workers {
				if err := et.MarkResolved(i, kmt.StatusSuccess); err != nil {
					t.Errorf("MarkResolved(%d): %v", i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	waitFor(t, func() bool { got, _ := c.snapshot(); return len(got) == n })
	cancel()
	<-done

	got, _ := c.snapshot()
	for i, o := range got {
		if o != int64(i) {
			t.Fatalf("committed[%d] = %d, want %d (offsets must be committed in order)", i, o, i)
		}
	}
}
