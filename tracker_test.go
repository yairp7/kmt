package kmt_test

import (
	"context"
	"errors"
	"github.com/yairp7/kmt"
	"reflect"
	"sync"
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
	if got := et.ResolvedCount(); got != 0 {
		t.Fatalf("ResolvedCount() = %d, want 0", got)
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

func TestTracker_ResolvedCountOnlyBumpsForTerminalStatuses(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1, 2, 3)

	mustMark(t, et, 1, kmt.StatusFailed)
	if got := et.ResolvedCount(); got != 0 {
		t.Fatalf("ResolvedCount() after Failed = %d, want 0", got)
	}
	mustMark(t, et, 2, kmt.StatusSuccess)
	mustMark(t, et, 3, kmt.StatusDLQ)
	if got := et.ResolvedCount(); got != 2 {
		t.Fatalf("ResolvedCount() = %d, want 2", got)
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

func TestCommitter_StopsOnCommitError(t *testing.T) {
	et := kmt.New()
	pushPending(et, 1)
	boom := errors.New("boom")
	cancel, done := startCommitter(et, func([]int64) error { return boom })
	defer cancel()

	mustMark(t, et, 1, kmt.StatusSuccess)
	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("StartCommitter err = %v, want boom", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StartCommitter did not return after commit error")
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
