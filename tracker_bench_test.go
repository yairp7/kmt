package kmt_test

import (
	"fmt"
	"github.com/yairp7/kmt"
	"sync/atomic"
	"testing"
)

// prefilled returns a tracker holding n pending events at offsets 0..n-1.
func prefilled(n int) *kmt.EventsTrackerImpl {
	et := kmt.New()
	for i := 0; i < n; i++ {
		et.Push(int64(i), kmt.Event{})
	}
	return et
}

func BenchmarkTracker_Push(b *testing.B) {
	et := kmt.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		et.Push(int64(i), kmt.Event{})
	}
}

func BenchmarkTracker_PushDuplicate(b *testing.B) {
	et := prefilled(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		et.Push(0, kmt.Event{})
	}
}

func BenchmarkTracker_MarkResolved(b *testing.B) {
	et := prefilled(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = et.MarkResolved(int64(i), kmt.StatusSuccess)
	}
}

func BenchmarkTracker_MarkResolvedMissing(b *testing.B) {
	et := kmt.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = et.MarkResolved(int64(i), kmt.StatusSuccess)
	}
}

func BenchmarkTracker_ResolvedCount(b *testing.B) {
	et := kmt.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = et.ResolvedCount()
	}
}

// Head is pending, so PopResolved peeks once and returns without popping.
func BenchmarkTracker_PopResolved_Blocked(b *testing.B) {
	for _, size := range []int{10, 1_000, 100_000} {
		b.Run(fmt.Sprintf("heap=%d", size), func(b *testing.B) {
			et := prefilled(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = et.PopResolved()
			}
		})
	}
}

// Cost of draining a fully resolved heap in one call; ns/op is per batch.
func BenchmarkTracker_PopResolved_Drain(b *testing.B) {
	for _, size := range []int{10, 1_000, 100_000} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				et := prefilled(size)
				for o := 0; o < size; o++ {
					_ = et.MarkResolved(int64(o), kmt.StatusSuccess)
				}
				b.StartTimer()
				if got := et.PopResolved(); len(got) != size {
					b.Fatalf("popped %d, want %d", len(got), size)
				}
			}
		})
	}
}

// Steady state: push, resolve and pop one offset at a time over a standing backlog.
func BenchmarkTracker_Cycle(b *testing.B) {
	for _, backlog := range []int{0, 1_000, 100_000} {
		b.Run(fmt.Sprintf("backlog=%d", backlog), func(b *testing.B) {
			et := kmt.New()
			// Backlog sits at high offsets so the cycled offset is always the heap minimum.
			base := int64(1) << 40
			for i := 0; i < backlog; i++ {
				et.Push(base+int64(i), kmt.Event{})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				o := int64(i)
				et.Push(o, kmt.Event{})
				_ = et.MarkResolved(o, kmt.StatusSuccess)
				_ = et.PopResolved()
			}
		})
	}
}

func BenchmarkTracker_ParallelPush(b *testing.B) {
	et := kmt.New()
	var next atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			et.Push(next.Add(1), kmt.Event{})
		}
	})
}

func BenchmarkTracker_ParallelMarkResolved(b *testing.B) {
	et := prefilled(1_000_000)
	var next atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Re-marking an existing offset keeps the benchmark independent of b.N.
			_ = et.MarkResolved(next.Add(1)%1_000_000, kmt.StatusSuccess)
		}
	})
}
