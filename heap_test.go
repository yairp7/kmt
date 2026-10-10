package kmt_test

import (
	"github.com/yairp7/kmt"
	"math/rand"
	"sort"
	"testing"
)

func TestMinHeap_NewIsEmpty(t *testing.T) {
	h := kmt.NewMinHeap[string]()
	if got := h.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
}

func TestMinHeap_PushReturnsItem(t *testing.T) {
	h := kmt.NewMinHeap[string]()
	item := h.Push(7, "seven")
	if item == nil || item.Offset != 7 || item.Value != "seven" {
		t.Fatalf("Push returned %+v, want Offset=7 Value=seven", item)
	}
	if got := h.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
}

func TestMinHeap_PopReturnsAscendingOffsets(t *testing.T) {
	h := kmt.NewMinHeap[string]()
	h.Push(5, "five")
	h.Push(1, "one")
	h.Push(3, "three")
	h.Push(2, "two")
	h.Push(4, "four")

	want := []struct {
		offset int64
		value  string
	}{{1, "one"}, {2, "two"}, {3, "three"}, {4, "four"}, {5, "five"}}

	for i, w := range want {
		off, val := h.Pop()
		if off != w.offset || val != w.value {
			t.Fatalf("pop %d = (%d, %q), want (%d, %q)", i, off, val, w.offset, w.value)
		}
		if got := h.Len(); got != len(want)-i-1 {
			t.Fatalf("Len() after pop %d = %d, want %d", i, got, len(want)-i-1)
		}
	}
}

func TestMinHeap_DuplicateOffsets(t *testing.T) {
	h := kmt.NewMinHeap[string]()
	h.Push(2, "a")
	h.Push(2, "b")
	h.Push(1, "c")

	if off, val := h.Pop(); off != 1 || val != "c" {
		t.Fatalf("first pop = (%d, %q), want (1, c)", off, val)
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		off, val := h.Pop()
		if off != 2 {
			t.Fatalf("pop offset = %d, want 2", off)
		}
		seen[val] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("expected both duplicates to be popped, got %v", seen)
	}
}

func TestMinHeap_NegativeAndZeroOffsets(t *testing.T) {
	h := kmt.NewMinHeap[int]()
	for _, o := range []int{0, -5, 10, -1} {
		h.Push(int64(o), o*10)
	}
	for _, want := range []int{-5, -1, 0, 10} {
		off, val := h.Pop()
		if off != int64(want) || val != want*10 {
			t.Fatalf("pop = (%d, %d), want (%d, %d)", off, val, want, want*10)
		}
	}
}

func TestMinHeap_InterleavedPushPop(t *testing.T) {
	h := kmt.NewMinHeap[int]()
	h.Push(10, 10)
	h.Push(20, 20)
	if off, _ := h.Pop(); off != 10 {
		t.Fatalf("pop = %d, want 10", off)
	}
	h.Push(5, 5)
	h.Push(15, 15)
	for _, want := range []int{5, 15, 20} {
		if off, _ := h.Pop(); off != int64(want) {
			t.Fatalf("pop = %d, want %d", off, want)
		}
	}
	if h.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", h.Len())
	}
}

func TestMinHeap_PointerValueRetained(t *testing.T) {
	type payload struct{ name string }
	h := kmt.NewMinHeap[*payload]()
	p := &payload{name: "x"}
	h.Push(1, p)
	_, got := h.Pop()
	if got != p {
		t.Fatalf("popped pointer %p, want %p", got, p)
	}
}

func TestMinHeap_PopEmptyPanics(t *testing.T) {
	h := kmt.NewMinHeap[int]()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic when popping an empty heap")
		}
	}()
	h.Pop()
}

func TestMinHeap_RandomMatchesSort(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	h := kmt.NewMinHeap[int64]()
	offsets := make([]int, 500)
	for i := range offsets {
		offsets[i] = rng.Intn(100) - 50
		h.Push(int64(offsets[i]), int64(offsets[i]))
	}
	sort.Ints(offsets)
	for i, want := range offsets {
		off, val := h.Pop()
		if off != int64(want) || val != int64(want) {
			t.Fatalf("pop %d = (%d, %d), want %d", i, off, val, want)
		}
	}
}
