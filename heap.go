package kmt

import (
	"container/heap"
)

type HeapItem[T any] struct {
	Offset int64
	Value  T
}

type genericHeap[T any] []*HeapItem[T]

func (h genericHeap[T]) Len() int { return len(h) }

func (h genericHeap[T]) Less(i, j int) bool { return h[i].Offset < h[j].Offset }

func (h genericHeap[T]) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *genericHeap[T]) Push(x any) { *h = append(*h, x.(*HeapItem[T])) }

func (h *genericHeap[T]) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}

type MinHeap[T any] struct {
	inner *genericHeap[T]
}

func NewMinHeap[T any]() *MinHeap[T] {
	gh := &genericHeap[T]{}
	heap.Init(gh)
	return &MinHeap[T]{inner: gh}
}

func (gh *MinHeap[T]) Push(offset int64, value T) *HeapItem[T] {
	item := &HeapItem[T]{Offset: offset, Value: value}
	heap.Push(gh.inner, item)
	return item
}

func (gh *MinHeap[T]) Pop() (int64, T) {
	item := heap.Pop(gh.inner).(*HeapItem[T])
	return item.Offset, item.Value
}

func (gh *MinHeap[T]) Peek() (offset int64, value T, ok bool) {
	if gh.Len() == 0 {
		return 0, value, false
	}

	item := (*gh.inner)[0]
	return item.Offset, item.Value, true
}

func (gh *MinHeap[T]) Len() int {
	return gh.inner.Len()
}
