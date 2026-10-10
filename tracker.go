package kmt

import (
	"context"
	"errors"
	"sync"
	"time"
)

type EventsTrackerStatus int

const (
	StatusPending EventsTrackerStatus = iota
	StatusFailed
	StatusSuccess
	StatusDLQ
)

var ErrMissingOffset = errors.New("offset is missing")

type Event struct {
	Status EventsTrackerStatus
}

type EventsTracker[E any] interface {
	Push(offset int64, event E)
	PopResolved() []int64
	MarkResolved(offset int64, status EventsTrackerStatus) error
}

type EventsTrackerImpl struct {
	offsets              map[int64]*Event
	offsetsHeap          *MinHeap[*Event]
	lock                 sync.RWMutex
	resolvedOffsetCount  int64
	committerOffsetCount int64
}

func New() *EventsTrackerImpl {
	return &EventsTrackerImpl{
		offsets:              make(map[int64]*Event),
		offsetsHeap:          NewMinHeap[*Event](),
		resolvedOffsetCount:  0,
		committerOffsetCount: -1,
	}
}

func (et *EventsTrackerImpl) Push(offset int64, event Event) {
	et.lock.Lock()
	defer et.lock.Unlock()
	et.unsafePush(offset, &event)
}

func (et *EventsTrackerImpl) unsafePush(offset int64, event *Event) {
	if _, ok := et.offsets[offset]; ok {
		return
	}

	et.offsets[offset] = event
	et.offsetsHeap.Push(offset, event)
}

func (et *EventsTrackerImpl) MarkResolved(offset int64, status EventsTrackerStatus) error {
	et.lock.Lock()
	defer et.lock.Unlock()

	if e, ok := et.offsets[offset]; ok {
		e.Status = status

		if status == StatusDLQ || status == StatusSuccess {
			et.resolvedOffsetCount++
		}

		return nil
	}

	return ErrMissingOffset
}

func (et *EventsTrackerImpl) PopResolved() []int64 {
	et.lock.Lock()
	defer et.lock.Unlock()

	offsets := make([]int64, 0)

	for et.offsetsHeap.Len() > 0 {
		_, event, _ := et.offsetsHeap.Peek()

		if event.Status == StatusPending || event.Status == StatusFailed {
			break
		}

		offset, _ := et.offsetsHeap.Pop()
		delete(et.offsets, offset)
		offsets = append(offsets, offset)
	}

	return offsets
}

func (et *EventsTrackerImpl) ResolvedCount() int64 {
	et.lock.RLock()
	defer et.lock.RUnlock()
	return et.resolvedOffsetCount
}

func (et *EventsTrackerImpl) StartCommitter(
	ctx context.Context,
	interval time.Duration,
	commitFunc func(offsets []int64) error,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentResolvedCount := et.ResolvedCount()
			if et.committerOffsetCount == currentResolvedCount {
				continue
			}

			offsets := et.PopResolved()
			if len(offsets) == 0 {
				continue
			}

			if err := commitFunc(offsets); err != nil {
				return err
			}

			et.committerOffsetCount = currentResolvedCount
		}
	}
}
