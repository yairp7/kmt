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
var ErrAlreadyResolved = errors.New("offset is already resolved")

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
	nextOffset           int64
	uncommittedOffsets   []int64
}

func New(startOffset int64) *EventsTrackerImpl {
	return &EventsTrackerImpl{
		offsets:              make(map[int64]*Event),
		offsetsHeap:          NewMinHeap[*Event](),
		resolvedOffsetCount:  0,
		committerOffsetCount: -1,
		nextOffset:           startOffset,
		uncommittedOffsets:   make([]int64, 0),
	}
}

func (et *EventsTrackerImpl) Push(offset int64, event Event) {
	et.lock.Lock()
	defer et.lock.Unlock()
	et.unsafePush(offset, &event)
}

func (et *EventsTrackerImpl) unsafePush(offset int64, event *Event) {
	if offset < et.nextOffset {
		return
	}

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
		isResolved := e.Status == StatusDLQ || e.Status == StatusSuccess
		if isResolved && (status == StatusPending || status == StatusFailed) {
			return ErrAlreadyResolved
		}

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
		offset, event, _ := et.offsetsHeap.Peek()

		if offset != et.nextOffset || event.Status == StatusPending || event.Status == StatusFailed {
			break
		}

		et.offsetsHeap.Pop()
		delete(et.offsets, offset)
		offsets = append(offsets, offset)
		et.nextOffset++
	}

	return offsets
}

func (et *EventsTrackerImpl) resolvedCount() int64 {
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

	commitIfNeeded := func() (bool, error) {
		currentResolvedCount := et.resolvedCount()
		if len(et.uncommittedOffsets) == 0 && et.committerOffsetCount == currentResolvedCount {
			return false, nil
		}

		et.uncommittedOffsets = append(et.uncommittedOffsets, et.PopResolved()...)
		if len(et.uncommittedOffsets) == 0 {
			return false, nil
		}

		if err := commitFunc(et.uncommittedOffsets); err != nil {
			return true, err
		}

		et.uncommittedOffsets = make([]int64, 0)
		et.committerOffsetCount = currentResolvedCount

		return true, nil
	}

	for {
		select {
		case <-ctx.Done():
			_, err := commitIfNeeded()
			return errors.Join(ctx.Err(), err)
		case <-ticker.C:
			_, _ = commitIfNeeded()
		}
	}
}
