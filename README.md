# kmt

A small, dependency-free Go library for tracking the processing state of events by offset, so you can safely commit offsets **in order** even when events finish **out of order**.

This is the typical problem when consuming from a log (Kafka-style) with concurrent workers: worker 3 may finish before worker 1, but you can only commit up to the highest *contiguous* completed offset. `kmt` keeps that bookkeeping for you.

```
import "github.com/yairp7/kmt"
```

Requires Go 1.22+. Standard library only.

## How it works

Each event is registered by its offset with a status. Offsets are kept in a min-heap, so the lowest offset is always at the head. `PopResolved` removes and returns offsets from the head **only while they are resolved and contiguous**; it stops at the first `Pending` or `Failed` offset, or at a gap where the next offset hasn't been pushed yet. Anything behind a blocked head stays in the tracker until the head is resolved.

The tracker remembers the next offset it expects to pop. Set it with `New(WithStartOffset(n))`, typically the partition's committed offset + 1. Without it, the tracker starts from the lowest offset pushed before the first pop.

| Status          | Meaning                                        | Blocks commit? | Counts as resolved? |
|-----------------|------------------------------------------------|----------------|---------------------|
| `StatusPending` | Not processed yet                              | Yes            | No                  |
| `StatusFailed`  | Failed, expected to be retried and re-marked   | Yes            | No                  |
| `StatusSuccess` | Processed successfully                         | No             | Yes                 |
| `StatusDLQ`     | Given up on and sent to a dead-letter queue    | No             | Yes                 |

## Usage

```go
et := kmt.New()

// Register events as they are consumed.
et.Push(1, kmt.Event{Status: kmt.StatusPending})
et.Push(2, kmt.Event{Status: kmt.StatusPending})
et.Push(3, kmt.Event{Status: kmt.StatusPending})

// Workers finish in any order.
_ = et.MarkResolved(3, kmt.StatusSuccess)
_ = et.MarkResolved(2, kmt.StatusSuccess)

et.PopResolved() // [] — offset 1 is still pending

_ = et.MarkResolved(1, kmt.StatusSuccess)
et.PopResolved() // [1 2 3]
```

### Background committer

`StartCommitter` polls on an interval, pops the contiguous resolved prefix, and hands it to your commit callback. It blocks until the context is cancelled, so run it in its own goroutine. If the callback returns an error, the same offsets, plus any resolved since, are passed again on the next tick. On cancellation it makes one final commit of whatever is still uncommitted, so `commitFunc` may be called after `ctx` is done.

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

go func() {
    err := et.StartCommitter(ctx, 500*time.Millisecond, func(offsets []int64) error {
        // e.g. commit offsets[len(offsets)-1] to your broker
        if err := commit(offsets); err != nil {
            log.Printf("commit failed, will retry: %v", err)
            return err
        }
        return nil
    })
    // err always wraps ctx.Err(); it also wraps the final commit's error if that failed.
    log.Printf("committer stopped: %v", err)
}()
```

## API

| Symbol | Description |
|--------|-------------|
| `New(opts ...Option) *EventsTrackerImpl` | Creates an empty tracker. |
| `WithStartOffset(offset int64) Option` | Sets the first offset the tracker will pop. |
| `Push(offset int64, event Event)` | Registers an offset. Pushing an offset that is already tracked, already popped, or below the start offset is ignored. |
| `MarkResolved(offset int64, status EventsTrackerStatus) error` | Sets an offset's status. Returns `ErrMissingOffset` if the offset isn't tracked. A failed offset can be re-marked later (e.g. after a retry). |
| `PopResolved() []int64` | Removes and returns the contiguous resolved offsets from the head, in ascending order. Returns an empty slice if the head is pending/failed. |
| `StartCommitter(ctx, interval, commitFunc) error` | Periodic commit loop described above. |
| `MinHeap[T]` | Generic offset-keyed min-heap (`Push`, `Pop`, `Peek`, `Len`) used internally; also exported. |

`EventsTrackerImpl` is safe for concurrent use by multiple goroutines.

## Compatibility

Works directly with queues that expose per-partition `int64` offsets and a cumulative commit: Apache Kafka, Redpanda, NATS JetStream and Azure Event Hubs. Use one tracker per partition.

## Behavior notes

- **Ordering:** offsets are popped in ascending, contiguous order regardless of push order, so a commit never skips an offset that hasn't been pushed yet.
- **Concurrent pushes:** if offsets can be pushed out of order (e.g. each worker pushes its own), use `WithStartOffset`. Otherwise the very first pop can happen before the lowest offset is pushed.
- **Offset gaps:** every offset must be pushed for the tracker to advance past it. If the log has gaps (e.g. Kafka compaction or transaction markers), push the missing offsets as `StatusSuccess`.
- **Failed events block progress** until they are re-marked `Success` or `DLQ`. If you want to skip a poison message, mark it `StatusDLQ`.
- **Commit errors are retried:** if `commitFunc` returns an error, the committer keeps the batch and passes it again on the next tick, extended with any offsets resolved since. The loop does not stop on commit errors, so log or alert from inside `commitFunc`. While the broker is unavailable the retried batch keeps growing. The batch is kept on the tracker, so if the committer stops with it still uncommitted, the next `StartCommitter` on the same tracker retries it first.
- **Shutdown flush:** when `ctx` is cancelled, `StartCommitter` commits any uncommitted offsets once more before returning. The returned error wraps `ctx.Err()` and, if that final commit failed, the commit error too, so check it with `errors.Is`.
- Run only one `StartCommitter` per tracker.

## Development

```sh
go test ./...
go test -race ./...
go test -bench=. -benchmem ./...
```

Tests live in the external `kmt_test` package and exercise only the public API.

## Benchmarks

Selected results on an Apple M1 (darwin/arm64, 8 threads), Go 1.26.5, via `go test -bench=. -benchmem`. Numbers are indicative; rerun on your own hardware.

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| Push | 232.3 | 126 | 2 |
| MarkResolved | 107.6 | 0 | 0 |
| PopResolved, head blocked, heap=100,000 | 19.66 | 0 | 0 |
| PopResolved, drain batch=1,000 | 104,279 | 25,216 | 12 |
| Push + Mark + Pop cycle, backlog=100,000 | 344.1 | 32 | 3 |
| Push, parallel | 362.8 | 109 | 2 |

Takeaways:
- Checking a blocked head is O(1) (~20 ns) regardless of heap size, so polling a stalled tracker is cheap.
- Draining costs roughly 100 ns per offset.
- Parallel writers are slower per op than serial because they contend on a single mutex.

## Layout

```
tracker.go             # tracker, statuses, committer
heap.go                # generic offset-keyed min-heap
heap_test.go           # heap unit tests
tracker_test.go        # tracker unit tests
tracker_bench_test.go  # benchmarks
```
