// Package pool is a Go-idiomatic port of divolte-collector's
// ProcessingPool<T,E>: a fixed set of workers, each owning its own buffered
// queue, with items routed by an affinity key hash so that everything for
// one party (partyId) always lands on the same worker - preserving
// per-party ordering and letting per-worker state (like a dedupe.Memory)
// partition naturally without locking.
//
// Deliberate simplification versus the Java original: that implementation
// micro-batches (drains up to 128 items per Process() call), which was a
// JIT/GC-era throughput optimization for the JVM. Go's goroutine/channel
// model doesn't need that - this pool calls Handler once per item, in
// enqueue order per worker, which preserves the behaviorally-important
// properties (affinity, ordering, per-worker state, backpressure) without
// replicating batching as an implementation detail that doesn't carry the
// same benefit here.
package pool

import (
	"context"
	"hash/fnv"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Item is one unit of work, routed by AffinityKey (e.g. a partyId string).
type Item struct {
	AffinityKey string
	Value       interface{}
}

// Handler processes one item. WorkerIndex identifies which worker is
// calling it (0..numWorkers-1), so a Handler can maintain per-worker state
// (e.g. one dedupe.Memory per worker) via a slice indexed by WorkerIndex.
type Handler func(workerIndex int, item Item)

// logEveryNDrops bounds how often a full-queue drop is logged: logging every
// single drop is itself expensive precisely under the sustained-overload
// condition the bounded queue exists to survive, turning backpressure into
// an ever-growing log-write burden that competes with request handling.
const logEveryNDrops = 100

// Pool is a fixed set of affinity-routed worker goroutines.
type Pool struct {
	workers         []chan Item
	wg              sync.WaitGroup
	handler         Handler
	heartbeat       func(workerIndex int)
	interval        time.Duration
	bufferSize      int
	panicsRecovered atomic.Int64
	dropped         atomic.Int64
}

// Config controls pool sizing and behavior.
type Config struct {
	// Workers is the number of worker goroutines (and queues). Must be >= 1.
	Workers int
	// BufferSize is the per-worker queue depth. When full, Enqueue drops
	// the item (matching the Java original's bounded-queue-with-drop
	// behavior under sustained overload) and returns false.
	BufferSize int
	// HeartbeatInterval, if > 0, calls Heartbeat on each worker on this
	// cadence when the worker has been idle (no items to process) - useful
	// for periodic retry/reconnect logic in a downstream sink. Optional.
	HeartbeatInterval time.Duration
	// Heartbeat is called periodically per HeartbeatInterval, if set.
	Heartbeat func(workerIndex int)
}

// New starts a Pool with the given handler and config. Call Stop to drain
// and shut down.
func New(cfg Config, handler Handler) *Pool {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.BufferSize < 1 {
		cfg.BufferSize = 1024
	}
	p := &Pool{
		workers:    make([]chan Item, cfg.Workers),
		handler:    handler,
		heartbeat:  cfg.Heartbeat,
		interval:   cfg.HeartbeatInterval,
		bufferSize: cfg.BufferSize,
	}
	for i := range p.workers {
		p.workers[i] = make(chan Item, cfg.BufferSize)
		p.wg.Add(1)
		go p.run(i)
	}
	return p
}

// NumWorkers returns the configured worker count.
func (p *Pool) NumWorkers() int { return len(p.workers) }

func (p *Pool) run(workerIndex int) {
	defer p.wg.Done()
	ch := p.workers[workerIndex]

	if p.heartbeat == nil || p.interval <= 0 {
		// No heartbeat configured - simple drain loop.
		for item := range ch {
			p.callHandler(workerIndex, item)
		}
		return
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case item, ok := <-ch:
			if !ok {
				return
			}
			p.callHandler(workerIndex, item)
		case <-ticker.C:
			p.heartbeat(workerIndex)
		}
	}
}

// callHandler invokes the handler for one item with panic recovery, so that
// a single bad item (e.g. a malformed request, or a live-published mapping
// rule that panics on unexpected input shape) drops just that item instead
// of taking down the worker's goroutine - which in Go would otherwise crash
// the entire process, not just that worker.
func (p *Pool) callHandler(workerIndex int, item Item) {
	defer func() {
		if r := recover(); r != nil {
			p.panicsRecovered.Add(1)
			log.Printf("pool: worker %d: recovered panic processing item for affinity key %q: %v\n%s",
				workerIndex, item.AffinityKey, r, debug.Stack())
		}
	}()
	p.handler(workerIndex, item)
}

// PanicsRecovered returns the total number of item-processing panics
// recovered so far, for monitoring/alerting.
func (p *Pool) PanicsRecovered() int64 {
	return p.panicsRecovered.Load()
}

// Enqueue routes item to a worker by hashing AffinityKey, and sends
// without blocking - if that worker's queue is full, the item is dropped
// (logged) and Enqueue returns false, matching the bounded-queue backpressure
// behavior of the original under sustained overload.
func (p *Pool) Enqueue(item Item) bool {
	idx := workerIndexFor(item.AffinityKey, len(p.workers))
	select {
	case p.workers[idx] <- item:
		return true
	default:
		n := p.dropped.Add(1)
		if n%logEveryNDrops == 1 {
			log.Printf("pool: worker %d queue full (capacity %d), dropping item for affinity key %q (%d total drops so far)", idx, p.bufferSize, item.AffinityKey, n)
		}
		return false
	}
}

// Dropped returns the total number of items dropped so far due to a full
// worker queue, for monitoring/alerting.
func (p *Pool) Dropped() int64 {
	return p.dropped.Load()
}

func workerIndexFor(affinityKey string, numWorkers int) int {
	if numWorkers <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(affinityKey))
	return int(h.Sum32() % uint32(numWorkers))
}

// Stop closes all worker queues (no more items accepted - callers must stop
// calling Enqueue first) and waits for in-flight items to drain, up to
// ctx's deadline. Mirrors the original's "stop upstream before downstream"
// shutdown ordering: callers should Stop an upstream pool (e.g. the
// mapping/dedupe stage) before stopping a downstream one (e.g. a Kafka
// sink), so nothing is stranded mid-pipeline.
func (p *Pool) Stop(ctx context.Context) error {
	for _, ch := range p.workers {
		close(ch)
	}
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
