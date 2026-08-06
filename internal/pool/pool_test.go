package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSameAffinityKeyAlwaysRoutesToSameWorker(t *testing.T) {
	for _, key := range []string{"party-1", "party-2", "abc", "", "unicode-🎉"} {
		first := workerIndexFor(key, 8)
		for i := 0; i < 100; i++ {
			if got := workerIndexFor(key, 8); got != first {
				t.Fatalf("workerIndexFor(%q) inconsistent: %d != %d", key, got, first)
			}
		}
	}
}

func TestPerKeyOrderingIsPreserved(t *testing.T) {
	var mu sync.Mutex
	var seen []int

	const n = 500
	p := New(Config{Workers: 4, BufferSize: n}, func(workerIndex int, item Item) {
		mu.Lock()
		seen = append(seen, item.Value.(int))
		mu.Unlock()
	})

	for i := 0; i < n; i++ {
		if !p.Enqueue(Item{AffinityKey: "same-party", Value: i}) {
			t.Fatalf("Enqueue dropped item %d unexpectedly", i)
		}
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if len(seen) != n {
		t.Fatalf("processed %d items, want %d", len(seen), n)
	}
	for i, v := range seen {
		if v != i {
			t.Fatalf("ordering violated at index %d: got %d, want %d", i, v, i)
		}
	}
}

func TestEnqueueDropsWhenWorkerQueueFull(t *testing.T) {
	block := make(chan struct{})
	var processed int32

	p := New(Config{Workers: 1, BufferSize: 2}, func(workerIndex int, item Item) {
		<-block // hold every item until the test releases it
		atomic.AddInt32(&processed, 1)
	})

	// First item is picked up immediately and blocks in the handler; the
	// next 2 fill the buffered channel; anything after that should drop.
	accepted := 0
	dropped := 0
	for i := 0; i < 10; i++ {
		if p.Enqueue(Item{AffinityKey: "k", Value: i}) {
			accepted++
		} else {
			dropped++
		}
	}
	close(block)
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if dropped == 0 {
		t.Error("expected at least one dropped item when the queue fills up")
	}
	if int(processed) != accepted {
		t.Errorf("processed %d, accepted %d - should match", processed, accepted)
	}
}

func TestHeartbeatFiresWhenIdle(t *testing.T) {
	var beats int32
	p := New(Config{
		Workers:           1,
		BufferSize:        10,
		HeartbeatInterval: 10 * time.Millisecond,
		Heartbeat: func(workerIndex int) {
			atomic.AddInt32(&beats, 1)
		},
	}, func(workerIndex int, item Item) {})

	time.Sleep(60 * time.Millisecond)
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if atomic.LoadInt32(&beats) == 0 {
		t.Error("expected at least one heartbeat while idle")
	}
}

func TestStopRespectsContextDeadline(t *testing.T) {
	block := make(chan struct{})
	p := New(Config{Workers: 1, BufferSize: 1}, func(workerIndex int, item Item) {
		<-block // never returns before the test releases it
	})
	p.Enqueue(Item{AffinityKey: "k", Value: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := p.Stop(ctx)
	close(block) // release the stuck handler so the goroutine doesn't leak past the test
	if err == nil {
		t.Error("expected Stop to time out while a handler is stuck")
	}
}
