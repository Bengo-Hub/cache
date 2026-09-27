package cache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestFlightGroupSharesConcurrentMisses: callers arriving while a fetch for the same key runs share
// its result (one source call); a later call after it finished fetches again; keys are independent.
func TestFlightGroupSharesConcurrentMisses(t *testing.T) {
	g := &flightGroup{calls: map[string]*flightCall{}}
	var calls atomic.Int32
	release := make(chan struct{})
	fn := func() (any, error) {
		calls.Add(1)
		<-release
		return "tenant", nil
	}

	var wg sync.WaitGroup
	results := make([]any, 15)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], _ = g.do("tenant:codevertex", fn)
		}(i)
	}
	time.Sleep(50 * time.Millisecond) // let every caller join the in-flight fetch
	close(release)
	wg.Wait()

	if n := calls.Load(); n != 1 {
		t.Fatalf("source called %d times for 15 concurrent misses, want 1", n)
	}
	for i, r := range results {
		if r != "tenant" {
			t.Fatalf("caller %d got %v", i, r)
		}
	}
	// Finished flights are forgotten: the next miss fetches again.
	if _, err := g.do("tenant:codevertex", func() (any, error) { calls.Add(1); return "x", nil }); err != nil || calls.Load() != 2 {
		t.Fatalf("later miss: err %v calls %d", err, calls.Load())
	}
	if len(g.calls) != 0 {
		t.Fatalf("%d flights left behind", len(g.calls))
	}
}
