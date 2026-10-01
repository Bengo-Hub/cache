package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func TestTryLockOwnership(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	a, ok, err := TryLock(ctx, rdb, "job", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first lock: ok=%v err=%v", ok, err)
	}
	if _, ok, _ := TryLock(ctx, rdb, "job", time.Minute); ok {
		t.Fatal("second holder must not get the lock")
	}

	// A stale holder (different token) must not release the current lease.
	stale := &Lock{rdb: rdb, key: "job", token: "someone-else", ttl: time.Minute}
	_ = stale.Release(ctx)
	if v, _ := rdb.Get(ctx, "job").Result(); v != a.token {
		t.Fatal("foreign release removed the lease")
	}
	if still, _ := stale.Refresh(ctx); still {
		t.Fatal("foreign refresh must fail")
	}

	if err := a.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := TryLock(ctx, rdb, "job", time.Minute); !ok {
		t.Fatal("lock should be free after owner release")
	}
}

func TestRunExclusiveOnlyOneRuns(t *testing.T) {
	_, rdb := newTestRedis(t)
	var running, ran int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = RunExclusive(context.Background(), rdb, zap.NewNop(), "sweep", 5*time.Second, func(ctx context.Context) error {
				if atomic.AddInt32(&running, 1) > 1 {
					t.Error("two runners overlapped")
				}
				atomic.AddInt32(&ran, 1)
				time.Sleep(50 * time.Millisecond)
				atomic.AddInt32(&running, -1)
				return nil
			})
		}()
	}
	wg.Wait()
	if ran < 1 {
		t.Fatal("nobody ran")
	}
}

func TestRunExclusiveRedisDownSkips(t *testing.T) {
	mr, rdb := newTestRedis(t)
	mr.Close()
	called := false
	ran, err := RunExclusive(context.Background(), rdb, nil, "k", 5*time.Second, func(context.Context) error {
		called = true
		return nil
	})
	if ran || called || err == nil {
		t.Fatalf("expected skip with error, ran=%v called=%v err=%v", ran, called, err)
	}
}

func TestGetOrSetStaleServesStaleAndRefreshes(t *testing.T) {
	mr, rdb := newTestRedis(t)
	c := New(rdb, zap.NewNop())
	ctx := context.Background()
	var calls int32
	fetch := func(context.Context) (int, error) { return int(atomic.AddInt32(&calls, 1)), nil }

	v, _ := GetOrSetStale(ctx, c, "dash", time.Second, time.Minute, fetch)
	if v != 1 {
		t.Fatalf("miss should fetch, got %d", v)
	}
	v, _ = GetOrSetStale(ctx, c, "dash", time.Second, time.Minute, fetch)
	if v != 1 || atomic.LoadInt32(&calls) != 1 {
		t.Fatal("fresh hit must not fetch")
	}

	time.Sleep(1100 * time.Millisecond) // past ttl, inside stale window
	mr.FastForward(1100 * time.Millisecond)
	v, _ = GetOrSetStale(ctx, c, "dash", time.Second, time.Minute, fetch)
	if v != 1 {
		t.Fatalf("stale hit must return the old value immediately, got %d", v)
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&calls) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	v, _ = GetOrSetStale(ctx, c, "dash", time.Second, time.Minute, fetch)
	if v != 2 {
		t.Fatalf("background refresh should have stored 2, got %d", v)
	}

	c.Invalidate(ctx, "dash")
	if mr.Exists("dash" + staleSuffix) {
		t.Fatal("Invalidate must clear the stale copy too")
	}
}

func TestLocalEvictsLeastRecentlyUsed(t *testing.T) {
	l := NewLocal[string, int](2, time.Minute)
	l.Set("a", 1)
	l.Set("b", 2)
	l.Get("a") // a is now most recently used
	l.Set("c", 3)
	if _, ok := l.Get("b"); ok {
		t.Fatal("b was least recently used and should be evicted")
	}
	if _, ok := l.Get("a"); !ok {
		t.Fatal("a should remain")
	}
	if l.Len() != 2 {
		t.Fatalf("len %d, want 2", l.Len())
	}
}

func TestLocalExpires(t *testing.T) {
	l := NewLocal[string, int](10, 50*time.Millisecond)
	l.Set("a", 1)
	time.Sleep(80 * time.Millisecond)
	if _, ok := l.Get("a"); ok {
		t.Fatal("entry should have expired")
	}
}

func TestNewAcceptsTypedNilClient(t *testing.T) {
	var rdb *redis.Client
	c := New(rdb, nil)
	v, err := GetOrSet(context.Background(), c, "k", time.Minute, func(context.Context) (int, error) { return 7, nil })
	if err != nil || v != 7 {
		t.Fatalf("typed nil client must fall back to fetch, got %v %v", v, err)
	}
}
