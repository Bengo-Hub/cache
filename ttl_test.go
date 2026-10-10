package cache

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestGetOrSetTTLUsesFetchedLifetime(t *testing.T) {
	mr, rdb := newTestRedis(t)
	c := New(rdb, zap.NewNop())
	ctx := context.Background()
	calls := 0
	fetch := func(ttl time.Duration) func(context.Context) (string, time.Duration, error) {
		return func(context.Context) (string, time.Duration, error) {
			calls++
			return "quote", ttl, nil
		}
	}

	v, err := GetOrSetTTL(ctx, c, "k1", fetch(90*time.Second))
	if err != nil || v != "quote" || calls != 1 {
		t.Fatalf("first call: %v %v %d", v, err, calls)
	}
	if got := mr.TTL("k1"); got != 90*time.Second {
		t.Fatalf("stored TTL = %v, want 90s", got)
	}
	if v, _ = GetOrSetTTL(ctx, c, "k1", fetch(time.Minute)); v != "quote" || calls != 1 {
		t.Fatalf("second call should hit the cache (calls %d)", calls)
	}

	// TTL 0: returned but never cached.
	_, _ = GetOrSetTTL(ctx, c, "k2", fetch(0))
	_, _ = GetOrSetTTL(ctx, c, "k2", fetch(0))
	if mr.Exists("k2") || calls != 3 {
		t.Fatalf("zero TTL must not cache (exists %v, calls %d)", mr.Exists("k2"), calls)
	}

	// Plain GetOrSet still works through the same path.
	if v, err := GetOrSet(ctx, c, "k3", time.Minute, func(context.Context) (int, error) { return 7, nil }); err != nil || v != 7 || mr.TTL("k3") != time.Minute {
		t.Fatalf("GetOrSet regression: %v %v ttl %v", v, err, mr.TTL("k3"))
	}

	// Without Redis the value is fetched every time.
	if v, err := GetOrSetTTL(ctx, nil, "k4", fetch(time.Minute)); err != nil || v != "quote" {
		t.Fatalf("nil cache: %v %v", v, err)
	}
}
