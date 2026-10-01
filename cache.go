// Package cache provides a cache-aside helper backed by Redis.
// It serialises values as JSON and handles TTL, invalidation, and
// transparent cache misses so callers only need to supply a fetch function.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Standard TTL tiers — pick the constant that matches the data's volatility.
const (
	TTLReference  = 5 * time.Minute  // semi-static: categories, zones, plans, features
	TTLModerate   = 1 * time.Minute  // moderate-change: items, stock, fleet
	TTLOperational = 30 * time.Second // active data: order lists, task lists
)

// MaxValueBytes is the largest serialized value GetOrSet/GetOrSetStale will store. Redis runs
// with maxmemory + allkeys-lru, so one oversized report would push thousands of small hot keys
// out; such values are returned to the caller but not cached.
const MaxValueBytes = 1 << 20

// fetchTimeout bounds a single-flight fetch. The fetch runs detached from the first caller's
// cancellation (other callers are waiting on it) but must still end.
const fetchTimeout = 60 * time.Second

// Aside implements the cache-aside (lazy-loading) pattern.
type Aside struct {
	rdb    redis.UniversalClient
	logger *zap.Logger
}

// New creates a new cache-aside helper. Accepts *redis.Client, *redis.ClusterClient or a
// failover client.
func New(rdb redis.UniversalClient, logger *zap.Logger) *Aside {
	if logger == nil {
		logger = zap.NewNop()
	}
	if isNilClient(rdb) {
		rdb = nil
	}
	return &Aside{rdb: rdb, logger: logger.Named("cache")}
}

// Client returns the underlying Redis client (nil when caching is disabled).
func (c *Aside) Client() redis.UniversalClient {
	if c == nil {
		return nil
	}
	return c.rdb
}

// GetOrSet returns the cached value for key if present, otherwise calls fetch,
// stores the result with the given TTL, and returns it.
// T must be JSON-serialisable.
func GetOrSet[T any](ctx context.Context, c *Aside, key string, ttl time.Duration, fetch func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	if c == nil || c.rdb == nil {
		return fetch(ctx)
	}

	// Try cache first.
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err == nil {
		var v T
		if unmarshalErr := json.Unmarshal(raw, &v); unmarshalErr == nil {
			return v, nil
		} else {
			// Corrupted entry — fall through to fetch.
			c.logger.Warn("cache unmarshal failed, refetching", zap.String("key", key), zap.Error(unmarshalErr))
		}
	} else if !errors.Is(err, redis.Nil) {
		// Redis error — log and fall through to fetch.
		c.logger.Warn("cache get failed", zap.String("key", key), zap.Error(err))
	}

	// Cache miss — fetch from source. Concurrent misses on the same key in this process share
	// one fetch (single-flight), so a cold key under load costs the source one call, not one per
	// request (seen live: 15 identical tenant lookups in the same second).
	// The fetch runs on a context detached from this caller's cancellation: other requests
	// may be waiting on the same flight, and one client disconnecting must not fail them all.
	shared, err := misses.do(key, func() (any, error) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		v, err := fetch(fctx)
		if err != nil {
			return nil, err
		}
		c.store(fctx, key, v, ttl)
		return v, nil
	})
	if err != nil {
		return zero, err
	}
	v, ok := shared.(T)
	if !ok {
		// Same key used with two value types: never share across them.
		return fetch(ctx)
	}
	return v, nil
}

// store writes v under key with ttl, best effort, skipping values over MaxValueBytes.
func (c *Aside) store(ctx context.Context, key string, v any, ttl time.Duration) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	if len(b) > MaxValueBytes {
		c.logger.Warn("cache value too large, not cached", zap.String("key", key), zap.Int("bytes", len(b)))
		return
	}
	if err := c.rdb.Set(ctx, key, b, ttl).Err(); err != nil {
		c.logger.Warn("cache set failed", zap.String("key", key), zap.Error(err))
	}
}

// flightGroup runs at most one fetch per key at a time; callers arriving while it runs wait for
// and share its result. A minimal local form of golang.org/x/sync/singleflight.
type flightGroup struct {
	mu    sync.Mutex
	calls map[string]*flightCall
}

type flightCall struct {
	done chan struct{}
	val  any
	err  error
}

var misses = &flightGroup{calls: map[string]*flightCall{}}

func (g *flightGroup) do(key string, fn func() (any, error)) (any, error) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		<-c.done
		return c.val, c.err
	}
	c := &flightCall{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
		close(c.done)
	}()
	c.val, c.err = fn()
	return c.val, c.err
}

// Invalidate deletes one or more keys. Use for mutation-triggered invalidation.
func (c *Aside) Invalidate(ctx context.Context, keys ...string) {
	if c == nil || c.rdb == nil || len(keys) == 0 {
		return
	}
	all := make([]string, 0, len(keys)*2)
	for _, k := range keys {
		all = append(all, k, k+staleSuffix)
	}
	if err := c.rdb.Del(ctx, all...).Err(); err != nil {
		c.logger.Warn("cache invalidate failed", zap.Strings("keys", keys), zap.Error(err))
	}
}

// InvalidatePattern deletes all keys matching a glob pattern (e.g. "inv:items:tenant-1:*").
// It walks the keyspace with SCAN (non-blocking, unlike KEYS) and deletes with UNLINK so large
// values are freed off the Redis main thread. A trailing "*" also matches the stale-while-
// revalidate copies written by GetOrSetStale. On a cluster client it scans every master.
func (c *Aside) InvalidatePattern(ctx context.Context, pattern string) {
	if c == nil || c.rdb == nil {
		return
	}
	if cc, ok := c.rdb.(*redis.ClusterClient); ok {
		_ = cc.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			c.scanDelete(ctx, node, pattern)
			return nil
		})
		return
	}
	c.scanDelete(ctx, c.rdb, pattern)
}

func (c *Aside) scanDelete(ctx context.Context, rdb redis.Cmdable, pattern string) {
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, pattern, 500).Result()
		if err != nil {
			c.logger.Warn("cache scan failed", zap.String("pattern", pattern), zap.Error(err))
			return
		}
		if len(keys) > 0 {
			if err := rdb.Unlink(ctx, keys...).Err(); err != nil {
				c.logger.Warn("cache unlink failed", zap.String("pattern", pattern), zap.Error(err))
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
}

// Key builds a namespaced cache key from parts: Key("inv","items","tenant-1","page-2") → "inv:items:tenant-1:page-2".
func Key(parts ...string) string {
	if len(parts) == 0 {
		return ""
	}
	key := parts[0]
	for _, p := range parts[1:] {
		key += ":" + p
	}
	return key
}

// FormatPage returns a page identifier for cache key construction.
func FormatPage(page, limit int) string {
	return fmt.Sprintf("p%d-l%d", page, limit)
}
