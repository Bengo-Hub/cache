package cache

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// staleSuffix marks the key GetOrSetStale writes, so it never collides with a plain GetOrSet
// value under the same logical key, and so Invalidate/InvalidatePattern clear both.
const staleSuffix = "|swr"

type staleEnvelope[T any] struct {
	V          T     `json:"v"`
	FreshUntil int64 `json:"f"` // unix millis
}

// refreshing tracks keys this process is already refreshing in the background.
var refreshing sync.Map

// GetOrSetStale is stale-while-revalidate for expensive reads such as dashboard aggregates.
//
//   - fresh hit (younger than ttl): returned as is.
//   - stale hit (older than ttl, younger than ttl+staleFor): returned immediately, and ONE
//     refresh runs in the background across the whole fleet (a short Redis NX marker picks the
//     pod, a process-local marker stops repeat goroutines), so a burst of dashboard loads never
//     turns into a burst of heavy queries.
//   - miss: fetched once per process (single-flight) and stored.
//
// Callers see at most ttl+staleFor old data; bust it with Invalidate on the write path when the
// data must be exact.
func GetOrSetStale[T any](ctx context.Context, c *Aside, key string, ttl, staleFor time.Duration, fetch func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	if c == nil || c.rdb == nil {
		return fetch(ctx)
	}
	skey := key + staleSuffix

	raw, err := c.rdb.Get(ctx, skey).Bytes()
	if err == nil {
		var env staleEnvelope[T]
		if json.Unmarshal(raw, &env) == nil {
			if time.Now().UnixMilli() >= env.FreshUntil {
				c.refreshInBackground(ctx, skey, ttl, staleFor, func(ctx context.Context) (any, error) {
					return fetch(ctx)
				})
			}
			return env.V, nil
		}
	} else if !errors.Is(err, redis.Nil) {
		c.logger.Warn("cache get failed", zap.String("key", skey), zap.Error(err))
	}

	shared, err := misses.do(skey, func() (any, error) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		v, err := fetch(fctx)
		if err != nil {
			return nil, err
		}
		c.storeStale(fctx, skey, v, ttl, staleFor)
		return v, nil
	})
	if err != nil {
		return zero, err
	}
	v, ok := shared.(T)
	if !ok {
		return fetch(ctx)
	}
	return v, nil
}

func (c *Aside) refreshInBackground(ctx context.Context, skey string, ttl, staleFor time.Duration, fetch func(context.Context) (any, error)) {
	if _, busy := refreshing.LoadOrStore(skey, struct{}{}); busy {
		return
	}
	// Only one pod refreshes: the marker lives for one fetch timeout at most.
	ok, err := c.rdb.SetNX(ctx, skey+":refresh", "1", fetchTimeout).Result()
	if err != nil || !ok {
		refreshing.Delete(skey)
		return
	}
	go func() {
		defer refreshing.Delete(skey)
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		defer c.rdb.Del(fctx, skey+":refresh")
		v, err := fetch(fctx)
		if err != nil {
			c.logger.Warn("background cache refresh failed", zap.String("key", skey), zap.Error(err))
			return
		}
		c.storeStale(fctx, skey, v, ttl, staleFor)
	}()
}

func (c *Aside) storeStale(ctx context.Context, skey string, v any, ttl, staleFor time.Duration) {
	b, err := json.Marshal(struct {
		V          any   `json:"v"`
		FreshUntil int64 `json:"f"`
	}{V: v, FreshUntil: time.Now().Add(ttl).UnixMilli()})
	if err != nil {
		return
	}
	if len(b) > MaxValueBytes {
		c.logger.Warn("cache value too large, not cached", zap.String("key", skey), zap.Int("bytes", len(b)))
		return
	}
	if err := c.rdb.Set(ctx, skey, b, ttl+staleFor).Err(); err != nil {
		c.logger.Warn("cache set failed", zap.String("key", skey), zap.Error(err))
	}
}

// isNilClient reports whether rdb is nil or a typed nil pointer wrapped in the interface (a
// service passing a nil *redis.Client when Redis is not configured).
func isNilClient(rdb redis.UniversalClient) bool {
	if rdb == nil {
		return true
	}
	v := reflect.ValueOf(rdb)
	return v.Kind() == reflect.Ptr && v.IsNil()
}
