// Package cache provides a cache-aside helper backed by Redis.
// It serialises values as JSON and handles TTL, invalidation, and
// transparent cache misses so callers only need to supply a fetch function.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// Aside implements the cache-aside (lazy-loading) pattern.
type Aside struct {
	rdb    *redis.Client
	logger *zap.Logger
}

// New creates a new cache-aside helper.
func New(rdb *redis.Client, logger *zap.Logger) *Aside {
	return &Aside{rdb: rdb, logger: logger.Named("cache")}
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

	// Cache miss — fetch from source.
	v, err := fetch(ctx)
	if err != nil {
		return zero, err
	}

	// Store in cache (best-effort).
	if b, jsonErr := json.Marshal(v); jsonErr == nil {
		if setErr := c.rdb.Set(ctx, key, b, ttl).Err(); setErr != nil {
			c.logger.Warn("cache set failed", zap.String("key", key), zap.Error(setErr))
		}
	}

	return v, nil
}

// Invalidate deletes one or more keys. Use for mutation-triggered invalidation.
func (c *Aside) Invalidate(ctx context.Context, keys ...string) {
	if c == nil || c.rdb == nil || len(keys) == 0 {
		return
	}
	if err := c.rdb.Del(ctx, keys...).Err(); err != nil {
		c.logger.Warn("cache invalidate failed", zap.Strings("keys", keys), zap.Error(err))
	}
}

// InvalidatePattern deletes all keys matching a glob pattern (e.g. "inv:items:tenant-1:*").
func (c *Aside) InvalidatePattern(ctx context.Context, pattern string) {
	if c == nil || c.rdb == nil {
		return
	}
	var cursor uint64
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			c.logger.Warn("cache scan failed", zap.String("pattern", pattern), zap.Error(err))
			return
		}
		if len(keys) > 0 {
			c.rdb.Del(ctx, keys...)
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
