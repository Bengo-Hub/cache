# cache (github.com/Bengo-Hub/cache)

Shared caching, locking and Redis client helpers for every Go service.

## What lives where

| Need | Use | Memory bound |
|---|---|---|
| Shared read cache visible to every pod | `GetOrSet` (Redis, cache-aside) | Redis `maxmemory 3gb` + `allkeys-lru`; every key has a TTL |
| Expensive aggregate (dashboards, reports) | `GetOrSetStale` (stale-while-revalidate) | same, TTL = ttl + staleFor |
| Per-pod cache (API keys, settings, compiled templates, clients) | `NewLocal[K,V](maxEntries, ttl)` | hard cap on entries, least recently used evicted, expired entries purged |
| Run a scheduled job on one pod only | `RunExclusive` | lease key with TTL |
| Build the Redis client | `NewRedis(ctx, RedisConfig)` | pool + 500ms timeouts |

## Eviction and invalidation

- **Add**: `GetOrSet` writes `SET key value EX ttl` after a miss. Concurrent misses in one pod share
  one fetch (single-flight); the fetch runs detached from the first caller's cancellation with a
  60s cap. Values over 1 MB are returned but not cached, so one huge report cannot push out
  thousands of hot keys.
- **Expire**: Redis expires keys at their TTL (TTL tiers: `TTLReference` 5m, `TTLModerate` 1m,
  `TTLOperational` 30s).
- **Evict under pressure**: Redis runs `maxmemory-policy allkeys-lru`, so when full it drops the
  least recently used keys of any kind. Everything here tolerates that: a lost cache key is a miss,
  a lost lease ends the job (its context is cancelled), and rate-limit state falls back locally.
- **Invalidate**: `Invalidate(ctx, keys...)` deletes keys (and their stale-while-revalidate copies).
  `InvalidatePattern(ctx, "inv:items:<tenant>:*")` walks with `SCAN` and frees with `UNLINK`, on
  every master for a cluster client.
- **Per-pod caches** (`Local`) cannot be invalidated from another pod by themselves. When a write
  must be seen everywhere immediately, publish an `events.Broadcaster` message and `Delete` in the
  handler; the TTL is the backstop.

## Locks

`TryLock`/`RunExclusive` use a random owner token: release and refresh are Lua compare-and-act, so
a slow job can never delete or extend the next holder's lease. `RunExclusive` renews every ttl/3
and cancels the job if the lease is lost. When Redis is down it does not run the job and returns
`ErrLockUnavailable` (skipping one tick of a scheduled side effect is safer than every pod doing it).

Do not use session `pg_try_advisory_lock` through PgBouncer for this: in transaction pooling the
lock and unlock can land on different backends.

## Versions

- v0.5.0: `UniversalClient`, `GetOrSetStale`, `Local` (bounded LRU), `TryLock`/`RunExclusive`,
  `NewRedis`, value-size guard, detached single-flight, `UNLINK` pattern invalidation, logo byte cache.
- v0.4.1: single-flight concurrent misses.
