package cache

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Lock is a Redis lease held by one process. It replaces two broken patterns found fleet-wide:
// SETNX + plain DEL (a job that outlives its TTL deletes the NEXT holder's lock) and session
// pg_try_advisory_lock through PgBouncer transaction pooling (lock and unlock can land on
// different backends, leaking the lock or letting a second pod in).
//
// Each lock carries a random owner token; Release and Refresh only act when the token still
// matches, so a holder can never remove or extend somebody else's lease.
type Lock struct {
	rdb   redis.UniversalClient
	key   string
	token string
	ttl   time.Duration
}

// ErrLockUnavailable means Redis could not be reached, so the lock state is unknown.
var ErrLockUnavailable = errors.New("cache: lock backend unavailable")

var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`)

var refreshScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`)

// TryLock takes the lease at key for ttl. It returns (nil, false, nil) when another process
// holds it, and ErrLockUnavailable when Redis cannot be reached.
func TryLock(ctx context.Context, rdb redis.UniversalClient, key string, ttl time.Duration) (*Lock, bool, error) {
	if isNilClient(rdb) {
		return nil, false, ErrLockUnavailable
	}
	token := uuid.NewString()
	ok, err := rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return nil, false, errors.Join(ErrLockUnavailable, err)
	}
	if !ok {
		return nil, false, nil
	}
	return &Lock{rdb: rdb, key: key, token: token, ttl: ttl}, true, nil
}

// Refresh extends the lease by its TTL. false means the lease was lost (expired or evicted).
func (l *Lock) Refresh(ctx context.Context) (bool, error) {
	n, err := refreshScript.Run(ctx, l.rdb, []string{l.key}, l.token, l.ttl.Milliseconds()).Int()
	return n == 1, err
}

// Release gives the lease up if this process still owns it.
func (l *Lock) Release(ctx context.Context) error {
	return releaseScript.Run(ctx, l.rdb, []string{l.key}, l.token).Err()
}

// RunExclusive runs fn on at most one process fleet-wide at a time for key.
//
// The lease is renewed every ttl/3 while fn runs, so a long job keeps it; if a renewal finds
// the lease gone, fn's context is cancelled so the job stops instead of overlapping a new
// holder. ran=false with a nil error means another pod holds it (normal for scheduled jobs
// that fire on every replica). When Redis is unreachable fn does NOT run and
// ErrLockUnavailable is returned: for scheduled side effects (emails, invoices, backups)
// skipping one tick is safer than running on every pod.
func RunExclusive(ctx context.Context, rdb redis.UniversalClient, log *zap.Logger, key string, ttl time.Duration, fn func(ctx context.Context) error) (ran bool, err error) {
	if ttl < 3*time.Second {
		ttl = 3 * time.Second
	}
	lock, ok, err := TryLock(ctx, rdb, key, ttl)
	if err != nil || !ok {
		return false, err
	}
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				still, rerr := lock.Refresh(context.WithoutCancel(ctx))
				if rerr == nil && !still {
					if log != nil {
						log.Warn("exclusive lease lost, stopping job", zap.String("key", key))
					}
					cancel()
					return
				}
			}
		}
	}()
	err = fn(jobCtx)
	close(done)
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer rcancel()
	if rerr := lock.Release(rctx); rerr != nil && log != nil {
		log.Warn("lease release failed, it will expire on its own", zap.String("key", key), zap.Error(rerr))
	}
	return true, err
}

var holdScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`)

// RunOnce runs fn at most once fleet-wide for key within hold. Use it for scheduled jobs where
// every replica wakes at the same time (top of the hour, nightly): put the period in the key
// (e.g. "auth:backup:2026100114") and pass a hold at least as long as the period.
//
// Unlike RunExclusive, a successful run keeps the key until hold expires, so a replica whose
// timer fires a moment after the first one finished cannot run the same period again. A
// failed run releases the key so another replica (or the next tick) can retry. While fn runs
// the lease is renewed like RunExclusive. Redis down: fn does not run, ErrLockUnavailable.
func RunOnce(ctx context.Context, rdb redis.UniversalClient, log *zap.Logger, key string, hold time.Duration, fn func(ctx context.Context) error) (ran bool, err error) {
	lease := hold
	if lease > 10*time.Minute {
		lease = 10 * time.Minute // renewed while running; short so a crashed holder frees fast
	}
	if lease < 3*time.Second {
		lease = 3 * time.Second
	}
	lock, ok, err := TryLock(ctx, rdb, key, lease)
	if err != nil || !ok {
		return false, err
	}
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(lease / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if still, rerr := lock.Refresh(context.WithoutCancel(ctx)); rerr == nil && !still {
					if log != nil {
						log.Warn("run-once lease lost, stopping job", zap.String("key", key))
					}
					cancel()
					return
				}
			}
		}
	}()
	err = fn(jobCtx)
	close(done)
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer rcancel()
	if err != nil {
		_ = lock.Release(rctx)
		return true, err
	}
	if herr := holdScript.Run(rctx, rdb, []string{key}, lock.token, hold.Milliseconds()).Err(); herr != nil && log != nil {
		log.Warn("run-once hold failed", zap.String("key", key), zap.Error(herr))
	}
	return true, nil
}

// PeriodKey appends the current period bucket to prefix, for use with RunOnce: every replica
// computes the same key within one period, so the job runs once per period fleet-wide no
// matter when each replica's ticker fires. Buckets are aligned in UTC (period 1h gives
// "prefix:202610011400", 6h gives 00:00/06:00/12:00/18:00, 24h gives UTC midnight).
func PeriodKey(prefix string, period time.Duration) string {
	if period <= 0 {
		period = time.Hour
	}
	return prefix + ":" + time.Now().UTC().Truncate(period).Format("200601021504")
}
