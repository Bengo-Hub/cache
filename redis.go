package cache

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig is the one way services build their Redis client, so pool sizes and timeouts
// are the same fleet-wide instead of go-redis defaults (3s read timeouts that let a stalled
// Redis hold a request for seconds).
type RedisConfig struct {
	// URL is a redis:// or rediss:// URL. When set it wins over Addr/Password/DB.
	URL      string
	Addr     string
	Username string // Redis 6+ ACL user; empty uses the default user
	Password string
	DB       int
	TLS      bool

	PoolSize     int           // default 10 per CPU (go-redis default), set for heavy services
	MinIdleConns int           // default 2
	DialTimeout  time.Duration // default 2s
	ReadTimeout  time.Duration // default 500ms
	WriteTimeout time.Duration // default 500ms
}

// NewRedis builds a client from cfg and pings it. It returns the concrete *redis.Client, which
// satisfies redis.UniversalClient everywhere this module accepts one. A ping failure is returned together with the
// client: callers that treat Redis as optional can log it and keep the client (it reconnects on
// its own), callers that require Redis can fail startup.
func NewRedis(ctx context.Context, cfg RedisConfig) (*redis.Client, error) {
	var opts *redis.Options
	if cfg.URL != "" {
		parsed, err := redis.ParseURL(cfg.URL)
		if err != nil {
			return nil, fmt.Errorf("parse redis url: %w", err)
		}
		opts = parsed
	} else {
		if strings.TrimSpace(cfg.Addr) == "" {
			return nil, fmt.Errorf("redis address not configured")
		}
		opts = &redis.Options{Addr: cfg.Addr, Username: cfg.Username, Password: cfg.Password, DB: cfg.DB}
		if cfg.TLS {
			opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
	}
	// Our Redis 7 has no CLIENT MAINT_NOTIFICATIONS / client-side-cache identity support; the
	// identity handshake only produces noisy errors on every new connection.
	opts.DisableIdentity = true
	opts.PoolSize = cfg.PoolSize
	opts.MinIdleConns = orInt(cfg.MinIdleConns, 2)
	opts.DialTimeout = orDur(cfg.DialTimeout, 2*time.Second)
	opts.ReadTimeout = orDur(cfg.ReadTimeout, 500*time.Millisecond)
	opts.WriteTimeout = orDur(cfg.WriteTimeout, 500*time.Millisecond)
	opts.PoolTimeout = opts.ReadTimeout + time.Second

	rdb := redis.NewClient(opts)
	pctx, cancel := context.WithTimeout(ctx, opts.DialTimeout)
	defer cancel()
	if err := rdb.Ping(pctx).Err(); err != nil {
		return rdb, fmt.Errorf("redis ping: %w", err)
	}
	return rdb, nil
}

func orInt(v, d int) int {
	if v > 0 {
		return v
	}
	return d
}

func orDur(v, d time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return d
}
