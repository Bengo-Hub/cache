package cache

import (
	"math/rand/v2"
	"time"
)

// Jitter returns ttl plus a random extra of up to fraction*ttl, so entries written at
// the same moment (every service caching the same tenant after a deploy wave or a node
// reboot) expire at different times and do not refetch in the same second. fraction is
// clamped to [0, 1]; a ttl of zero or less is returned unchanged.
//
// Added 2026-11-07 after ~15 services fetched the same tenant from auth-api within one
// second and met a brief upstream gap there.
func Jitter(ttl time.Duration, fraction float64) time.Duration {
	if ttl <= 0 || fraction <= 0 {
		return ttl
	}
	if fraction > 1 {
		fraction = 1
	}
	max := int64(float64(ttl) * fraction)
	if max <= 0 {
		return ttl
	}
	return ttl + time.Duration(rand.Int64N(max+1))
}
