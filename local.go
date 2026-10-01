package cache

import (
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// Local is a bounded, thread-safe in-process cache with per-entry TTL and least-recently-used
// eviction. Use it for every cache that lives in a pod's memory.
//
// Why: hand-rolled map caches across the fleet only skipped expired entries on read and never
// removed them, so they grew with every distinct key until the pod ran out of memory (and one
// had no mutex at all). Local caps the entry count (the least recently used entry is dropped
// when full) and purges expired entries in the background.
//
// Data in Local is per pod. When a write must be visible on every pod at once, pair it with
// an events.Broadcaster invalidation, or use the Redis-backed Aside instead.
type Local[K comparable, V any] struct {
	lru *expirable.LRU[K, V]
}

// NewLocal creates a Local holding at most maxEntries entries, each living at most ttl.
// maxEntries <= 0 defaults to 1000; ttl <= 0 defaults to TTLReference.
func NewLocal[K comparable, V any](maxEntries int, ttl time.Duration) *Local[K, V] {
	if maxEntries <= 0 {
		maxEntries = 1000
	}
	if ttl <= 0 {
		ttl = TTLReference
	}
	return &Local[K, V]{lru: expirable.NewLRU[K, V](maxEntries, nil, ttl)}
}

// Get returns the cached value and marks it recently used.
func (l *Local[K, V]) Get(key K) (V, bool) { return l.lru.Get(key) }

// Set stores value, evicting the least recently used entry when full.
func (l *Local[K, V]) Set(key K, value V) { l.lru.Add(key, value) }

// Delete removes key.
func (l *Local[K, V]) Delete(key K) { l.lru.Remove(key) }

// Purge removes every entry.
func (l *Local[K, V]) Purge() { l.lru.Purge() }

// Len reports the current entry count.
func (l *Local[K, V]) Len() int { return l.lru.Len() }

// GetOrLoad returns the cached value or calls load and caches its result. Errors are not
// cached. Concurrent loads of one key may both run; use it for cheap-to-repeat loads.
func (l *Local[K, V]) GetOrLoad(key K, load func() (V, error)) (V, error) {
	if v, ok := l.lru.Get(key); ok {
		return v, nil
	}
	v, err := load()
	if err != nil {
		return v, err
	}
	l.lru.Add(key, v)
	return v, nil
}
