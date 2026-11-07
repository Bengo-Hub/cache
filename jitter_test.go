package cache

import (
	"testing"
	"time"
)

func TestJitterStaysWithinBoundsAndSpreads(t *testing.T) {
	ttl := 6 * time.Hour
	seen := map[time.Duration]bool{}
	for i := 0; i < 500; i++ {
		got := Jitter(ttl, 0.1)
		if got < ttl || got > ttl+36*time.Minute {
			t.Fatalf("Jitter(6h, 0.1) = %v, want within [6h, 6h36m]", got)
		}
		seen[got] = true
	}
	if len(seen) < 400 {
		t.Fatalf("expected spread-out values, got only %d distinct out of 500", len(seen))
	}
}

func TestJitterEdgeCases(t *testing.T) {
	cases := []struct {
		name     string
		ttl      time.Duration
		fraction float64
		want     time.Duration
	}{
		{"zero ttl unchanged", 0, 0.1, 0},
		{"negative ttl unchanged", -time.Second, 0.1, -time.Second},
		{"zero fraction unchanged", time.Hour, 0, time.Hour},
		{"negative fraction unchanged", time.Hour, -1, time.Hour},
		{"tiny ttl unchanged", time.Nanosecond, 0.1, time.Nanosecond},
	}
	for _, c := range cases {
		if got := Jitter(c.ttl, c.fraction); got != c.want {
			t.Errorf("%s: Jitter(%v, %v) = %v, want %v", c.name, c.ttl, c.fraction, got, c.want)
		}
	}
	for i := 0; i < 100; i++ {
		if got := Jitter(time.Hour, 5); got < time.Hour || got > 2*time.Hour {
			t.Fatalf("fraction above 1 must clamp to 1: got %v", got)
		}
	}
}
