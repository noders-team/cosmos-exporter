package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestCache(clock *fakeClock) *MetricsCache {
	cache := NewMetricsCache(30*time.Second, 5*time.Second)
	cache.now = clock.Now
	return cache
}

func serveKey(cache *MetricsCache, key string, fetch fetchFunc) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics/test", nil)
	cache.Serve(rec, req, key, fetch)
	return rec
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestCacheServesFreshEntryWithoutRefetch(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	cache := newTestCache(clock)

	var calls atomic.Int64
	fetch := func(ctx context.Context) ([]byte, error) {
		calls.Add(1)
		return []byte("metrics v1"), nil
	}

	first := serveKey(cache, "general", fetch)
	if first.Code != http.StatusOK || first.Body.String() != "metrics v1" {
		t.Fatalf("first response: code=%d body=%q", first.Code, first.Body.String())
	}

	clock.Advance(10 * time.Second)
	second := serveKey(cache, "general", fetch)
	if second.Body.String() != "metrics v1" {
		t.Fatalf("second response body = %q", second.Body.String())
	}
	if calls.Load() != 1 {
		t.Errorf("fetch called %d times, want 1", calls.Load())
	}
}

func TestCacheStaleServesOldAndRefreshesInBackground(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	cache := newTestCache(clock)

	var calls atomic.Int64
	fetch := func(ctx context.Context) ([]byte, error) {
		n := calls.Add(1)
		return []byte(fmt.Sprintf("metrics v%d", n)), nil
	}

	serveKey(cache, "general", fetch)
	clock.Advance(31 * time.Second)

	stale := serveKey(cache, "general", fetch)
	if stale.Body.String() != "metrics v1" {
		t.Fatalf("stale response body = %q, want old value", stale.Body.String())
	}

	waitFor(t, func() bool { return calls.Load() == 2 })
	waitFor(t, func() bool {
		return serveKey(cache, "general", fetch).Body.String() == "metrics v2"
	})
	if calls.Load() != 2 {
		t.Errorf("fetch called %d times, want 2", calls.Load())
	}
}

func TestCacheConcurrentStaleRequestsTriggerSingleRefresh(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	cache := newTestCache(clock)

	var calls atomic.Int64
	release := make(chan struct{})
	fetch := func(ctx context.Context) ([]byte, error) {
		if calls.Add(1) > 1 {
			<-release
		}
		return []byte("metrics"), nil
	}

	serveKey(cache, "general", fetch)
	clock.Advance(31 * time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			serveKey(cache, "general", fetch)
		}()
	}
	wg.Wait()
	close(release)
	waitFor(t, func() bool { return calls.Load() == 2 })

	// Give any extra spurious refreshes a moment to fire before asserting.
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 2 {
		t.Errorf("fetch called %d times, want 2 (initial + one refresh)", calls.Load())
	}
}

func TestCacheFirstFetchErrorReturns500ThenRecovers(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	cache := newTestCache(clock)

	var fail atomic.Bool
	fail.Store(true)
	fetch := func(ctx context.Context) ([]byte, error) {
		if fail.Load() {
			return nil, fmt.Errorf("node unreachable")
		}
		return []byte("metrics ok"), nil
	}

	errResp := serveKey(cache, "general", fetch)
	if errResp.Code != http.StatusInternalServerError {
		t.Fatalf("error response code = %d, want 500", errResp.Code)
	}

	fail.Store(false)
	okResp := serveKey(cache, "general", fetch)
	if okResp.Code != http.StatusOK || okResp.Body.String() != "metrics ok" {
		t.Fatalf("recovery response: code=%d body=%q", okResp.Code, okResp.Body.String())
	}
}

func TestCacheRefreshErrorKeepsServingStale(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	cache := newTestCache(clock)

	var calls atomic.Int64
	fetch := func(ctx context.Context) ([]byte, error) {
		if calls.Add(1) > 1 {
			return nil, fmt.Errorf("node down")
		}
		return []byte("metrics v1"), nil
	}

	serveKey(cache, "general", fetch)
	clock.Advance(31 * time.Second)
	serveKey(cache, "general", fetch)
	waitFor(t, func() bool { return calls.Load() == 2 })

	resp := serveKey(cache, "general", fetch)
	if resp.Code != http.StatusOK || resp.Body.String() != "metrics v1" {
		t.Fatalf("stale-after-error response: code=%d body=%q", resp.Code, resp.Body.String())
	}
}

func TestCacheKeysAreIndependent(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	cache := newTestCache(clock)

	fetchA := func(ctx context.Context) ([]byte, error) { return []byte("A"), nil }
	fetchB := func(ctx context.Context) ([]byte, error) { return []byte("B"), nil }

	if got := serveKey(cache, "validator?address=a", fetchA).Body.String(); got != "A" {
		t.Errorf("key A body = %q", got)
	}
	if got := serveKey(cache, "validator?address=b", fetchB).Body.String(); got != "B" {
		t.Errorf("key B body = %q", got)
	}
}
