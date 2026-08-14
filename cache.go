package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

// goSafe запускает горутину сбора метрик с recover: паника (например, из
// SDK на неожиданных данных ноды) не должна ронять весь экспортер.
func goSafe(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Error().
					Interface("panic", r).
					Bytes("stack", debug.Stack()).
					Msg("Recovered from panic in metrics collection goroutine")
			}
		}()
		fn()
	}()
}

const metricsContentType = "text/plain; version=0.0.4; charset=utf-8"

// fetchFunc collects metrics and returns them rendered in Prometheus text format.
type fetchFunc func(ctx context.Context) ([]byte, error)

type cacheEntry struct {
	ready      chan struct{} // closed once the initial fetch completes
	body       []byte        // nil if no successful fetch yet
	err        error
	fetchedAt  time.Time
	refreshing bool
}

// MetricsCache serves rendered metrics with stale-while-revalidate semantics:
// a fresh entry is served as-is, a stale entry is served immediately while a
// single background refresh runs, so the node sees at most one collection per
// key per TTL regardless of how many Prometheus instances scrape.
type MetricsCache struct {
	mu      sync.Mutex
	entries map[string]*cacheEntry
	ttl     time.Duration
	timeout time.Duration
	now     func() time.Time
}

func NewMetricsCache(ttl, timeout time.Duration) *MetricsCache {
	return &MetricsCache{
		entries: make(map[string]*cacheEntry),
		ttl:     ttl,
		timeout: timeout,
		now:     time.Now,
	}
}

func (c *MetricsCache) Serve(w http.ResponseWriter, r *http.Request, key string, fetch fetchFunc) {
	c.mu.Lock()
	entry := c.entries[key]

	if entry != nil {
		select {
		case <-entry.ready:
			// A failed fetch with nothing cached is retried synchronously,
			// but not more often than once per TTL — otherwise every scrape
			// against a down node would block for the full timeout.
			if entry.body == nil && c.now().Sub(entry.fetchedAt) >= c.ttl {
				delete(c.entries, key)
				entry = nil
			}
		default:
			// Initial fetch is in flight — wait for it instead of duplicating.
			c.mu.Unlock()
			<-entry.ready
			c.serveEntry(w, entry)
			return
		}
	}

	if entry == nil {
		entry = &cacheEntry{ready: make(chan struct{})}
		c.entries[key] = entry
		c.mu.Unlock()

		body, err := c.runFetch(fetch)

		c.mu.Lock()
		entry.body = body
		entry.err = err
		entry.fetchedAt = c.now()
		close(entry.ready)
		c.mu.Unlock()

		c.serveEntry(w, entry)
		return
	}

	if entry.body != nil && c.now().Sub(entry.fetchedAt) >= c.ttl && !entry.refreshing {
		entry.refreshing = true
		go c.refresh(key, entry, fetch)
	}
	c.mu.Unlock()

	c.serveEntry(w, entry)
}

func (c *MetricsCache) refresh(key string, entry *cacheEntry, fetch fetchFunc) {
	body, err := c.runFetch(fetch)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		log.Warn().Err(err).Str("cache-key", key).Msg("Background metrics refresh failed, keeping stale data")
	} else {
		entry.body = body
		entry.err = nil
	}
	// Advance fetchedAt even on failure so an unreachable node is retried
	// once per TTL instead of on every scrape.
	entry.fetchedAt = c.now()
	entry.refreshing = false
}

func (c *MetricsCache) runFetch(fetch fetchFunc) (body []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error().
				Interface("panic", r).
				Bytes("stack", debug.Stack()).
				Msg("Recovered from panic during metrics collection")
			body, err = nil, fmt.Errorf("panic during metrics collection: %v", r)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	return fetch(ctx)
}

func (c *MetricsCache) serveEntry(w http.ResponseWriter, entry *cacheEntry) {
	c.mu.Lock()
	body, err := entry.body, entry.err
	c.mu.Unlock()

	if body == nil {
		http.Error(w, fmt.Sprintf("failed to collect metrics: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", metricsContentType)
	if _, err := w.Write(body); err != nil {
		log.Warn().Err(err).Msg("Failed to write metrics response")
	}
}

// renderRegistry encodes all metrics of a registry into Prometheus text format.
func renderRegistry(registry *prometheus.Registry) ([]byte, error) {
	families, err := registry.Gather()
	if err != nil {
		return nil, fmt.Errorf("failed to gather metrics: %w", err)
	}

	var buf bytes.Buffer
	encoder := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		if err := encoder.Encode(family); err != nil {
			return nil, fmt.Errorf("failed to encode metric family: %w", err)
		}
	}
	return buf.Bytes(), nil
}
