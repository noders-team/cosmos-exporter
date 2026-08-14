package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	querytypes "github.com/cosmos/cosmos-sdk/types/query"
)

// seiLikePager emulates sei-cosmos FilteredPaginate: offset-mode requests fail
// once the scan exceeds maxScan entries without filling the page, while
// key-mode requests are served page by page.
type seiLikePager struct {
	totalItems int
	maxScan    int
	calls      int
	offsetUsed bool
}

func (p *seiLikePager) fetch(ctx context.Context, page *querytypes.PageRequest) ([]string, *querytypes.PageResponse, error) {
	p.calls++

	if len(page.Key) == 0 && p.totalItems > p.maxScan {
		p.offsetUsed = true
		return nil, nil, status.Errorf(codes.InvalidArgument,
			"scanned more than %d entries without filling the page; use key-based pagination instead", p.maxScan)
	}

	start := 0
	if len(page.Key) > 0 && !bytes.Equal(page.Key, firstPageKey) {
		if _, err := fmt.Sscanf(string(page.Key), "item-%d", &start); err != nil {
			return nil, nil, fmt.Errorf("bad key %q", page.Key)
		}
	}

	end := start + int(page.Limit)
	if end > p.totalItems {
		end = p.totalItems
	}

	items := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		items = append(items, fmt.Sprintf("item-%d", i))
	}

	response := &querytypes.PageResponse{}
	if end < p.totalItems {
		response.NextKey = []byte(fmt.Sprintf("item-%d", end))
	}
	return items, response, nil
}

func TestPaginateAllCollectsEveryPage(t *testing.T) {
	pager := &seiLikePager{totalItems: 2500, maxScan: 10000}

	items, err := paginateAll(context.Background(), 1000, 0, pager.fetch)
	if err != nil {
		t.Fatalf("paginateAll failed: %v", err)
	}
	if len(items) != 2500 {
		t.Errorf("collected %d items, want 2500", len(items))
	}
	if pager.calls != 3 {
		t.Errorf("made %d requests, want 3 pages", pager.calls)
	}
	if items[0] != "item-0" || items[2499] != "item-2499" {
		t.Errorf("unexpected boundaries: first=%q last=%q", items[0], items[2499])
	}
}

func TestPaginateAllRecoversFromSeiScanLimit(t *testing.T) {
	// 50k delegations: the offset-mode probe trips Sei's scan limit and the
	// walk must transparently fall back to key-based pagination.
	pager := &seiLikePager{totalItems: 50_000, maxScan: 10_000}

	items, err := paginateAll(context.Background(), 1000, 0, pager.fetch)
	if err != nil {
		t.Fatalf("paginateAll failed on Sei-like node: %v", err)
	}
	if !pager.offsetUsed {
		t.Error("expected the first attempt to use offset mode (standard-chain path)")
	}
	if len(items) != 50_000 {
		t.Errorf("collected %d items, want 50000", len(items))
	}
}

func TestPaginateAllKeepsOffsetModeOnStandardChains(t *testing.T) {
	// A chain without a scan limit must never see a key-mode request.
	var sawKey bool
	fetch := func(ctx context.Context, page *querytypes.PageRequest) ([]string, *querytypes.PageResponse, error) {
		if len(page.Key) > 0 {
			sawKey = true
		}
		return []string{"a", "b"}, &querytypes.PageResponse{}, nil
	}

	if _, err := paginateAll(context.Background(), 1000, 0, fetch); err != nil {
		t.Fatalf("paginateAll failed: %v", err)
	}
	if sawKey {
		t.Error("key-based pagination was used on a chain that accepted offset mode")
	}
}

func TestPaginateAllRespectsMaxItems(t *testing.T) {
	pager := &seiLikePager{totalItems: 50_000, maxScan: 10_000}

	items, err := paginateAll(context.Background(), 1000, 2500, pager.fetch)
	if err != nil {
		t.Fatalf("paginateAll failed: %v", err)
	}
	if len(items) != 2500 {
		t.Errorf("collected %d items, want the 2500 cap", len(items))
	}
}

func TestPaginateAllPropagatesRealErrors(t *testing.T) {
	failing := func(ctx context.Context, page *querytypes.PageRequest) ([]string, *querytypes.PageResponse, error) {
		return nil, nil, fmt.Errorf("node unreachable")
	}

	if _, err := paginateAll(context.Background(), 1000, 0, failing); err == nil {
		t.Fatal("expected an error to propagate")
	}
}

func TestPaginateAllStopsOnRepeatedKey(t *testing.T) {
	// A node that keeps returning the same NextKey must not loop forever.
	stuck := func(ctx context.Context, page *querytypes.PageRequest) ([]string, *querytypes.PageResponse, error) {
		return []string{"x"}, &querytypes.PageResponse{NextKey: []byte("same")}, nil
	}

	if _, err := paginateAll(context.Background(), 10, 0, stuck); err == nil {
		t.Fatal("expected an error when the node repeats the same page key")
	}
}
