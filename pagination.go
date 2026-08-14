package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	querytypes "github.com/cosmos/cosmos-sdk/types/query"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// delegationsUnsupported latches once the node refuses to serve
// ValidatorDelegations because of its store-scan limit. Retrying that query
// every refresh is actively harmful: each attempt makes the node scan tens of
// thousands of store entries and starves concurrent queries, so the metric is
// dropped for the lifetime of the process instead.
var delegationsUnsupported atomic.Bool

// firstPageKey starts a key-based scan from the very beginning of the store:
// it sorts before any real entry, so the iterator behaves like an unkeyed one
// while still selecting the node's key-pagination code path.
var firstPageKey = []byte{0x00}

// pageFetcher runs one paginated gRPC query and returns the page items plus
// the pagination response carrying the next key.
type pageFetcher[T any] func(ctx context.Context, page *querytypes.PageRequest) ([]T, *querytypes.PageResponse, error)

// isScanLimitError reports whether the node refused an offset-mode query
// because it would scan too many store entries. sei-cosmos enforces this for
// queries that filter over a large store (e.g. ValidatorDelegations) and asks
// the client to switch to key-based pagination.
func isScanLimitError(err error) bool {
	if status.Code(err) != codes.InvalidArgument && status.Code(err) != codes.Internal {
		return false
	}
	return strings.Contains(err.Error(), "scanned more than")
}

// paginateAll walks every page of a paginated query and returns the combined
// result.
//
// The first page is requested the plain way, exactly as before; if the node
// rejects it with a scan-limit error, the whole walk is retried using
// key-based pagination, which such nodes serve without scanning the entire
// store up front.
//
// maxItems caps the total number of collected items (0 means no cap) so a
// validator with a very large delegator set cannot stall a scrape.
func paginateAll[T any](ctx context.Context, pageLimit uint64, maxItems int, fetch pageFetcher[T]) ([]T, error) {
	results, err := paginateFrom(ctx, nil, pageLimit, maxItems, fetch)
	if err == nil {
		return results, nil
	}
	if !isScanLimitError(err) {
		return nil, err
	}

	log.Debug().Err(err).Msg("Node rejected offset pagination, retrying with key-based pagination")
	return paginateFrom(ctx, firstPageKey, pageLimit, maxItems, fetch)
}

// minPageLimit is the smallest page size worth trying: below this the number
// of round trips outweighs any chance of the node accepting the page.
const minPageLimit = 10

func paginateFrom[T any](ctx context.Context, startKey []byte, pageLimit uint64, maxItems int, fetch pageFetcher[T]) ([]T, error) {
	if pageLimit == 0 {
		pageLimit = 100
	}

	var (
		results []T
		nextKey = startKey
	)

	for {
		limit := pageLimit
		if maxItems > 0 {
			remaining := maxItems - len(results)
			if remaining <= 0 {
				return results, nil
			}
			if uint64(remaining) < limit {
				limit = uint64(remaining)
			}
		}

		items, pageResponse, err := fetch(ctx, &querytypes.PageRequest{
			Key:   nextKey,
			Limit: limit,
		})
		// A node that caps store scans cannot fill a large page when matching
		// entries are sparse. Shrink the page and retry; the smaller size
		// sticks for the rest of the walk.
		if err != nil && isScanLimitError(err) && pageLimit > minPageLimit {
			pageLimit /= 10
			if pageLimit < minPageLimit {
				pageLimit = minPageLimit
			}
			log.Debug().
				Uint64("page-limit", pageLimit).
				Msg("Node could not fill the page within its scan limit, retrying with a smaller page")
			continue
		}
		if err != nil {
			return nil, err
		}

		results = append(results, items...)

		key := pageResponse.GetNextKey()
		if len(key) == 0 {
			return results, nil
		}
		if bytes.Equal(key, nextKey) {
			return nil, fmt.Errorf("node returned the same pagination key twice (%d items collected)", len(results))
		}
		nextKey = key
	}
}
