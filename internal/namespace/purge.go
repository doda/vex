package namespace

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/vexsearch/vex/pkg/objectstore"
)

// PurgeOptions controls how namespace purge behaves.
type PurgeOptions struct {
	// KeepMeta preserves state.json and tombstone.json if true.
	KeepMeta bool
}

// PurgeNamespace deletes all objects under a namespace prefix.
// Returns the number of objects deleted.
func (m *StateManager) PurgeNamespace(ctx context.Context, namespace string, opts PurgeOptions) (int, error) {
	if m == nil || m.store == nil {
		return 0, fmt.Errorf("state manager not configured")
	}

	prefix := fmt.Sprintf("vex/namespaces/%s/", namespace)
	skipKeys := map[string]struct{}{}
	if opts.KeepMeta {
		skipKeys[StateKey(namespace)] = struct{}{}
		skipKeys[TombstoneKey(namespace)] = struct{}{}
	}

	deleted := 0
	marker := ""
	for {
		result, err := m.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return deleted, err
		}

		for _, obj := range result.Objects {
			key := strings.TrimSpace(obj.Key)
			if key == "" {
				continue
			}
			if _, skip := skipKeys[key]; skip {
				continue
			}
			if err := m.store.Delete(ctx, key); err != nil && !objectstore.IsNotFoundError(err) {
				return deleted, err
			}
			deleted++
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	extraKeys := []string{
		fmt.Sprintf("vex/namespaces/%s", namespace),
		CatalogKey(namespace),
	}
	sort.Strings(extraKeys)
	for _, key := range extraKeys {
		if key == "" {
			continue
		}
		if err := m.store.Delete(ctx, key); err != nil && !objectstore.IsNotFoundError(err) {
			return deleted, err
		}
	}

	return deleted, nil
}
