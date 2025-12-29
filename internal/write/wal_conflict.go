package write

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/vexsearch/vex/pkg/objectstore"
)

var errWALContentMismatch = errors.New("existing WAL content does not match")

func confirmExistingWAL(ctx context.Context, store objectstore.Store, key string, expected []byte) error {
	reader, _, err := store.Get(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("failed to read existing WAL: %w", err)
	}
	defer reader.Close()

	existing, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read existing WAL data: %w", err)
	}

	if !bytes.Equal(existing, expected) {
		return errWALContentMismatch
	}

	return nil
}
