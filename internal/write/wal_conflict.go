package write

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var errWALContentMismatch = errors.New("existing WAL content does not match")

const walRepairTimeout = 30 * time.Second

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

func wrapWALConflict(ctx context.Context, store objectstore.Store, stateMan *namespace.StateManager, ns string, cause error) error {
	if !errors.Is(cause, errWALContentMismatch) {
		return fmt.Errorf("%w: %v", wal.ErrWALSeqConflict, cause)
	}

	repairCtx, cancel := context.WithTimeout(ctx, walRepairTimeout)
	defer cancel()

	log.Printf("[wal_conflict] WAL content mismatch for namespace %s; attempting repair", ns)
	repairer := wal.NewRepairer(store, stateMan)
	result, repairErr := repairer.Repair(repairCtx, ns)
	if repairErr != nil {
		log.Printf("[wal_conflict] Repair failed for namespace %s: %v", ns, repairErr)
		return fmt.Errorf("%w: %v (repair failed: %v)", wal.ErrWALSeqConflict, cause, repairErr)
	}
	if result != nil && result.NewHeadSeq != result.OriginalHeadSeq {
		log.Printf("[wal_conflict] Repair advanced head_seq for %s from %d to %d", ns, result.OriginalHeadSeq, result.NewHeadSeq)
		return fmt.Errorf("%w: %v (repair advanced head_seq from %d to %d; retry)", wal.ErrWALSeqConflict, cause, result.OriginalHeadSeq, result.NewHeadSeq)
	}
	log.Printf("[wal_conflict] Repair completed for %s with no head_seq change", ns)
	return fmt.Errorf("%w: %v (repair attempted; retry)", wal.ErrWALSeqConflict, cause)
}
