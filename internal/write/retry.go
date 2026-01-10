package write

import (
	"errors"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
)

const maxWALConflictRetries = 1

func isRetryableWALConflict(err error) bool {
	return errors.Is(err, wal.ErrWALSeqConflict) || errors.Is(err, namespace.ErrWALSeqNotMonotonic)
}
