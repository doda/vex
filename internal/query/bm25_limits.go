package query

import (
	"os"
	"strconv"
	"sync/atomic"
)

var bm25MaxSegmentsFlag int32 = -1
var bm25LoadConcurrencyFlag int32 = -1

func bm25MaxSegments() int {
	current := atomic.LoadInt32(&bm25MaxSegmentsFlag)
	if current >= 0 {
		return int(current)
	}

	raw := os.Getenv("VEX_BM25_MAX_SEGMENTS")
	if raw == "" {
		atomic.StoreInt32(&bm25MaxSegmentsFlag, 0)
		return 0
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		value = 0
	}

	atomic.StoreInt32(&bm25MaxSegmentsFlag, int32(value))
	return value
}

func bm25LoadConcurrency() int {
	current := atomic.LoadInt32(&bm25LoadConcurrencyFlag)
	if current >= 0 {
		return int(current)
	}

	raw := os.Getenv("VEX_BM25_LOAD_CONCURRENCY")
	if raw == "" {
		atomic.StoreInt32(&bm25LoadConcurrencyFlag, 4)
		return 4
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		value = 1
	}

	atomic.StoreInt32(&bm25LoadConcurrencyFlag, int32(value))
	return value
}
