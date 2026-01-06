package index

import (
	"log"
	"os"
	"sync/atomic"
)

var indexDebugFlag int32 = -1

func indexDebugEnabled() bool {
	current := atomic.LoadInt32(&indexDebugFlag)
	if current >= 0 {
		return current == 1
	}
	if os.Getenv("VEX_INDEX_DEBUG") == "1" || os.Getenv("VEX_QUERY_DEBUG") == "1" {
		atomic.StoreInt32(&indexDebugFlag, 1)
	} else {
		atomic.StoreInt32(&indexDebugFlag, 0)
	}
	return atomic.LoadInt32(&indexDebugFlag) == 1
}

func indexDebugf(format string, args ...any) {
	if !indexDebugEnabled() {
		return
	}
	log.Printf(format, args...)
}
