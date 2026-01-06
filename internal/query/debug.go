package query

import (
	"log"
	"os"
	"sync/atomic"
)

var queryDebugFlag int32 = -1

func queryDebugEnabled() bool {
	current := atomic.LoadInt32(&queryDebugFlag)
	if current >= 0 {
		return current == 1
	}
	if os.Getenv("VEX_QUERY_DEBUG") == "1" {
		atomic.StoreInt32(&queryDebugFlag, 1)
	} else {
		atomic.StoreInt32(&queryDebugFlag, 0)
	}
	return atomic.LoadInt32(&queryDebugFlag) == 1
}

func debugLogf(format string, args ...any) {
	if !queryDebugEnabled() {
		return
	}
	log.Printf(format, args...)
}
