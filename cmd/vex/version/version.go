package version

import "fmt"

var (
	Version   = "dev"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

func Run() {
	fmt.Printf("vex version %s\n", Version)
	fmt.Printf("  commit: %s\n", GitCommit)
	fmt.Printf("  built:  %s\n", BuildTime)
}
