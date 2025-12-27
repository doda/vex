package main

import (
	"fmt"
	"os"

	"github.com/vexsearch/vex/cmd/vex/indexer"
	"github.com/vexsearch/vex/cmd/vex/query"
	"github.com/vexsearch/vex/cmd/vex/serve"
	"github.com/vexsearch/vex/cmd/vex/version"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		serve.Run(os.Args[2:])
	case "query":
		query.Run(os.Args[2:])
	case "indexer":
		indexer.Run(os.Args[2:])
	case "version":
		version.Run()
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`vex - Object-Storage-First Search Engine

Usage:
  vex <command> [options]

Commands:
  serve     Start the all-in-one server (query + indexer)
  query     Start the query node only
  indexer   Start the indexer node only
  version   Print version information
  help      Show this help message

Run 'vex <command> --help' for more information on a command.`)
}
