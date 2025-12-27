package indexer

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vexsearch/vex/internal/config"
)

func Run(args []string) {
	fs := flag.NewFlagSet("indexer", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to config file")
	fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	cfg.Mode = config.ModeIndexer

	fmt.Printf("Starting vex indexer (object store: %s)\n", cfg.ObjectStore.Endpoint)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// TODO: Start indexer loop watching for WAL changes
	_ = ctx

	<-done
	fmt.Println("\nShutting down indexer...")
	cancel()
	fmt.Println("Indexer stopped")
}
