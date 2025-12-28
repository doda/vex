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
	idxr "github.com/vexsearch/vex/internal/indexer"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
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

	// Initialize object store
	store, err := objectstore.New(objectstore.Config{
		Type:      cfg.ObjectStore.Type,
		Endpoint:  cfg.ObjectStore.Endpoint,
		Bucket:    cfg.ObjectStore.Bucket,
		AccessKey: cfg.ObjectStore.AccessKey,
		SecretKey: cfg.ObjectStore.SecretKey,
		Region:    cfg.ObjectStore.Region,
		UseSSL:    cfg.ObjectStore.UseSSL,
	})
	if err != nil {
		log.Fatalf("Failed to create object store: %v", err)
	}

	// Initialize state manager and indexer
	stateManager := namespace.NewStateManager(store)
	indexerConfig := idxr.DefaultConfig()
	if cfg.Indexer.WriteWALVersion != 0 {
		indexerConfig.WriteWALVersion = cfg.Indexer.WriteWALVersion
	}
	if cfg.Indexer.WriteManifestVersion != 0 {
		indexerConfig.WriteManifestVersion = cfg.Indexer.WriteManifestVersion
	}
	if err := indexerConfig.ValidateVersionConfig(); err != nil {
		log.Fatalf("Invalid indexer format version config: %v", err)
	}

	indexer := idxr.New(store, stateManager, indexerConfig, nil)

	// Start the indexer
	indexer.Start()
	fmt.Println("Indexer started, watching for namespaces...")

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	_ = ctx

	<-done
	fmt.Println("\nShutting down indexer...")
	indexer.Stop()
	cancel()
	fmt.Println("Indexer stopped")
}
