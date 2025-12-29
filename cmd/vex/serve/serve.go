package serve

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/logging"
	"github.com/vexsearch/vex/internal/membership"
	"github.com/vexsearch/vex/internal/routing"
	"github.com/vexsearch/vex/internal/warmer"
	"github.com/vexsearch/vex/pkg/api"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func Run(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to config file")
	addr := fs.String("addr", "", "Listen address (overrides config)")
	fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if *addr != "" {
		cfg.ListenAddr = *addr
	}

	// Initialize cluster routing with static membership from config
	clusterRouter := routing.New(cfg.RoutingAddr())
	membershipProvider := membership.NewFromConfig(cfg.Membership)
	membershipMgr := membership.NewManager(membershipProvider, clusterRouter)

	if err := membershipMgr.Start(); err != nil {
		log.Fatalf("Failed to start membership manager: %v", err)
	}

	// Initialize object store from config
	var store objectstore.Store
	if cfg.ObjectStore.Type != "" {
		var err error
		store, err = objectstore.New(objectstore.Config{
			Type:      cfg.ObjectStore.Type,
			Endpoint:  cfg.ObjectStore.Endpoint,
			Bucket:    cfg.ObjectStore.Bucket,
			AccessKey: cfg.ObjectStore.AccessKey,
			SecretKey: cfg.ObjectStore.SecretKey,
			Region:    cfg.ObjectStore.Region,
			UseSSL:    cfg.ObjectStore.UseSSL,
			RootPath:  cfg.ObjectStore.RootPath,
		})
		if err != nil {
			log.Fatalf("Failed to initialize object store: %v", err)
		}
		fmt.Printf("Connected to object store: %s at %s\n", cfg.ObjectStore.Type, cfg.ObjectStore.Endpoint)
	}

	logger := logging.New()
	router := api.NewRouterWithLogger(cfg, clusterRouter, membershipMgr, logger)

	var diskCache *cache.DiskCache
	var ramCache *cache.MemoryCache
	if store != nil {
		diskCfg := cache.DiskCacheConfig{
			RootPath:  cfg.Cache.NVMePath,
			BudgetPct: cfg.Cache.BudgetPct,
		}
		if cfg.Cache.NVMESizeGB > 0 {
			diskCfg.MaxBytes = int64(cfg.Cache.NVMESizeGB) * 1024 * 1024 * 1024
		}
		var err error
		diskCache, err = cache.NewDiskCache(diskCfg)
		if err != nil {
			log.Fatalf("Failed to initialize disk cache: %v", err)
		}

		if cfg.Cache.RAMSizeMB > 0 {
			ramCache = cache.NewMemoryCache(cache.MemoryCacheConfig{
				MaxBytes:      int64(cfg.Cache.RAMSizeMB) * 1024 * 1024,
				DefaultCapPct: cfg.Cache.RAMNamespaceCapPct,
			})
		}

		router.SetDiskCache(diskCache)
		router.SetRAMCache(ramCache)
		if err := router.SetStore(store); err != nil {
			log.Fatalf("Failed to initialize server store: %v", err)
		}

		if diskCache != nil || ramCache != nil {
			cacheWarmer := warmer.New(store, router.StateManager(), diskCache, ramCache, warmer.DefaultConfig())
			router.SetCacheWarmer(cacheWarmer)
		}
	}

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		fmt.Printf("Starting vex server on %s\n", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-done
	fmt.Println("\nShutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Shutdown error: %v", err)
	}

	// Close router resources (batcher, write handler, tail store)
	if err := router.Close(); err != nil {
		log.Printf("Router close error: %v", err)
	}

	membershipMgr.Stop()

	fmt.Println("Server stopped")
}
