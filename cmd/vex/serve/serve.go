package serve

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/indexer"
	"github.com/vexsearch/vex/internal/logging"
	"github.com/vexsearch/vex/internal/membership"
	"github.com/vexsearch/vex/internal/metrics"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/routing"
	"github.com/vexsearch/vex/internal/wal"
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
	var stopWarm func()
	var stopCompactorSync func()
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
			stopWarm = startWarmLoop(cacheWarmer, cfg.Cache)
		}

		// Start the indexer for all-in-one mode
		indexerConfig := indexer.DefaultConfig()
		idx := indexer.New(store, router.StateManager(), indexerConfig, nil)
		idx.Start()
		fmt.Println("Indexer started")
		defer func() {
			fmt.Println("Stopping indexer...")
			idx.Stop()
		}()

		// Start background compaction for indexed segments.
		lsmConfig := index.DefaultLSMConfig()
		if cfg.Compaction.MaxSegments > 0 {
			lsmConfig.MaxCompactionSegments = cfg.Compaction.MaxSegments
		}
		if cfg.Compaction.MaxBytesMB > 0 {
			lsmConfig.MaxCompactionBytes = int64(cfg.Compaction.MaxBytesMB) * 1024 * 1024
		}

		compactor := index.NewBackgroundCompactor(store, lsmConfig, nil)
		compactor.SetStateManager(router.StateManager())
		compactor.Start()
		stopCompactorSync = startCompactorSync(compactor, router.StateManager(), store, indexerConfig.NamespacePollInterval, lsmConfig)
		fmt.Println("Compactor started")
		defer func() {
			if stopCompactorSync != nil {
				stopCompactorSync()
			}
			compactor.Stop()
		}()
	}

	var repairTask *wal.RepairTask
	var repairCancel context.CancelFunc
	if store != nil && router.StateManager() != nil {
		namespaces, err := listCatalogNamespaces(context.Background(), store)
		if err != nil {
			log.Printf("WAL repair: failed to list namespaces: %v", err)
		} else if len(namespaces) > 0 {
			repairer := wal.NewRepairer(store, router.StateManager())
			repairTask = wal.NewRepairTask(repairer, 0, 0)
			repairCtx, cancel := context.WithCancel(context.Background())
			repairCancel = cancel
			for _, ns := range namespaces {
				if _, err := repairTask.RepairOnce(repairCtx, ns); err != nil {
					log.Printf("WAL repair: namespace %s: %v", ns, err)
				}
			}
			repairTask.Start(repairCtx, namespaces)
		}
	}

	queryTimeout := time.Duration(cfg.Timeout.GetQueryTimeout()) * time.Millisecond
	if queryTimeout <= 0 {
		queryTimeout = 30 * time.Second
	}
	writeTimeout := queryTimeout + 5*time.Second
	if writeTimeout < 30*time.Second {
		writeTimeout = 30 * time.Second
	}

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: writeTimeout,
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

	if repairCancel != nil {
		repairCancel()
	}
	if repairTask != nil {
		repairTask.Stop()
	}

	if stopWarm != nil {
		stopWarm()
	}

	// Close router resources (batcher, write handler, tail store)
	if err := router.Close(); err != nil {
		log.Printf("Router close error: %v", err)
	}

	membershipMgr.Stop()

	fmt.Println("Server stopped")
}

func listCatalogNamespaces(ctx context.Context, store objectstore.Store) ([]string, error) {
	if store == nil {
		return nil, nil
	}

	namespaces := make(map[string]struct{})
	marker := ""
	for {
		result, err := store.List(ctx, &objectstore.ListOptions{
			Prefix:  "catalog/namespaces/",
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Objects {
			parts := strings.Split(obj.Key, "/")
			if len(parts) != 3 {
				continue
			}
			if parts[2] == "" {
				continue
			}
			namespaces[parts[2]] = struct{}{}
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	result := make([]string, 0, len(namespaces))
	for ns := range namespaces {
		result = append(result, ns)
	}
	return result, nil
}

func startCompactorSync(compactor *index.BackgroundCompactor, stateMan *namespace.StateManager, store objectstore.Store, interval time.Duration, lsmConfig *index.LSMConfig) func() {
	if compactor == nil || stateMan == nil || store == nil {
		return nil
	}

	syncOnce := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := syncCompactorNamespaces(ctx, compactor, stateMan, store, lsmConfig); err != nil {
			log.Printf("Compactor namespace sync error: %v", err)
		}
	}

	syncOnce()

	if interval <= 0 {
		interval = 10 * time.Second
	}

	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				syncOnce()
			case <-stop:
				return
			}
		}
	}()

	return func() {
		close(stop)
	}
}

func syncCompactorNamespaces(ctx context.Context, compactor *index.BackgroundCompactor, stateMan *namespace.StateManager, store objectstore.Store, lsmConfig *index.LSMConfig) error {
	namespaces, err := listVexNamespaces(ctx, store)
	if err != nil {
		return err
	}

	for _, ns := range namespaces {
		loaded, err := stateMan.Load(ctx, ns)
		if err != nil {
			continue
		}
		seq := loaded.State.Index.ManifestSeq
		if seq == 0 {
			continue
		}

		manifest, err := index.LoadManifest(ctx, store, ns, seq)
		if err != nil || manifest == nil {
			continue
		}
		metrics.SetDocumentsIndexed(ns, manifest.Stats.ApproxRowCount)
		totalSegments := 0
		l0Segments := 0
		l1Segments := 0
		l2Segments := 0
		for _, seg := range manifest.Segments {
			totalSegments++
			switch seg.Level {
			case index.L0:
				l0Segments++
			case index.L1:
				l1Segments++
			case index.L2:
				l2Segments++
			}
		}
		metrics.SetSegmentCounts(ns, totalSegments, l0Segments, l1Segments, l2Segments)
		tree := index.LoadLSMTree(ns, store, manifest, lsmConfig)
		compactor.RegisterNamespace(ns, tree)
	}

	return nil
}

func listVexNamespaces(ctx context.Context, store objectstore.Store) ([]string, error) {
	if store == nil {
		return nil, nil
	}

	namespaces := make(map[string]struct{})
	marker := ""
	for {
		result, err := store.List(ctx, &objectstore.ListOptions{
			Prefix:  "vex/namespaces/",
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Objects {
			ns := extractNamespaceFromKey(obj.Key)
			if ns == "" {
				continue
			}
			namespaces[ns] = struct{}{}
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	result := make([]string, 0, len(namespaces))
	for ns := range namespaces {
		result = append(result, ns)
	}
	return result, nil
}

func extractNamespaceFromKey(key string) string {
	const base = "vex/namespaces/"
	if !strings.HasPrefix(key, base) {
		return ""
	}
	rest := strings.TrimPrefix(key, base)
	if strings.Count(rest, "/") == 1 && strings.HasSuffix(rest, "/") {
		return strings.TrimSuffix(rest, "/")
	}
	if idx := strings.Index(rest, "/"); idx > 0 {
		return rest[:idx]
	}
	return ""
}

func startWarmLoop(cacheWarmer *warmer.Warmer, cfg config.CacheConfig) func() {
	if cacheWarmer == nil {
		return nil
	}

	namespaces := cfg.WarmNamespaces
	if len(namespaces) == 0 {
		return nil
	}

	if cfg.WarmOnStart {
		for _, ns := range namespaces {
			cacheWarmer.Enqueue(ns)
		}
	}

	if cfg.WarmIntervalSeconds <= 0 {
		return nil
	}

	stop := make(chan struct{})
	interval := time.Duration(cfg.WarmIntervalSeconds) * time.Second
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, ns := range namespaces {
					cacheWarmer.Enqueue(ns)
				}
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
	}
}
