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

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/membership"
	"github.com/vexsearch/vex/internal/routing"
	"github.com/vexsearch/vex/pkg/api"
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
	clusterRouter := routing.New(cfg.ListenAddr)
	membershipProvider := membership.NewFromConfig(cfg.Membership)
	membershipMgr := membership.NewManager(membershipProvider, clusterRouter)

	if err := membershipMgr.Start(); err != nil {
		log.Fatalf("Failed to start membership manager: %v", err)
	}

	router := api.NewRouterWithMembership(cfg, clusterRouter, membershipMgr)

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

	membershipMgr.Stop()

	fmt.Println("Server stopped")
}
