package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestSubcommands(t *testing.T) {
	binaryPath, err := filepath.Abs("../../vex")
	if err != nil {
		t.Fatalf("failed to get binary path: %v", err)
	}

	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		t.Skip("vex binary not found - run 'go build -o vex ./cmd/vex' first")
	}

	t.Run("help shows usage", func(t *testing.T) {
		cmd := exec.Command(binaryPath, "help")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("help command failed: %v", err)
		}
		if !contains(string(out), "serve") || !contains(string(out), "query") || !contains(string(out), "indexer") {
			t.Errorf("help output missing subcommands: %s", out)
		}
	})

	t.Run("version prints version info", func(t *testing.T) {
		cmd := exec.Command(binaryPath, "version")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("version command failed: %v", err)
		}
		if !contains(string(out), "vex version") {
			t.Errorf("version output incorrect: %s", out)
		}
	})

	t.Run("no args shows usage and exits 1", func(t *testing.T) {
		cmd := exec.Command(binaryPath)
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatal("expected non-zero exit for no args")
		}
		if !contains(string(out), "Usage:") {
			t.Errorf("expected usage output, got: %s", out)
		}
	})

	t.Run("unknown command exits 1", func(t *testing.T) {
		cmd := exec.Command(binaryPath, "notreal")
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatal("expected non-zero exit for unknown command")
		}
		if !contains(string(out), "Unknown command") {
			t.Errorf("expected unknown command message, got: %s", out)
		}
	})
}

func TestServeMode(t *testing.T) {
	binaryPath, err := filepath.Abs("../../vex")
	if err != nil {
		t.Fatalf("failed to get binary path: %v", err)
	}

	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		t.Skip("vex binary not found")
	}

	port := 18080
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, binaryPath, "serve", fmt.Sprintf("--addr=:%d", port))
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start serve: %v", err)
	}
	defer cmd.Process.Kill()

	time.Sleep(2 * time.Second)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["status"] != "ok" {
		t.Errorf("expected status ok, got %s", result["status"])
	}
}

func TestQueryMode(t *testing.T) {
	binaryPath, err := filepath.Abs("../../vex")
	if err != nil {
		t.Fatalf("failed to get binary path: %v", err)
	}

	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		t.Skip("vex binary not found")
	}

	port := 18081
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, binaryPath, "query", fmt.Sprintf("--addr=:%d", port))
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start query: %v", err)
	}
	defer cmd.Process.Kill()

	time.Sleep(2 * time.Second)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestConfigLoading(t *testing.T) {
	binaryPath, err := filepath.Abs("../../vex")
	if err != nil {
		t.Fatalf("failed to get binary path: %v", err)
	}

	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		t.Skip("vex binary not found")
	}

	configFile, err := os.CreateTemp("", "vex-config-*.json")
	if err != nil {
		t.Fatalf("failed to create temp config: %v", err)
	}
	defer os.Remove(configFile.Name())

	port := 18082
	config := fmt.Sprintf(`{
		"listen_addr": ":%d",
		"object_store": {
			"endpoint": "http://custom-endpoint:9000",
			"bucket": "custom-bucket"
		}
	}`, port)
	if _, err := configFile.WriteString(config); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	configFile.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, binaryPath, "serve", "--config="+configFile.Name())
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start serve with config: %v", err)
	}
	defer cmd.Process.Kill()

	time.Sleep(2 * time.Second)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
	if err != nil {
		t.Fatalf("health check on config port failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
