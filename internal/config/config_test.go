package config

import (
	"os"
	"testing"
)

func TestDefault(t *testing.T) {
	cfg := Default()
	if cfg.ListenAddr != ":8080" {
		t.Errorf("expected listen addr :8080, got %s", cfg.ListenAddr)
	}
	if cfg.Mode != ModeServe {
		t.Errorf("expected mode serve, got %s", cfg.Mode)
	}
	if cfg.ObjectStore.Endpoint != "http://localhost:9000" {
		t.Errorf("expected endpoint http://localhost:9000, got %s", cfg.ObjectStore.Endpoint)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	os.Setenv("VEX_LISTEN_ADDR", ":9090")
	defer os.Unsetenv("VEX_LISTEN_ADDR")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ListenAddr != ":9090" {
		t.Errorf("expected listen addr :9090, got %s", cfg.ListenAddr)
	}
}

func TestLoadAuthToken(t *testing.T) {
	os.Setenv("VEX_AUTH_TOKEN", "secret-token")
	defer os.Unsetenv("VEX_AUTH_TOKEN")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AuthToken != "secret-token" {
		t.Errorf("expected auth token secret-token, got %s", cfg.AuthToken)
	}
}
