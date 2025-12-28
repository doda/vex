package config

import (
	"os"
	"path/filepath"
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
	if cfg.Cache.BudgetPct != 95 {
		t.Errorf("expected cache budget 95, got %d", cfg.Cache.BudgetPct)
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

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
		"listen_addr": ":3000",
		"mode": "query",
		"object_store": {
			"type": "s3",
			"endpoint": "https://s3.amazonaws.com",
			"bucket": "my-bucket",
			"access_key": "AKIAIOSFODNN7EXAMPLE",
			"secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			"region": "us-west-2",
			"use_ssl": true
		},
		"cache": {
			"nvme_path": "/mnt/nvme/cache",
			"nvme_size_gb": 100,
			"ram_size_mb": 2048,
			"budget_pct": 80
		},
		"membership": {
			"type": "static",
			"nodes": ["node1:8080", "node2:8080", "node3:8080"]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.ListenAddr != ":3000" {
		t.Errorf("expected listen addr :3000, got %s", cfg.ListenAddr)
	}
	if cfg.Mode != ModeQuery {
		t.Errorf("expected mode query, got %s", cfg.Mode)
	}
}

func TestLoadFromVexConfigEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vex.json")
	content := `{"listen_addr": ":4000"}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	os.Setenv("VEX_CONFIG", path)
	defer os.Unsetenv("VEX_CONFIG")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.ListenAddr != ":4000" {
		t.Errorf("expected listen addr :4000, got %s", cfg.ListenAddr)
	}
}

func TestEnvOverridesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{"listen_addr": ":3000"}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	os.Setenv("VEX_LISTEN_ADDR", ":5000")
	defer os.Unsetenv("VEX_LISTEN_ADDR")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.ListenAddr != ":5000" {
		t.Errorf("env should override file: expected :5000, got %s", cfg.ListenAddr)
	}
}

func TestObjectStoreConfig(t *testing.T) {
	envs := map[string]string{
		"VEX_OBJECT_STORE_ENDPOINT":   "https://minio.example.com",
		"VEX_OBJECT_STORE_BUCKET":     "test-bucket",
		"VEX_OBJECT_STORE_ACCESS_KEY": "test-access-key",
		"VEX_OBJECT_STORE_SECRET_KEY": "test-secret-key",
		"VEX_OBJECT_STORE_REGION":     "eu-west-1",
		"VEX_OBJECT_STORE_USE_SSL":    "true",
	}
	for k, v := range envs {
		os.Setenv(k, v)
		defer os.Unsetenv(k)
	}

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ObjectStore.Endpoint != "https://minio.example.com" {
		t.Errorf("expected endpoint https://minio.example.com, got %s", cfg.ObjectStore.Endpoint)
	}
	if cfg.ObjectStore.Bucket != "test-bucket" {
		t.Errorf("expected bucket test-bucket, got %s", cfg.ObjectStore.Bucket)
	}
	if cfg.ObjectStore.AccessKey != "test-access-key" {
		t.Errorf("expected access key test-access-key, got %s", cfg.ObjectStore.AccessKey)
	}
	if cfg.ObjectStore.SecretKey != "test-secret-key" {
		t.Errorf("expected secret key test-secret-key, got %s", cfg.ObjectStore.SecretKey)
	}
	if cfg.ObjectStore.Region != "eu-west-1" {
		t.Errorf("expected region eu-west-1, got %s", cfg.ObjectStore.Region)
	}
	if !cfg.ObjectStore.UseSSL {
		t.Error("expected use_ssl true, got false")
	}
}

func TestObjectStoreConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
		"object_store": {
			"type": "s3",
			"endpoint": "https://s3.example.com",
			"bucket": "prod-bucket",
			"access_key": "PROD_KEY",
			"secret_key": "PROD_SECRET",
			"region": "ap-southeast-1",
			"use_ssl": true
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.ObjectStore.Endpoint != "https://s3.example.com" {
		t.Errorf("expected endpoint https://s3.example.com, got %s", cfg.ObjectStore.Endpoint)
	}
	if cfg.ObjectStore.Bucket != "prod-bucket" {
		t.Errorf("expected bucket prod-bucket, got %s", cfg.ObjectStore.Bucket)
	}
	if cfg.ObjectStore.Region != "ap-southeast-1" {
		t.Errorf("expected region ap-southeast-1, got %s", cfg.ObjectStore.Region)
	}
}

func TestCacheSizeConfig(t *testing.T) {
	envs := map[string]string{
		"VEX_CACHE_NVME_PATH":    "/data/cache",
		"VEX_CACHE_NVME_SIZE_GB": "500",
		"VEX_CACHE_RAM_SIZE_MB":  "4096",
		"VEX_CACHE_BUDGET_PCT":   "90",
	}
	for k, v := range envs {
		os.Setenv(k, v)
		defer os.Unsetenv(k)
	}

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Cache.NVMePath != "/data/cache" {
		t.Errorf("expected nvme path /data/cache, got %s", cfg.Cache.NVMePath)
	}
	if cfg.Cache.NVMESizeGB != 500 {
		t.Errorf("expected nvme size 500GB, got %d", cfg.Cache.NVMESizeGB)
	}
	if cfg.Cache.RAMSizeMB != 4096 {
		t.Errorf("expected ram size 4096MB, got %d", cfg.Cache.RAMSizeMB)
	}
	if cfg.Cache.BudgetPct != 90 {
		t.Errorf("expected budget pct 90, got %d", cfg.Cache.BudgetPct)
	}
}

func TestCacheConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
		"cache": {
			"nvme_path": "/mnt/ssd/vex",
			"nvme_size_gb": 200,
			"ram_size_mb": 8192,
			"budget_pct": 85
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Cache.NVMePath != "/mnt/ssd/vex" {
		t.Errorf("expected nvme path /mnt/ssd/vex, got %s", cfg.Cache.NVMePath)
	}
	if cfg.Cache.NVMESizeGB != 200 {
		t.Errorf("expected nvme size 200GB, got %d", cfg.Cache.NVMESizeGB)
	}
	if cfg.Cache.RAMSizeMB != 8192 {
		t.Errorf("expected ram size 8192MB, got %d", cfg.Cache.RAMSizeMB)
	}
	if cfg.Cache.BudgetPct != 85 {
		t.Errorf("expected budget pct 85, got %d", cfg.Cache.BudgetPct)
	}
}

func TestMembershipConfig(t *testing.T) {
	os.Setenv("VEX_MEMBERSHIP_TYPE", "gossip")
	os.Setenv("VEX_MEMBERSHIP_NODES", "node1:8080, node2:8080, node3:8080")
	defer os.Unsetenv("VEX_MEMBERSHIP_TYPE")
	defer os.Unsetenv("VEX_MEMBERSHIP_NODES")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Membership.Type != "gossip" {
		t.Errorf("expected membership type gossip, got %s", cfg.Membership.Type)
	}
	if len(cfg.Membership.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(cfg.Membership.Nodes))
	}
	expected := []string{"node1:8080", "node2:8080", "node3:8080"}
	for i, node := range cfg.Membership.Nodes {
		if node != expected[i] {
			t.Errorf("expected node %s, got %s", expected[i], node)
		}
	}
}

func TestMembershipConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
		"membership": {
			"type": "static",
			"nodes": ["vex-1.cluster:8080", "vex-2.cluster:8080"]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Membership.Type != "static" {
		t.Errorf("expected membership type static, got %s", cfg.Membership.Type)
	}
	if len(cfg.Membership.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(cfg.Membership.Nodes))
	}
	if cfg.Membership.Nodes[0] != "vex-1.cluster:8080" {
		t.Errorf("expected node vex-1.cluster:8080, got %s", cfg.Membership.Nodes[0])
	}
}

func TestCompatModeConfig(t *testing.T) {
	os.Setenv("VEX_COMPAT_MODE", "vex")
	defer os.Unsetenv("VEX_COMPAT_MODE")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.CompatMode != "vex" {
		t.Errorf("expected compat mode vex, got %s", cfg.CompatMode)
	}
}

func TestGossipConfig(t *testing.T) {
	os.Setenv("VEX_MEMBERSHIP_TYPE", "gossip")
	os.Setenv("VEX_GOSSIP_BIND_ADDR", "10.0.0.1")
	os.Setenv("VEX_GOSSIP_BIND_PORT", "7947")
	os.Setenv("VEX_GOSSIP_ADVERTISE_ADDR", "192.168.1.100")
	os.Setenv("VEX_GOSSIP_ADVERTISE_PORT", "7948")
	os.Setenv("VEX_GOSSIP_SEED_NODES", "seed1:7946, seed2:7946, seed3:7946")
	defer os.Unsetenv("VEX_MEMBERSHIP_TYPE")
	defer os.Unsetenv("VEX_GOSSIP_BIND_ADDR")
	defer os.Unsetenv("VEX_GOSSIP_BIND_PORT")
	defer os.Unsetenv("VEX_GOSSIP_ADVERTISE_ADDR")
	defer os.Unsetenv("VEX_GOSSIP_ADVERTISE_PORT")
	defer os.Unsetenv("VEX_GOSSIP_SEED_NODES")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Membership.Type != "gossip" {
		t.Errorf("expected membership type gossip, got %s", cfg.Membership.Type)
	}
	if cfg.Membership.Gossip.BindAddr != "10.0.0.1" {
		t.Errorf("expected gossip bind addr 10.0.0.1, got %s", cfg.Membership.Gossip.BindAddr)
	}
	if cfg.Membership.Gossip.BindPort != 7947 {
		t.Errorf("expected gossip bind port 7947, got %d", cfg.Membership.Gossip.BindPort)
	}
	if cfg.Membership.Gossip.AdvertiseAddr != "192.168.1.100" {
		t.Errorf("expected gossip advertise addr 192.168.1.100, got %s", cfg.Membership.Gossip.AdvertiseAddr)
	}
	if cfg.Membership.Gossip.AdvertisePort != 7948 {
		t.Errorf("expected gossip advertise port 7948, got %d", cfg.Membership.Gossip.AdvertisePort)
	}
	if len(cfg.Membership.Gossip.SeedNodes) != 3 {
		t.Errorf("expected 3 seed nodes, got %d", len(cfg.Membership.Gossip.SeedNodes))
	}
	expectedSeeds := []string{"seed1:7946", "seed2:7946", "seed3:7946"}
	for i, seed := range cfg.Membership.Gossip.SeedNodes {
		if seed != expectedSeeds[i] {
			t.Errorf("expected seed node %s, got %s", expectedSeeds[i], seed)
		}
	}
}

func TestGossipConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
		"membership": {
			"type": "gossip",
			"nodes": ["localhost:8080"],
			"gossip": {
				"bind_addr": "0.0.0.0",
				"bind_port": 7946,
				"advertise_addr": "192.168.1.50",
				"advertise_port": 7946,
				"seed_nodes": ["seed-node-1:7946", "seed-node-2:7946"]
			}
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Membership.Type != "gossip" {
		t.Errorf("expected membership type gossip, got %s", cfg.Membership.Type)
	}
	if cfg.Membership.Gossip.BindAddr != "0.0.0.0" {
		t.Errorf("expected gossip bind addr 0.0.0.0, got %s", cfg.Membership.Gossip.BindAddr)
	}
	if cfg.Membership.Gossip.BindPort != 7946 {
		t.Errorf("expected gossip bind port 7946, got %d", cfg.Membership.Gossip.BindPort)
	}
	if cfg.Membership.Gossip.AdvertiseAddr != "192.168.1.50" {
		t.Errorf("expected gossip advertise addr 192.168.1.50, got %s", cfg.Membership.Gossip.AdvertiseAddr)
	}
	if len(cfg.Membership.Gossip.SeedNodes) != 2 {
		t.Errorf("expected 2 seed nodes, got %d", len(cfg.Membership.Gossip.SeedNodes))
	}
}

func TestInvalidConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{invalid json`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestMissingConfigFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.json")
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestPartialConfigMerge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{"listen_addr": ":3000"}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.ListenAddr != ":3000" {
		t.Errorf("expected listen addr :3000, got %s", cfg.ListenAddr)
	}
	if cfg.ObjectStore.Endpoint != "http://localhost:9000" {
		t.Errorf("expected default endpoint to be preserved, got %s", cfg.ObjectStore.Endpoint)
	}
	if cfg.Cache.BudgetPct != 95 {
		t.Errorf("expected default budget pct to be preserved, got %d", cfg.Cache.BudgetPct)
	}
}
