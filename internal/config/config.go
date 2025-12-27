package config

import (
	"encoding/json"
	"os"
)

type Mode string

const (
	ModeServe   Mode = "serve"
	ModeQuery   Mode = "query"
	ModeIndexer Mode = "indexer"
)

type Config struct {
	Mode        Mode              `json:"mode"`
	ListenAddr  string            `json:"listen_addr"`
	AuthToken   string            `json:"auth_token"`
	CompatMode  string            `json:"compat_mode"`
	ObjectStore ObjectStoreConfig `json:"object_store"`
	Cache       CacheConfig       `json:"cache"`
	Membership  MembershipConfig  `json:"membership"`
}

type ObjectStoreConfig struct {
	Type      string `json:"type"`
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Region    string `json:"region"`
	UseSSL    bool   `json:"use_ssl"`
}

type CacheConfig struct {
	NVMePath     string `json:"nvme_path"`
	NVMESizeGB   int    `json:"nvme_size_gb"`
	RAMSizeMB    int    `json:"ram_size_mb"`
	BudgetPct    int    `json:"budget_pct"`
}

type MembershipConfig struct {
	Type  string   `json:"type"`
	Nodes []string `json:"nodes"`
}

func Default() *Config {
	return &Config{
		Mode:       ModeServe,
		ListenAddr: ":8080",
		CompatMode: "turbopuffer",
		ObjectStore: ObjectStoreConfig{
			Type:      "s3",
			Endpoint:  "http://localhost:9000",
			Bucket:    "vex",
			AccessKey: "minioadmin",
			SecretKey: "minioadmin",
			Region:    "us-east-1",
			UseSSL:    false,
		},
		Cache: CacheConfig{
			NVMePath:   "/tmp/vex-cache",
			NVMESizeGB: 10,
			RAMSizeMB:  512,
			BudgetPct:  95,
		},
		Membership: MembershipConfig{
			Type:  "static",
			Nodes: []string{"localhost:8080"},
		},
	}
}

func Load(path string) (*Config, error) {
	cfg := Default()

	if path == "" {
		path = os.Getenv("VEX_CONFIG")
	}

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, err
		}
	}

	if env := os.Getenv("VEX_LISTEN_ADDR"); env != "" {
		cfg.ListenAddr = env
	}
	if env := os.Getenv("VEX_AUTH_TOKEN"); env != "" {
		cfg.AuthToken = env
	}
	if env := os.Getenv("VEX_OBJECT_STORE_ENDPOINT"); env != "" {
		cfg.ObjectStore.Endpoint = env
	}
	if env := os.Getenv("VEX_OBJECT_STORE_BUCKET"); env != "" {
		cfg.ObjectStore.Bucket = env
	}
	if env := os.Getenv("VEX_OBJECT_STORE_ACCESS_KEY"); env != "" {
		cfg.ObjectStore.AccessKey = env
	}
	if env := os.Getenv("VEX_OBJECT_STORE_SECRET_KEY"); env != "" {
		cfg.ObjectStore.SecretKey = env
	}

	return cfg, nil
}
