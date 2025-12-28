package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
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
	AdminToken  string            `json:"admin_token"`
	CompatMode  string            `json:"compat_mode"`
	ObjectStore ObjectStoreConfig `json:"object_store"`
	Cache       CacheConfig       `json:"cache"`
	Membership  MembershipConfig  `json:"membership"`
	Indexer     IndexerConfig     `json:"indexer"`
}

// IndexerConfig holds indexer-specific configuration.
type IndexerConfig struct {
	// WriteWALVersion specifies which WAL format version to write.
	// 0 means use the current version. Use this for N-1 compatibility during upgrades.
	WriteWALVersion int `json:"write_wal_version,omitempty"`
	// WriteManifestVersion specifies which manifest format version to write.
	// 0 means use the current version.
	WriteManifestVersion int `json:"write_manifest_version,omitempty"`
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
	// Gossip-specific configuration
	Gossip GossipConfig `json:"gossip"`
}

type GossipConfig struct {
	// BindAddr is the address to bind gossip listener to (default: "0.0.0.0")
	BindAddr string `json:"bind_addr"`
	// BindPort is the port to bind gossip listener to (default: 7946)
	BindPort int `json:"bind_port"`
	// AdvertiseAddr is the address advertised to other cluster members (optional)
	AdvertiseAddr string `json:"advertise_addr"`
	// AdvertisePort is the port advertised to other cluster members (optional)
	AdvertisePort int `json:"advertise_port"`
	// SeedNodes is a list of seed nodes to bootstrap gossip membership
	SeedNodes []string `json:"seed_nodes"`
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
	if env := os.Getenv("VEX_ADMIN_TOKEN"); env != "" {
		cfg.AdminToken = env
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
	if env := os.Getenv("VEX_OBJECT_STORE_REGION"); env != "" {
		cfg.ObjectStore.Region = env
	}
	if env := os.Getenv("VEX_OBJECT_STORE_USE_SSL"); env != "" {
		cfg.ObjectStore.UseSSL = env == "true" || env == "1"
	}

	if env := os.Getenv("VEX_CACHE_NVME_PATH"); env != "" {
		cfg.Cache.NVMePath = env
	}
	if env := os.Getenv("VEX_CACHE_NVME_SIZE_GB"); env != "" {
		if n, err := parseIntEnv(env); err == nil {
			cfg.Cache.NVMESizeGB = n
		}
	}
	if env := os.Getenv("VEX_CACHE_RAM_SIZE_MB"); env != "" {
		if n, err := parseIntEnv(env); err == nil {
			cfg.Cache.RAMSizeMB = n
		}
	}
	if env := os.Getenv("VEX_CACHE_BUDGET_PCT"); env != "" {
		if n, err := parseIntEnv(env); err == nil {
			cfg.Cache.BudgetPct = n
		}
	}

	if env := os.Getenv("VEX_MEMBERSHIP_TYPE"); env != "" {
		cfg.Membership.Type = env
	}
	if env := os.Getenv("VEX_MEMBERSHIP_NODES"); env != "" {
		cfg.Membership.Nodes = parseNodeList(env)
	}
	if env := os.Getenv("VEX_GOSSIP_BIND_ADDR"); env != "" {
		cfg.Membership.Gossip.BindAddr = env
	}
	if env := os.Getenv("VEX_GOSSIP_BIND_PORT"); env != "" {
		if n, err := parseIntEnv(env); err == nil {
			cfg.Membership.Gossip.BindPort = n
		}
	}
	if env := os.Getenv("VEX_GOSSIP_ADVERTISE_ADDR"); env != "" {
		cfg.Membership.Gossip.AdvertiseAddr = env
	}
	if env := os.Getenv("VEX_GOSSIP_ADVERTISE_PORT"); env != "" {
		if n, err := parseIntEnv(env); err == nil {
			cfg.Membership.Gossip.AdvertisePort = n
		}
	}
	if env := os.Getenv("VEX_GOSSIP_SEED_NODES"); env != "" {
		cfg.Membership.Gossip.SeedNodes = parseNodeList(env)
	}

	if env := os.Getenv("VEX_COMPAT_MODE"); env != "" {
		cfg.CompatMode = env
	}

	// Indexer format version configuration
	if env := os.Getenv("VEX_INDEXER_WRITE_WAL_VERSION"); env != "" {
		if n, err := parseIntEnv(env); err == nil {
			cfg.Indexer.WriteWALVersion = n
		}
	}
	if env := os.Getenv("VEX_INDEXER_WRITE_MANIFEST_VERSION"); env != "" {
		if n, err := parseIntEnv(env); err == nil {
			cfg.Indexer.WriteManifestVersion = n
		}
	}

	return cfg, nil
}

func parseIntEnv(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

func parseNodeList(s string) []string {
	var nodes []string
	for _, node := range strings.Split(s, ",") {
		node = strings.TrimSpace(node)
		if node != "" {
			nodes = append(nodes, node)
		}
	}
	return nodes
}
