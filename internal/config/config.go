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

// CompatMode represents the compatibility mode for the server.
type CompatMode string

const (
	// CompatModeTurbopuffer enforces strict turbopuffer API compatibility.
	// - Rejects dot_product distance metric
	// - Disables Vex-only extensions
	CompatModeTurbopuffer CompatMode = "turbopuffer"

	// CompatModeVex enables Vex-only extensions.
	// - Allows dot_product distance metric
	// - Enables all Vex-specific features
	CompatModeVex CompatMode = "vex"
)

// DefaultCompatMode is the default compatibility mode when not specified.
const DefaultCompatMode = CompatModeTurbopuffer

// IsValid returns true if the compat mode is a recognized value.
func (m CompatMode) IsValid() bool {
	switch m {
	case CompatModeTurbopuffer, CompatModeVex:
		return true
	default:
		return false
	}
}

// IsTurbopuffer returns true if the compat mode is turbopuffer.
func (m CompatMode) IsTurbopuffer() bool {
	return m == CompatModeTurbopuffer
}

// AllowsDotProduct returns true if dot_product distance metric is allowed.
func (m CompatMode) AllowsDotProduct() bool {
	return m == CompatModeVex
}

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
	Guardrails  GuardrailsConfig  `json:"guardrails"`
}

// GetCompatMode returns the compatibility mode as a typed CompatMode.
// Returns DefaultCompatMode if the stored value is empty or invalid.
func (c *Config) GetCompatMode() CompatMode {
	if c.CompatMode == "" {
		return DefaultCompatMode
	}
	mode := CompatMode(c.CompatMode)
	if !mode.IsValid() {
		return DefaultCompatMode
	}
	return mode
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

// GuardrailsConfig holds per-namespace guardrails configuration.
type GuardrailsConfig struct {
	// MaxNamespaces is the maximum number of namespaces to keep in memory.
	// Default: 1000
	MaxNamespaces int `json:"max_namespaces"`
	// MaxTailBytesMB is the maximum tail bytes per namespace in MB.
	// Default: 256
	MaxTailBytesMB int `json:"max_tail_bytes_mb"`
	// MaxConcurrentColdFills limits parallel cold cache fills.
	// Default: 4
	MaxConcurrentColdFills int `json:"max_concurrent_cold_fills"`
}

// MaxTailBytesPerNamespace returns the max tail bytes (converted from MB to bytes).
func (c GuardrailsConfig) MaxTailBytesPerNamespace() int64 {
	if c.MaxTailBytesMB <= 0 {
		return 256 * 1024 * 1024 // 256 MB default
	}
	return int64(c.MaxTailBytesMB) * 1024 * 1024
}

// GetMaxNamespaces returns MaxNamespaces with default fallback.
func (c GuardrailsConfig) GetMaxNamespaces() int {
	if c.MaxNamespaces <= 0 {
		return 1000
	}
	return c.MaxNamespaces
}

// GetMaxConcurrentColdFills returns MaxConcurrentColdFills with default fallback.
func (c GuardrailsConfig) GetMaxConcurrentColdFills() int {
	if c.MaxConcurrentColdFills <= 0 {
		return 4
	}
	return c.MaxConcurrentColdFills
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

	// Guardrails configuration
	if env := os.Getenv("VEX_GUARDRAILS_MAX_NAMESPACES"); env != "" {
		if n, err := parseIntEnv(env); err == nil {
			cfg.Guardrails.MaxNamespaces = n
		}
	}
	if env := os.Getenv("VEX_GUARDRAILS_MAX_TAIL_BYTES_MB"); env != "" {
		if n, err := parseIntEnv(env); err == nil {
			cfg.Guardrails.MaxTailBytesMB = n
		}
	}
	if env := os.Getenv("VEX_GUARDRAILS_MAX_CONCURRENT_COLD_FILLS"); env != "" {
		if n, err := parseIntEnv(env); err == nil {
			cfg.Guardrails.MaxConcurrentColdFills = n
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
