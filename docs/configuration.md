# Configuration Reference

Vex uses a JSON configuration file that is merged with defaults and can be overridden by
environment variables and CLI flags.

## Loading order and precedence

1. Built-in defaults from `internal/config/config.go`
2. JSON config file (from `-config` flag or `VEX_CONFIG`)
3. Environment variables (`VEX_*`)
4. CLI flags (`-addr` for `serve` and `query`)

If a field is omitted, the default value applies. Environment variables override config
file values. CLI flags override both.

## JSON schema (informal)

```json
{
  "mode": "serve|query|indexer",
  "listen_addr": ":8080",
  "auth_token": "string",
  "admin_token": "string",
  "compat_mode": "turbopuffer|vex",
  "object_store": {
    "type": "s3|minio|filesystem|fs|memory|mem",
    "endpoint": "string",
    "bucket": "string",
    "access_key": "string",
    "secret_key": "string",
    "region": "string",
    "use_ssl": false,
    "root_path": "string"
  },
  "cache": {
    "nvme_path": "string",
    "nvme_size_gb": 10,
    "ram_size_mb": 512,
    "ram_namespace_cap_pct": 25,
    "budget_pct": 95,
    "warm_namespaces": ["string"],
    "warm_on_start": false,
    "warm_interval_seconds": 0
  },
  "membership": {
    "type": "static|gossip",
    "nodes": ["host:port"],
    "gossip": {
      "bind_addr": "0.0.0.0",
      "bind_port": 7946,
      "advertise_addr": "string",
      "advertise_port": 0,
      "seed_nodes": ["host:port"]
    }
  },
  "indexer": {
    "write_wal_version": 0,
    "write_manifest_version": 0
  },
  "guardrails": {
    "max_namespaces": 1000,
    "max_tail_bytes_mb": 256,
    "max_concurrent_cold_fills": 4
  },
  "timeout": {
    "query_timeout_ms": 30000,
    "write_timeout_ms": 60000
  }
}
```

Notes:
- `compat_mode` defaults to `turbopuffer` when empty or invalid.
- `mode` is set by the CLI subcommand (`serve`, `query`, `indexer`) after config load.
- Timeouts are CPU budget limits enforced by API middleware, not HTTP server timeouts.

## Configuration sections

### mode
- **Type:** string (`serve`, `query`, `indexer`)
- **Default:** `serve`
- **Notes:** The `vex query` and `vex indexer` commands override this to `query` and
  `indexer` respectively.

### listen_addr
- **Type:** string (host:port or :port)
- **Default:** `:8080`
- **Overrides:** `-addr` flag for `serve` and `query`

### auth_token
- **Type:** string
- **Default:** empty
- **Behavior:** Required for all non-health endpoints. If unset, requests return 403
  with "auth token not configured".

### admin_token
- **Type:** string
- **Default:** empty
- **Behavior:** Required for admin/debug endpoints. If unset, admin endpoints return 403.

### compat_mode
- **Type:** string (`turbopuffer`, `vex`)
- **Default:** `turbopuffer`
- **Notes:** `turbopuffer` rejects `dot_product` vector distance. `vex` allows it.

### object_store
- **Type:** object
- **Default:** S3-compatible MinIO defaults (local dev):
  - type: `s3`
  - endpoint: `http://localhost:9000`
  - bucket: `vex`
  - access_key: `minioadmin`
  - secret_key: `minioadmin`
  - region: `us-east-1`
  - use_ssl: `false`
- **Fields:**
  - `type`: `s3`/`minio`, `filesystem`/`fs`, or `memory`/`mem`
  - `endpoint`: S3/MinIO endpoint URL
  - `bucket`: S3 bucket name
  - `access_key`: S3 access key
  - `secret_key`: S3 secret key
  - `region`: S3 region
  - `use_ssl`: enable HTTPS for S3/MinIO
  - `root_path`: filesystem root for `filesystem`/`fs`

### cache
- **Type:** object
- **Defaults:**
  - `nvme_path`: `/tmp/vex-cache`
  - `nvme_size_gb`: `10`
  - `ram_size_mb`: `512`
  - `ram_namespace_cap_pct`: `25`
  - `budget_pct`: `95`
  - `warm_namespaces`: empty (disabled)
  - `warm_on_start`: `false`
  - `warm_interval_seconds`: `0` (disabled)
- **Notes:**
  - Disk cache is initialized when an object store is configured.
  - RAM cache is enabled when `ram_size_mb` > 0.
  - Warm loop only runs when `warm_namespaces` is non-empty and
    `warm_interval_seconds` > 0. `warm_on_start` enqueues once at startup.

### membership
- **Type:** object
- **Defaults:**
  - `type`: `static`
  - `nodes`: `["localhost:8080"]`
- **Gossip defaults:**
  - `gossip.bind_addr`: `0.0.0.0`
  - `gossip.bind_port`: `7946`
  - `gossip.advertise_addr`: empty (optional)
  - `gossip.advertise_port`: `0` (optional)
  - `gossip.seed_nodes`: empty

### indexer
- **Type:** object
- **Defaults:**
  - `write_wal_version`: `0` (use current WAL format)
  - `write_manifest_version`: `0` (use current manifest format)
- **Notes:** Used by `vex indexer` to pin format versions during upgrades.

### guardrails
- **Type:** object
- **Defaults:**
  - `max_namespaces`: `1000`
  - `max_tail_bytes_mb`: `256`
  - `max_concurrent_cold_fills`: `4`
- **Notes:** Guardrails limit per-namespace in-memory state and tail materialization.

### timeout
- **Type:** object
- **Defaults:**
  - `query_timeout_ms`: `30000`
  - `write_timeout_ms`: `60000`
- **Notes:** Enforced as request CPU budgets by API middleware.

## Environment variable overrides

The following environment variables override config file values:

| Environment variable | Config field |
| --- | --- |
| VEX_CONFIG | Config file path (used when -config is empty) |
| VEX_LISTEN_ADDR | listen_addr |
| VEX_AUTH_TOKEN | auth_token |
| VEX_ADMIN_TOKEN | admin_token |
| VEX_COMPAT_MODE | compat_mode |
| VEX_OBJECT_STORE_TYPE | object_store.type |
| VEX_OBJECT_STORE_ENDPOINT | object_store.endpoint |
| VEX_OBJECT_STORE_BUCKET | object_store.bucket |
| VEX_OBJECT_STORE_ROOT | object_store.root_path |
| VEX_OBJECT_STORE_ACCESS_KEY | object_store.access_key |
| VEX_OBJECT_STORE_SECRET_KEY | object_store.secret_key |
| VEX_OBJECT_STORE_REGION | object_store.region |
| VEX_OBJECT_STORE_USE_SSL | object_store.use_ssl (true/1) |
| VEX_CACHE_NVME_PATH | cache.nvme_path |
| VEX_CACHE_NVME_SIZE_GB | cache.nvme_size_gb |
| VEX_CACHE_RAM_SIZE_MB | cache.ram_size_mb |
| VEX_CACHE_RAM_NAMESPACE_CAP_PCT | cache.ram_namespace_cap_pct |
| VEX_CACHE_BUDGET_PCT | cache.budget_pct |
| VEX_MEMBERSHIP_TYPE | membership.type |
| VEX_MEMBERSHIP_NODES | membership.nodes (comma-separated) |
| VEX_GOSSIP_BIND_ADDR | membership.gossip.bind_addr |
| VEX_GOSSIP_BIND_PORT | membership.gossip.bind_port |
| VEX_GOSSIP_ADVERTISE_ADDR | membership.gossip.advertise_addr |
| VEX_GOSSIP_ADVERTISE_PORT | membership.gossip.advertise_port |
| VEX_GOSSIP_SEED_NODES | membership.gossip.seed_nodes (comma-separated) |
| VEX_INDEXER_WRITE_WAL_VERSION | indexer.write_wal_version |
| VEX_INDEXER_WRITE_MANIFEST_VERSION | indexer.write_manifest_version |
| VEX_GUARDRAILS_MAX_NAMESPACES | guardrails.max_namespaces |
| VEX_GUARDRAILS_MAX_TAIL_BYTES_MB | guardrails.max_tail_bytes_mb |
| VEX_GUARDRAILS_MAX_CONCURRENT_COLD_FILLS | guardrails.max_concurrent_cold_fills |
| VEX_TIMEOUT_QUERY_MS | timeout.query_timeout_ms |
| VEX_TIMEOUT_WRITE_MS | timeout.write_timeout_ms |

## Object store types and required fields

Configured via `object_store.type` (or `VEX_OBJECT_STORE_TYPE`).

- **s3 / minio**
  - Required: `endpoint`, `bucket`, `access_key`, `secret_key`, `region`
  - Optional: `use_ssl` (defaults to `false` in the built-in config)
- **filesystem / fs**
  - Required: `root_path`
  - Default: `/tmp/vex-objectstore` when `root_path` is empty
- **memory / mem**
  - No required fields (in-memory only; not durable)

## Example configurations

### Minimal (filesystem store)

```json
{
  "listen_addr": ":8080",
  "auth_token": "dev-token",
  "object_store": {
    "type": "filesystem",
    "root_path": "/tmp/vex-data"
  }
}
```

### Full example

```json
{
  "mode": "serve",
  "listen_addr": ":8080",
  "auth_token": "prod-token",
  "admin_token": "admin-token",
  "compat_mode": "turbopuffer",
  "object_store": {
    "type": "s3",
    "endpoint": "https://s3.us-west-2.amazonaws.com",
    "bucket": "vex-prod",
    "access_key": "ACCESS_KEY",
    "secret_key": "SECRET_KEY",
    "region": "us-west-2",
    "use_ssl": true
  },
  "cache": {
    "nvme_path": "/mnt/nvme/vex-cache",
    "nvme_size_gb": 200,
    "ram_size_mb": 4096,
    "ram_namespace_cap_pct": 20,
    "budget_pct": 90,
    "warm_namespaces": ["customers", "orders"],
    "warm_on_start": true,
    "warm_interval_seconds": 3600
  },
  "membership": {
    "type": "gossip",
    "nodes": ["10.0.0.1:8080"],
    "gossip": {
      "bind_addr": "0.0.0.0",
      "bind_port": 7946,
      "advertise_addr": "10.0.0.1",
      "advertise_port": 7946,
      "seed_nodes": ["10.0.0.1:7946", "10.0.0.2:7946"]
    }
  },
  "indexer": {
    "write_wal_version": 0,
    "write_manifest_version": 0
  },
  "guardrails": {
    "max_namespaces": 1000,
    "max_tail_bytes_mb": 512,
    "max_concurrent_cold_fills": 8
  },
  "timeout": {
    "query_timeout_ms": 30000,
    "write_timeout_ms": 60000
  }
}
```
