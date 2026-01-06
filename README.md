# Vex

Vex is an open-source, object-storage-first search engine (vector + full-text + hybrid)
compatible with turbopuffer's published API and semantics. It uses object storage as
the system of record, an object-storage WAL, asynchronous indexing, and stateless
query nodes with NVMe + RAM caching.

Compatibility target: turbopuffer public docs/API semantics as of Dec 2025.

## Highlights

- Object storage is the durability layer; writes are committed through an
  object-storage WAL before they are visible.
- Stateless query nodes with cache-locality routing and NVMe + RAM caches.
- Vector ANN search, BM25 full-text search, and hybrid retrieval via multi-query.
- Strong consistency by default, eventual consistency for performance.
- turbopuffer-compatible HTTP API surface (auth, write, query, metadata, list,
  warm-cache hint, delete, recall/debug).

## Quickstart

Prerequisites: Go 1.21+ and an object store. For local development you can use
MinIO (see `./scripts/setup-minio.sh`) or the filesystem store.

Build the binary:

```bash
/usr/local/go/bin/go build -o vex ./cmd/vex
```

Run the all-in-one server using the filesystem object store:

```bash
export VEX_OBJECT_STORE_TYPE=filesystem
export VEX_OBJECT_STORE_ROOT=/tmp/vex-objectstore
./vex serve --addr :8080
```

Health check:

```bash
curl http://localhost:8080/health
```

For full configuration options (including MinIO/S3), see
`docs/configuration.md`.

## CLI Commands and Run Modes

`vex` ships as a single binary with subcommands:

- `vex serve` - all-in-one mode (query + indexer in one process).
  - Flags: `--config <path>`, `--addr <listen addr>`.
- `vex query` - query node only.
  - Flags: `--config <path>`, `--addr <listen addr>`.
- `vex indexer` - indexer node only.
  - Flags: `--config <path>`.
- `vex version` - print build/version info.
- `vex help` - show command help.

All modes load config from `--config` or `VEX_CONFIG`, with environment overrides
available. See `docs/configuration.md` for details.

## Documentation

- Architecture overview: `docs/architecture.md`
- Configuration: `docs/configuration.md`
- API overview: `docs/api/overview.md`
- Write API: `docs/api/write.md`
- Query API: `docs/api/query.md`
- Namespace APIs: `docs/api/namespaces.md`
- Debug/recall APIs: `docs/api/debug.md`

## Status

Vex implements the turbopuffer-compatible API surface and core architecture
defined in `SPEC.md`. It is intended to be wire-compatible with turbopuffer's
published HTTP API while remaining a clean Go implementation.
