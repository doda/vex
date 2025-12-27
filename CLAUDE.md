# Vex - Object-Storage-First Search Engine

## Project Overview

Vex is an open-source search engine compatible with turbopuffer's published API. It supports:
- **Vector search** (ANN with IVF centroid-based indexes)
- **Full-text search** (BM25 ranking)
- **Hybrid search** (via multi-query snapshot isolation)
- **Metadata filtering** (roaring bitmaps, recall-aware filtering)

**Core Language:** Go
**Compatibility Target:** turbopuffer public API semantics (Dec 2025)

## Project Status

**NOT YET IMPLEMENTED** - This project is in the planning phase. See `task_list.json` for the comprehensive task breakdown.

## Key Requirements

### Architecture
- Object storage is the **system of record** (durability layer)
- Writes append to **object-storage WAL** (batched, 1/sec per namespace)
- **Asynchronous indexing** after WAL commit
- **Stateless query nodes** with NVMe SSD + RAM caching
- **Cache-locality routing** via rendezvous hashing

### API Endpoints (turbopuffer-compatible)
- `POST /v2/namespaces/:namespace` - Write documents
- `POST /v2/namespaces/:namespace/query` - Query documents
- `GET /v1/namespaces/:namespace/metadata` - Namespace metadata
- `GET /v1/namespaces/:namespace/hint_cache_warm` - Warm cache hint
- `GET /v1/namespaces` - List namespaces
- `DELETE /v2/namespaces/:namespace` - Delete namespace
- `POST /v1/namespaces/:namespace/_debug/recall` - Recall measurement

### Consistency Modes
- **Strong (default):** All committed writes included, cache refreshed
- **Eventual:** Up to 60s stale, max 128 MiB tail searched

### Key Limits
- Max batch size: 256MB
- Max batch rate: 1/sec per namespace
- Unindexed data cap: 2GB (backpressure at 429)
- Max query concurrency: 16 per namespace
- Max multi-query: 16 subqueries
- Max top_k: 10,000

## Recommended Stack

- **Language:** Go 1.21+
- **Object Store:** S3-compatible (MinIO for local dev)
- **WAL Encoding:** Protobuf + zstd compression
- **Filter Indexes:** Roaring bitmaps
- **Vector Index:** IVF (centroid clusters) - SPFresh-like
- **HTTP:** net/http with chi or similar router
- **Metrics:** Prometheus

## File Structure (Suggested)

```
cmd/
  vex/           # Main binary with subcommands
internal/
  cache/         # NVMe + RAM cache
  config/        # Configuration management
  filter/        # Filter AST and evaluation
  fts/           # Full-text search (BM25)
  gc/            # Garbage collection
  index/         # Index segments and manifests
  indexer/       # Indexer process
  logging/       # Structured logging
  membership/    # Cluster membership
  metrics/       # Prometheus metrics
  namespace/     # Namespace state management
  query/         # Query execution engine
  routing/       # Rendezvous hashing + proxy
  schema/        # Type system and validation
  tail/          # Tail materialization
  vector/        # Vector operations and IVF index
  wal/           # WAL format and commit
  write/         # Write path processing
pkg/
  api/           # HTTP API handlers
  objectstore/   # ObjectStore interface
  state/         # State management
tests/
  compat/        # Compatibility tests
  fault/         # Fault injection tests
  golden/        # Golden API tests
  simulation/    # Deterministic simulation tests
```

## Getting Started

1. Complete the `project-setup` task first
2. Run `./init.sh` to set up dependencies
3. Follow the task list in order of dependencies

## Reference

See `SPEC.md` for the complete technical specification.
