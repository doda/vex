# Architecture Overview

This document summarizes how Vex is structured, how data flows through the
system, and how storage, caching, routing, and consistency work in practice.

## Roles and runtime modes

Vex is split into three roles that can be combined or run independently:

- Query nodes: Serve HTTP APIs, maintain RAM/NVMe caches, materialize WAL tail,
  and execute queries.
- Indexer nodes: Watch WAL and state, build index segments, and publish new
  manifests to object storage.
- Object storage: The sole durable system of record for WAL, state, and index
  artifacts.

The single `vex` binary exposes runtime modes:

- `vex serve`: all-in-one server (query + indexer)
- `vex query`: query node only
- `vex indexer`: indexer node only
- `vex version`: version info

## Storage layout (object store)

All namespace data lives under `vex/namespaces/<namespace>/`:

```
vex/
  namespaces/
    <namespace>/
      meta/
        state.json
        tombstone.json
      wal/
        00000000000000000001.wal.zst
        ...
      index/
        manifests/
          00000000000000000010.idx.json
        segments/
          seg_<uuid>/
            docs.col.zst
            vectors.ivf.zst
            filters.<attr>.rbm
            fts.<field>.bm25
      gc/
        orphan_scan_marker.json
```

`meta/state.json` is the authoritative coordination object. WAL entries and
index segments are immutable; manifests and state pointers provide atomic
publish points.

## Write path (WAL -> tail -> indexing)

1. **Request handling:** The write handler validates the request, applies
   canonicalization (IDs, types, datetime parsing), and enforces write ordering:
   delete_by_filter -> patch_by_filter -> copy_from_namespace -> upserts ->
   patches -> deletes.
2. **Snapshot-dependent operations:** Filter-based and conditional writes use
   a tail snapshot to re-evaluate against the latest WAL head when needed.
3. **WAL entry creation:** The handler builds a WAL entry containing one
   sub-batch per request, encodes it deterministically (protobuf + zstd), and
   computes a checksum.
4. **Commit protocol:** The WAL entry is uploaded with `If-None-Match: *` for
   idempotency, then `state.json` is updated via CAS (`If-Match`). The write is
   acknowledged only after both succeed.
5. **Tail materialization:** Query nodes maintain a tiered tail store (RAM,
   NVMe, object store) that overlays unindexed WAL entries for strong reads.
6. **Indexer consumption:** Indexer nodes watch `state.json`, read WAL ranges
   `(indexed_wal_seq, head_seq]`, build L0 segments (IVF vectors, filter/FTS
   artifacts), publish a new manifest, and advance `indexed_wal_seq`.

## Query path (snapshot -> caches -> merge)

1. **Snapshot load:** The query handler reads `state.json` and the current
   manifest. Strong consistency refreshes tail to WAL head; eventual consistency
   reuses a cached snapshot for up to 60s and caps tail scanning at 128 MiB.
2. **Index reads:** The index reader fetches manifest and segment objects,
   backed by NVMe disk cache and RAM hot cache. Cold reads fall back to object
   storage.
3. **Tail overlay:** Tail scans (vector/attr/BM25) run over unindexed WAL data.
   Tail results are merged with index results using WAL sequence ordering so
   newer tail versions override stale indexed documents.
4. **Result shaping:** Filters, aggregations, per-attribute diversification,
   and include/exclude projections are applied to build the final response.

## Cache layers

- **NVMe disk cache:** Content-addressable by `(object_key, etag)`, LRU managed,
  supports pinning namespaces and temperature metrics.
- **RAM cache:** Shard-aware LRU with per-namespace budget caps. Priority tiers
  keep tail data highest, then centroids, filter bitmaps, doc columns.

Cache eviction never breaks correctness; missing objects are re-fetched from
object storage.

## Routing and membership

- **Routing:** Rendezvous hashing maps each namespace to a "home" node for cache
  locality. Any node can still serve any namespace as a fallback.
- **Membership:** A membership manager (static or gossip-based) feeds the router
  with the current node list and updates routing on membership changes.

## Durability and consistency

- **Durability:** A write is durable only after WAL upload and state.json CAS
  update. WAL objects are immutable and are the source of truth for mutations.
- **Consistency modes:**
  - **Strong (default):** refresh tail to WAL head; includes all committed writes
    before the query starts.
  - **Eventual:** may be up to 60s stale; scans at most 128 MiB of tail data.
  - If `disable_backpressure=true` and unindexed data exceeds 2 GB, strong
    queries return 503 while eventual queries continue with the tail cap.

## End-to-end flow (ASCII)

```
Clients
  |
  v
Query Node (router + API handlers)
  |        \
  |         \-- Query path --> state.json + manifest
  |                      |-> index reader (RAM/NVMe cache -> object store)
  |                      |-> tail store (RAM/NVMe -> object store)
  |                      `-> merge + response
  |
  `-- Write path --> validate/canonicalize
                 -> WAL encode (protobuf + zstd)
                 -> PUT wal/*.wal.zst (If-None-Match)
                 -> CAS update state.json (If-Match)
                 -> tail store overlay
Object Storage (system of record)
  |
  `-- Indexer Nodes
        -> poll state.json
        -> read WAL range
        -> build segments + manifest
        -> publish manifest + advance indexed_wal_seq
```
