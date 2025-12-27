# Vex v1.2 — Technical Specification

Open-source, object-storage-first search engine (vector + full-text + hybrid) compatible with turbopuffer's published API, architecture, and behavior.

**Core language:** Go. Clean-room new codebase. Compatibility target: turbopuffer public docs/API semantics as of Dec 2025.

> **Compatibility intent:** Vex aims to match turbopuffer's **documented** API surface and semantics as closely as is practical, while remaining a clean new Go codebase. turbopuffer uses **object storage as the system of record**, an **object-storage-backed WAL**, **asynchronous indexing**, and **stateless query nodes with NVMe+RAM cache**, with strong reads by default and optional eventual reads for performance. ([turbopuffer][1])

---

## 0. Goals, non-goals, and guiding principles

### 0.1 Goals (v1)

1. **Faithful architecture**

   * Object storage is the **durability / system-of-record** layer; writes are durable only once committed to object storage. ([turbopuffer][1])
   * Writes append to an **object-storage WAL** (batched) and are followed by **asynchronous indexing**. ([turbopuffer][2])
   * Query nodes are stateless (or near-stateless) and rely on **NVMe SSD + RAM caching**, with the ability to serve **cold queries directly from object storage**. ([turbopuffer][1])
   * Cache-locality routing: queries/writes for a namespace **tend to route to the same node**, but **any node can serve any namespace**. ([turbopuffer][1])

2. **API compatibility**

   * Implement turbopuffer-style JSON HTTP APIs: auth, write, query (including multi-query), metadata, list namespaces, warm-cache hint, delete namespace, and recall/debug. ([turbopuffer][3])
   * Match key request/response shapes and semantics, including filters, BM25 expressions, multi-queries snapshot isolation, and consistency modes. ([turbopuffer][4])

3. **Correctness-first implementation for LLM-built engineering**

   * Explicit subsystem contracts/invariants.
   * Deterministic tests (goldens + simulation + fault injection).
   * Clear failure handling and safe upgrade story.

4. **Operational simplicity**

   * Minimal critical-path dependencies: **object storage + compute**. ([turbopuffer][1])
   * Easy local dev with a single binary + MinIO.
   * Straightforward debugging: consistent logs, metrics, and introspection.

### 0.2 v1.2 focus

v1.2 MUST fully specify and implement:

* Object-store WAL append/commit model
* Asynchronous indexing pipeline
* Query path for **vector ANN** + **metadata filtering** + **warm-cache routing**
* Strong + eventual consistency semantics and backpressure semantics
* Namespace operations: list, metadata, delete
* Export semantics via query pagination by `id`
* Full-text search and hybrid semantics sufficient for turbopuffer-compatible first-stage retrieval:

  * BM25 rank_by
  * Multi-query (for hybrid search client-side fusion) ([turbopuffer][4])

### 0.3 Non-goals (explicitly deferred)

* Built-in second-stage reranking / ML hosting
* Advanced arbitrary query DAGs beyond turbopuffer's documented "first-stage retrieval" + multi-queries
* Enterprise IAM/private networking beyond a minimal secure baseline
* Multi-region active-active replication

### 0.4 Compatibility contract (normative)

Vex aims to be **wire-compatible** with turbopuffer's published HTTP API:

* Same endpoints/paths
* Same JSON shapes
* Same observable semantics: ordering, limits, backpressure, consistency, filter behavior
* Same error envelope (`{"status":"error","error":"..."}`) for non-2xx ([turbopuffer][3])

If Vex intentionally deviates, it MUST:

* Either be gated behind an explicit config flag (default off), or
* Be exposed as a new Vex-only endpoint/field, not silently accepted.

### 0.5 Must-fix deltas (incorporated in v1.2)

This consolidated spec explicitly matches turbopuffer on:

* **Vector metrics**: compatible `distance_metric` values are **only** `cosine_distance` and `euclidean_squared`. ([turbopuffer][11])

  * "dot product" is NOT part of compatibility. If implemented, it MUST be a **Vex-only extension** (opt-in), and MUST NOT be accepted when `compat_mode=turbopuffer`.
* **Document IDs**: IDs are **u64**, **UUID**, or **strings up to 64 bytes**. ([turbopuffer][13])
* **Attribute names**: up to **128 characters** and **must not start with `$`**. ([turbopuffer][11])
* **Patch semantics**:

  * `vector` **cannot be patched** (row or column patch).
  * Patches to missing IDs are ignored (patch does not create docs).
* **Write ordering within one request**:

  * `delete_by_filter` runs **before** all other operations. ([turbopuffer][11])
  * `patch_by_filter` runs **after delete_by_filter** but **before any other operations**.
* **Allow-partial knobs**:

  * `patch_by_filter_allow_partial` and `delete_by_filter_allow_partial` exist and govern success vs failure and `rows_remaining`. ([turbopuffer][11])
* **Filter-based write isolation**: `patch_by_filter` and `delete_by_filter` are **two-phase with re-evaluation** and behave like **Read Committed** (newly qualifying docs between phases can be missed). ([turbopuffer][1])
* **Backpressure with disable_backpressure**:

  * Normally: writes above **2 GB unindexed** return 429. ([turbopuffer][11])
  * If `disable_backpressure=true`: writes keep succeeding, but **strong queries error** above the threshold; eventual queries search only up to **128 MiB** of unindexed data. ([turbopuffer][11])

---

## 1. Compatibility target: public HTTP API surface

### 1.1 Authentication, encoding, compression

* **Auth:** `Authorization: Bearer <API_KEY>` ([turbopuffer][3])
* **Encoding:** JSON request/response payloads ([turbopuffer][3])
* **Compression:**

  * Accept gzip request bodies (`Content-Encoding: gzip`)
  * Return gzip responses when requested (`Accept-Encoding: gzip`) ([turbopuffer][3])

**Error response format (all endpoints):**

```json
{ "status": "error", "error": "an error message" }
```

([turbopuffer][3])

**HTTP status codes:**

| Code | Meaning |
|------|---------|
| 200 | Success |
| 202 | Accepted (query depends on index still building) |
| 400 | Invalid request / schema / types / duplicate IDs |
| 401/403 | Authentication errors |
| 404 | Namespace not found (where applicable) |
| 413 | Payload too large |
| 429 | Rate limiting / backpressure (unindexed > 2GB) |
| 500 | Internal server error |
| 503 | Object store failures / strong query with disable_backpressure > 2GB |

### 1.2 Endpoints (must implement)

#### Write

* `POST /v2/namespaces/:namespace` — create/update/delete documents; schema updates; conditional writes; delete_by_filter/patch_by_filter; backpressure options. ([turbopuffer][5])

#### Query

* `POST /v2/namespaces/:namespace/query` — vector search (ANN), BM25, order-by attribute, filter-only lookup, aggregations, grouped aggregations, multi-query (snapshot isolation), consistency modes. ([turbopuffer][4])

#### Namespace metadata

* `GET /v1/namespaces/:namespace/metadata` — schema, approx stats, created_at/updated_at, index status and unindexed_bytes. ([turbopuffer][6])

#### Warm cache hint

* `GET /v1/namespaces/:namespace/hint_cache_warm` — hint low-latency usage soon. ([turbopuffer][7])

#### List namespaces

* `GET /v1/namespaces?cursor=&prefix=&page_size=` — pagination and prefix filtering. ([turbopuffer][8])

#### Delete namespace

* `DELETE /v2/namespaces/:namespace` — irreversible. ([turbopuffer][9])

#### Recall (debug)

* `POST /v1/namespaces/:namespace/_debug/recall` — compare ANN vs exhaustive to compute recall. ([turbopuffer][10])

---

## 2. System architecture overview

Vex is split into three roles (can be one binary with modes):

1. **Query Nodes (`vex-query`)**

   * Serve all HTTP APIs (write/query/metadata/warm/list/delete/debug).
   * Maintain local **NVMe cache** for index/doc objects + **RAM hot cache** for frequently used structures.
   * Maintain **namespace-tail materialization** for unindexed data (strong reads).
   * Perform **query execution** and lightweight incremental “cache fill” tasks, but not heavy indexing. ([turbopuffer][1])

2. **Indexer Nodes (`vex-indexer`)**

   * Watch WAL + namespace state, build/compact:

     * Vector ANN index structures
     * Filter/inverted index structures
     * Full-text (BM25) index structures (later phase)
   * Publish new immutable index artifacts to object storage and update namespace index pointers.
   * Do expensive compute; can autoscale independently. ([turbopuffer][1])

3. **Object Storage**

   * The only durable state.
   * Stores:

     * Namespace state/metadata manifests
     * WAL entries
     * Immutable index segments
     * Garbage-collection tombstones
   * Used for concurrency control via conditional operations as needed (ETag / If-Match semantics). ([turbopuffer][1])

---

## 3. Data model

### 3.1 Namespace

* Namespace is an isolated dataset, implicitly created on first write.
* Names must match `[A-Za-z0-9-_.]{1,128}`. ([turbopuffer][5])

### 3.2 Document

* Every document has a unique `id`.
* Attributes are top-level fields (not nested object required) and are nullable; type must remain consistent. ([turbopuffer][11])
* Special vector attribute name: **`vector`**, optional (namespace can have no vectors). ([turbopuffer][11])

### 3.3 Types and schema

Supported types (as per docs):
`string, int(i64), uint(u64), float(f64), uuid(16B), datetime(ms epoch), bool` and array variants. ([turbopuffer][11])

Schema fields (per attribute):

* `type` (required)
* `filterable` (default true; default false if regex or full-text enabled)
* `regex` (default false)
* `full_text_search` (boolean or object with tokenizer/language/etc)
* `vector` object: `{type: "[dims]f16" | "[dims]f32", ann: true/false}` ([turbopuffer][11])

Schema evolution:

* Changing existing attribute type is an error. ([turbopuffer][11])
* `filterable` and `full_text_search` can be updated online.

  * Queries that depend on a newly enabled index return **HTTP 202** until built. ([turbopuffer][11])
  * Full-text parameter changes rebuild in background while queries use old settings until new ready. ([turbopuffer][11])

---

## 4. Object storage layout

All namespace data lives under a prefix:

```
vex/
  namespaces/
    <namespace>/
      meta/
        state.json        # authoritative namespace state (schema, pointers, stats)
        tombstone.json    # present if namespace deleted (for fast rejection)
      wal/
        00000000000000000001.wal.zst
        00000000000000000002.wal.zst
        ...
      index/
        manifests/
          00000000000000000010.idx.json
          00000000000000000011.idx.json
        segments/
          seg_<uuid>/
            docs.col.zst
            vectors.ivf.zst
            filters.<attr>.rbm
            fts.<field>.bm25 (later)
      gc/
        orphan_scan_marker.json
```

### 4.1 Design notes

* **`meta/state.json` is the single "coordination object"** for the namespace. It is small, updated with CAS, and contains all pointers needed for low-roundtrip strong reads.
* WAL entries and index segments are immutable; manifests/pointers provide atomic "publish" points.

### 4.2 Required ObjectStore semantics (explicit dependency)

Vex MUST require an ObjectStore backend with:

1. **Strong read-after-write** for PUT/GET/LIST (or a compensating strategy if not). S3 provides strong consistency for reads and list operations.
2. **Conditional writes (CAS)**:

   * create-if-absent (`If-None-Match: *`)
   * update-if-match (`If-Match: <etag>`)
3. **Well-defined conflict errors**:

   * handle 409/412 on conditional write conflicts (retry with jitter)
4. **Multipart caveats**:

   * ETag may not be MD5 for multipart; Vex MUST store its own checksums (SHA-256) in metadata or file trailer for integrity.

**ObjectStore interface contract:**

```go
type ObjectStore interface {
    Get(key string) (data []byte, etag string, err error)
    Head(key string) (etag string, err error)
    PutIfAbsent(key string, data []byte) (etag string, err error)
    PutIfMatch(key string, etag string, data []byte) (newEtag string, err error)
    List(prefix string, cursor string) (keys []string, nextCursor string, err error)
    Delete(key string) error  // best-effort; deletion not required for correctness if tombstoned
}
```

---

## 5. Namespace state object (`meta/state.json`)

### 5.1 Purpose

A single authoritative state snapshot used by query nodes and indexers to:

* find latest WAL sequence
* find latest published index manifest
* expose metadata endpoint fields
* coordinate schema changes and index build status
* enforce backpressure thresholds (unindexed bytes)

This aligns with turbopuffer’s principle that **object storage is the only stateful dependency** and concurrency control lives there. ([turbopuffer][1])

### 5.2 State schema (versioned)

```json
{
  "format_version": 1,
  "namespace": "my-ns",

  "created_at": "2024-03-15T10:30:45Z",
  "updated_at": "2024-04-16T09:27:32Z",

  "schema": { "...": "..." },

  "vector": {
    "dims": 1536,
    "dtype": "f32",
    "distance_metric": "cosine_distance",
    "ann": true
  },

  "wal": {
    "head_seq": 12345,
    "head_key": "wal/0000000000000012345.wal.zst",
    "bytes_unindexed_est": 104857600,
    "status": "up-to-date" | "updating"
  },

  "index": {
    "manifest_seq": 678,
    "manifest_key": "index/manifests/00000000000000000678.idx.json",
    "indexed_wal_seq": 12200,

    "status": "up-to-date" | "updating",

    "pending_rebuilds": [
      { "kind": "filter", "attribute": "permissions", "requested_at": "...", "ready": false },
      { "kind": "fts", "attribute": "content", "requested_at": "...", "ready": true, "version": 3 }
    ]
  },

  "namespace_flags": {
    "disable_backpressure": false
  },

  "deletion": {
    "tombstoned": false,
    "tombstoned_at": null
  }
}
```

### 5.3 Invariants

* **State monotonicity**

  * `wal.head_seq` strictly increases by 1 per committed WAL entry.
  * `index.indexed_wal_seq` monotonically increases and never exceeds `wal.head_seq`.
* **Publish ordering**

  * A WAL object for `head_seq` MUST exist before `state.json` is updated to reference it.
* **Schema type immutability**

  * Attribute type cannot change once set. ([turbopuffer][11])
* **Index readiness**

  * If `pending_rebuilds` contains not-ready entries for an attribute and the query depends on that index, the query returns **202**. ([turbopuffer][11])

---

## 6. Write path

### 6.1 Behavioral requirements (compatibility)

* **Durable writes:** acknowledge success only after the write is committed to object storage. ([turbopuffer][1])
* **Atomic batches:** all mutations in a request become visible together. ([turbopuffer][1])
* **Conditional writes are atomic with writing.** ([turbopuffer][1])
* **Backpressure behavior:** by default, reject writes when unindexed data > 2 GiB; `disable_backpressure` can override but changes query behavior. ([turbopuffer][11])

### 6.2 API request forms (write)

Vex must support both row-oriented and column-oriented writes, matching turbopuffer:

* `upsert_rows`, `patch_rows`, `deletes`, `delete_by_filter`, `patch_by_filter`
* `upsert_columns`, `patch_columns`
* `schema` object
* `disable_backpressure` boolean
* `distance_metric` (on first write with vectors)
* conditional variants: `upsert_condition`, `patch_condition`, `delete_condition`, including `$ref_new` references (per turbopuffer docs). ([turbopuffer][11])
* `copy_from_namespace` — server-side copy from another namespace (see §6.9)

**Write request ordering (canonical within a single request):**

1. `delete_by_filter` — before all other operations ([turbopuffer][11])
2. `patch_by_filter` — after delete_by_filter, before any other operation
3. `copy_from_namespace` — before explicit upserts/patches/deletes
4. explicit upserts (`upsert_rows` / `upsert_columns`)
5. explicit patches (`patch_rows` / `patch_columns`)
6. explicit deletes (`deletes`)

All of the above are part of one **atomic batch**: no partial visibility before the batch commits. ([turbopuffer][1])

**Conflict resolution (last-write-wins):**

If multiple operations in the same request affect the same ID, later phases in the ordering above MUST win (e.g., delete_by_filter can be "resurrected" by a later upsert in the same request).

Within the same phase:

* For `*_rows` arrays: later occurrences of the same ID SHOULD override earlier ones (last-write-wins within the array).
* For `*_columns`: duplicate IDs MUST be rejected as HTTP 400 (columnar format makes last-write-wins ambiguous).

**Filter-based write limits (hard caps):**

| Operation | Max rows affected |
|-----------|-------------------|
| `patch_by_filter` | 500,000 |
| `delete_by_filter` | 5,000,000 |

**Allow-partial flags:**

* `patch_by_filter_allow_partial` (bool, default false)
* `delete_by_filter_allow_partial` (bool, default false)

Behavior when more rows match than the max limit:

* If `*_allow_partial=false` (default): request MUST fail with HTTP 400
* If `*_allow_partial=true`: request succeeds, applies up to the limit, and response sets `rows_remaining=true` so client can retry ([turbopuffer][11])

### 6.3 Write batching model (1 WAL entry/sec)

To mirror turbopuffer’s behavior:

* Per namespace, Vex commits **at most 1 WAL entry per second**, batching concurrent writes into the same entry. ([turbopuffer][12])

**Implementation detail:**

* A per-namespace in-memory batcher runs on the namespace’s “home” query node (see routing §10).
* Batcher collects requests during a 1s window (or until size threshold), merges them into one WAL record, then commits.

**Commit latency expectation:** if a new batch starts within 1 second of the previous, commit may take up to 1 second (window alignment). ([turbopuffer][12])

### 6.4 WAL entry format (object-store durable record)

#### 6.4.1 Encoding

* Stored as a compressed binary blob (`.wal.zst`).
* Internally encoded as **protobuf** or **CBOR** (preferred: protobuf for stable schema and forward compatibility).
* Compressed with **zstd** (fast decompress) for storage efficiency; gzip optional.

#### 6.4.2 WAL record schema (protobuf-like)

A single WAL entry may contain multiple **sub-batches**, one per HTTP write request. Each sub-batch is atomic and serialized (deterministic order) within the WAL entry. All sub-batches become visible at the same WAL commit boundary.

```
message WalEntry {
  uint32 format_version = 1;
  string namespace = 2;
  uint64 seq = 3;
  int64  committed_unix_ms = 4;

  // Multiple sub-batches (one per HTTP request in the batch window)
  repeated WriteSubBatch sub_batches = 5;

  // Integrity
  bytes  sha256 = 100;            // over uncompressed payload
}

message WriteSubBatch {
  string request_id = 1;          // for idempotency
  int64  received_at_ms = 2;
  SchemaDelta schema_delta = 3;   // optional
  repeated Mutation mutations = 4;

  // For filter-based ops: snapshot info for deterministic replay
  uint64 phase1_snapshot_seq = 10;
  repeated bytes candidate_ids = 11;  // for patch_by_filter/delete_by_filter

  // Response fields (pre-computed for deterministic replay)
  WriteResponseStats stats = 20;
}

message Mutation {
  oneof kind {
    Upsert upsert = 1;
    Patch  patch = 2;
    Delete del = 3;
    DeleteByFilter delete_by_filter = 4;
    PatchByFilter patch_by_filter = 5;
    CopyFromNamespace copy_from = 6;
  }
  // Optional condition evaluated atomically at commit-time:
  FilterExpr condition = 10;
}

message WriteResponseStats {
  int64 rows_affected = 1;
  int64 rows_upserted = 2;
  int64 rows_patched = 3;
  int64 rows_deleted = 4;
  bool  rows_remaining = 5;  // true if filter-based write hit cap
}
```

This sub-batch model avoids "write A sees write B before B is acknowledged" anomalies.

#### 6.4.3 Determinism requirements

* WAL entries must be deterministic for the same batch content:

  * stable sort by docID within each mutation list
  * stable schema delta ordering
* Deterministic encoding is critical for golden tests and reproducible debugging.

#### 6.4.4 Canonicalization (correctness-critical)

Before WAL recording, Vex MUST canonicalize all inputs to ensure deterministic storage and replay:

* **Field ordering**: stable, alphabetical by key within objects
* **Schema inference**: results explicitly recorded in WAL (not re-inferred on replay)
* **ID normalization**: normalize to typed representation (u64/uuid/string) based on input
* **Datetime parsing**: ISO 8601 strings converted to UTC milliseconds since epoch
* **Vector parsing**: float array or base64 decoded to internal float32 array representation
* **Null handling**: explicit null vs missing attribute distinction preserved
* **String encoding**: UTF-8 normalized (NFC recommended)

Canonicalization failures (e.g., invalid datetime format, non-UTF-8 strings) MUST return HTTP 400 before WAL commit.

### 6.5 Commit protocol (object storage durability + state CAS)

**Goal:** succeed only when WAL exists in object store and namespace state is advanced.

Steps (on home query node):

1. Load `state.json` (capture ETag).
2. Validate request(s), infer schema updates, type-check, enforce limits.
3. If `disable_backpressure` false and `state.wal.bytes_unindexed_est` would exceed 2 GiB, return **HTTP 429**. ([turbopuffer][11])
4. Choose `seq = state.wal.head_seq + 1`.
5. Serialize WAL entry to bytes, compute checksum, compress.
6. `PUT wal/<seq>.wal.zst` with **If-None-Match: "*"**.
7. Update `meta/state.json` with **If-Match: <old-etag>**:

   * `wal.head_seq = seq`
   * `wal.head_key = ...`
   * `wal.status = updating` (if becomes non-zero unindexed)
   * `wal.bytes_unindexed_est += logical_bytes_written`
   * `updated_at = now`
   * apply schema delta
8. Return success JSON with fields matching turbopuffer write response:

   * `rows_affected`, `rows_upserted`, `rows_patched`, `rows_deleted`, `rows_remaining` (when filter-based write hits cap) ([turbopuffer][11])

**CAS retry loop:**

* If state update fails due to ETag mismatch:

  * re-read state, re-apply merge logic, retry state update
  * do not re-upload WAL (idempotent by seq+If-None-Match); if WAL key exists unexpectedly, abort and repair logic.

### 6.6 Conditional write semantics

Vex must match turbopuffer semantics:

* Conditions evaluated against the current doc value (or absent doc).
* For upsert:

  * if doc exists and condition met → apply upsert
  * doc exists and condition not met → skip
  * doc missing → apply unconditionally for upserts, skip for patches/deletes ([turbopuffer][11])
* `$ref_new` references allowed inside conditions for per-doc varying conditions. ([turbopuffer][11])
* For `delete_condition`: `$ref_new` attributes are supplied as null (since deletion has no "new" value).
* Conditions are atomic with writing (no lost update anomalies). ([turbopuffer][1])

**Implementation strategy:**

* Evaluate conditions in the writer node during batching using a consistent snapshot view (see §8 snapshot model).
* Commit WAL entry contains only mutations that actually apply (or contains "conditional mutation" records with evaluated outcome); for determinism and replay correctness, record the evaluated outcomes.

### 6.7 Filter-based write isolation (two-phase, Read Committed)

`patch_by_filter` and `delete_by_filter` execute in **two phases**:

1. **Phase 1**: evaluate the filter at a point-in-time snapshot; select matching IDs (bounded by limits)
2. **Phase 2**: atomically re-evaluate filter against those IDs and modify those that still match ([turbopuffer][1])

Observable implication: docs that newly qualify between phases can be missed. Vex MUST reproduce this anomaly surface.

**WAL record requirements for filter-based writes**:

* `phase1_snapshot_seq` — the WAL seq at which phase 1 was evaluated
* `candidate_ids[]` — the IDs selected in phase 1 (bounded by limits)
* the original filter expression
* the patch payload (for patch_by_filter)

During WAL replay (tail reads and indexing), Vex MUST re-evaluate the filter for each candidate ID against the then-current version and apply only if it still matches. This yields deterministic replay.

### 6.8 Request idempotency

Each write request MUST include or be assigned a stable `request_id` (UUID).

Vex MUST implement:

* Request ID de-duplication within a bounded time window (at least "recent batches" per namespace)
* WAL sub-batch record contains `request_id` so replay can detect duplicates
* If a duplicate `request_id` is detected, return the original response (from cache or WAL) rather than re-applying

This ensures safe retries when clients timeout but the write succeeded.

### 6.9 `copy_from_namespace` semantics

Vex MUST implement turbopuffer's `copy_from_namespace` for server-side bulk copy between namespaces.

**Request field:**

```json
{
  "copy_from_namespace": "source-namespace"
}
```

**Semantics:**

* Performs a server-side bulk upsert of all documents from the source namespace into the destination namespace.
* Runs in the write ordering position specified in §6.2 (after `patch_by_filter`, before explicit upserts/patches/deletes).
* Can be combined with other write operations in the same request (e.g., copy then patch specific docs).

**Interaction with export:**

* `copy_from_namespace` is the recommended server-side alternative to client-side export/import.
* For cross-region or cross-cluster copies where `copy_from_namespace` is not available, use the export pattern described in §14.3 (paginated query by `id`).
* Documents inserted in the source namespace during a `copy_from_namespace` operation may or may not be included (point-in-time snapshot semantics).

**Implementation:**

* Treat copy as a special write operation that bulk-upserts source docs into destination.
* Source namespace is read at a consistent snapshot (like a strong query).
* Large copies may be internally batched but appear atomic to the client.

---

## 7. Indexing pipeline

### 7.1 Requirements

* Indexing is **asynchronous** after WAL commit. ([turbopuffer][2])
* Recent unindexed writes must still be searchable:

  * vector queries: include exhaustive search over unindexed docs (strong) ([turbopuffer][4])
* Vector indexing is centroid-based and object-storage-friendly (few round trips), like turbopuffer’s SPFresh approach. ([turbopuffer][2])

### 7.2 Index artifacts

Each published index “generation” consists of:

* `index/manifests/<gen>.idx.json` (small)
* one or more `segments/seg_<uuid>/...` objects (larger, immutable)

**Manifest fields:**

```json
{
  "format_version": 1,
  "namespace": "my-ns",
  "generated_at": "...",
  "indexed_wal_seq": 12200,
  "schema_version_hash": "...",
  "segments": [
    {
      "id": "seg_01H...",
      "level": 0,
      "start_wal_seq": 12100,
      "end_wal_seq": 12200,
      "docs_key": "...",
      "vectors_key": "...",
      "filter_keys": { "category": "...", "permissions": "..." },
      "fts_keys": { "content": "..."}
    }
  ],
  "stats": {
    "approx_row_count": 1234567,
    "approx_logical_bytes": 987654321
  }
}
```

### 7.3 Incremental indexing model (LSM-like)

Segments are immutable. Each segment covers a WAL sequence interval:

* `start_wal_seq` — first WAL entry included in this segment
* `end_wal_seq` — last WAL entry included in this segment
* Contains the consolidated latest versions of docs for that interval (plus tombstones for deletes)

To preserve performance over time:

* **L0 segments** built frequently from WAL batches.
* Periodic **compaction** merges segments into larger L1/L2 segments.
* Query reads across all segments + tail (unindexed WAL) and resolves latest doc versions.

### 7.4 Publishing protocol (atomic visibility)

Indexers must never publish a manifest that references missing or partial objects.

Steps:

1. Read namespace `state.json` and current index manifest pointer.
2. Determine WAL range to index: `(state.index.indexed_wal_seq+1 .. state.wal.head_seq]`.
3. Build new segment(s) and upload all objects with checksums.
4. Upload new manifest with a new generation key.
5. CAS update `state.json` to:

   * set `index.manifest_key = new`
   * advance `index.indexed_wal_seq`
   * update `wal.bytes_unindexed_est` downward (approx)
   * update `index.status`
6. Background GC deletes obsolete segments after a retention delay.

### 7.5 Partial index builds and recovery

* If indexer crashes after uploading some segment objects but before publishing manifest:

  * Those objects are unreachable and safe to GC later.
* GC is driven by:

  * manifest reachability (only delete objects not referenced by any active manifest)
  * minimum retention time (e.g., 24h) to support safe rollbacks.

---

## 8. Query consistency and snapshot model

### 8.1 turbopuffer-compatible semantics

Query API includes a `consistency` object with default strong:

* **Strong:** searches all unindexed writes and updates cache; includes all data written before query started. ([turbopuffer][4])
* **Eventual:** searches up to 128 MiB of unindexed writes; may be stale up to 60 seconds; usually consistent because reads/writes go to same node. ([turbopuffer][4])

### 8.2 Vex snapshot definition

A **query snapshot** is defined by:

* `snapshot_indexed_wal_seq` from published index manifest
* plus a tail window:

  * strong: up to `state.wal.head_seq` as of query start
  * eventual: up to `min(state.wal.head_seq_cached, indexed_wal_seq + tail_cap_bytes)` and may skip refreshing state for up to 60s TTL

### 8.3 Multi-query snapshot isolation

Multi-query request (`queries: [...]`) must be executed:

* “simultaneously and atomically”
* all reads against the same consistent snapshot (“snapshot isolation”) ([turbopuffer][4])

Implementation:

* Resolve snapshot once per request (state + manifest + tail cutoff).
* Execute each subquery against same snapshot.
* Return `results: [...]` in same order as request. ([turbopuffer][4])

### 8.4 Tail materialization (unindexed WAL overlay)

#### 8.4.1 Requirements

* Strong queries must include all committed WAL tail (bounded by 2 GB policy).
* Eventual queries include only up to 128 MiB of tail. ([turbopuffer][4])

#### 8.4.2 Tail storage tiers

Implement TailStore with tiered storage:

* **Tier 0 (RAM):** recently committed WAL sub-batches in decoded columnar form
* **Tier 1 (NVMe):** spill decoded tail blocks to disk (zstd compressed), keyed by `(namespace, wal_seq)`
* **Tier 2 (ObjectStore):** source-of-truth WAL objects

Tail blocks MUST support:

* Vector scan (exact) and optional ANN build for tail (future optimization)
* Filter evaluation via the same filter operators (naive scan acceptable at small scale; indexed tail optional)

#### 8.4.3 Eventual tail window definition

Define the "128 MiB" tail subset deterministically:

* Order unindexed WAL entries by seq descending (newest first)
* Include entries until accumulated compressed WAL bytes ≤ 128 MiB
* This maximizes freshness while matching the "tail cap" spirit. ([turbopuffer][4])

#### 8.4.4 Strong-query error when disable_backpressure

If `disable_backpressure=true` and unindexed bytes > 2 GB:

* Strong query MUST return an error (HTTP 503). ([turbopuffer][11])
* Eventual queries continue to work (only searching up to 128 MiB tail).

---

## 9. Query execution engine

### 9.1 Request fields (must match)

Key query request fields:

* `rank_by` (unless `aggregate_by`) ([turbopuffer][4])
* `filters` (exact filter expressions) ([turbopuffer][4])
* `include_attributes` / `exclude_attributes` ([turbopuffer][4])
* `limit` (or `top_k` alias) with optional `per` diversification for order-by queries ([turbopuffer][4])
* `aggregate_by`, `group_by` ([turbopuffer][4])
* `queries` multi-query ([turbopuffer][4])
* `vector_encoding` (`float` or `base64`) ([turbopuffer][4])
* `consistency` (`strong` default, or `eventual`) ([turbopuffer][4])

### 9.2 Response fields (must match)

* For rank_by queries:

  * `rows: [{ id, "$dist", ...attrs }]` ([turbopuffer][4])
  * `$dist` meaning:

    * ANN: distance from query vector
    * BM25: BM25 score
    * order-by: omitted ([turbopuffer][4])
* For multi-query:

  * `results: [{rows|aggregations|aggregation_groups}, ...]` ([turbopuffer][4])
* For aggregations:

  * `aggregations` or `aggregation_groups` ([turbopuffer][4])
* Include:

  * `billing` object (Vex can return zeros but keep schema) ([turbopuffer][4])
  * `performance` object including cache metrics and timing fields ([turbopuffer][4])

### 9.3 Ranking expressions (`rank_by`)

Supported ranking functions:

* Vector ANN:

  * `["vector", "ANN", <vector>]` ([turbopuffer][4])
* BM25:

  * `["text", "BM25", "query"]` and compositional operators:

    * `["Sum", [...]]`, `["Max", [...]]`
    * `["Product", <weight>, <clause>]` (field boosts)
  * Filters inside rank_by for boosting: filter clause yields score 1 or 0 ([turbopuffer][4])
* Order by attribute:

  * `["timestamp", "desc"]` / `["id", "asc"]` for lookups ([turbopuffer][4])

**Rule:** documents with score zero are excluded from results. ([turbopuffer][4])

### 9.4 Query plan stages (vector + filters)

For the initial focus (vector search with metadata filtering), implement:

1. **Snapshot resolve**

   * load state+manifest per consistency mode.

2. **Filter evaluation**

   * Build a candidate set representation:

     * ideally a roaring bitmap of matching doc IDs for filterable fields.
   * Some filter ops may require scan (glob without prefix) or partial postfilter.

3. **ANN candidate generation**

   * Using a centroid/cluster IVF-style index:

     * load centroids (RAM cache)
     * compute nearest clusters
     * fetch clusters’ posting lists/vectors (NVMe cache or object store range reads)

4. **Recall-aware combination**

   * Intersect ANN candidates with filter candidates.
   * If too few results, increase probe/candidate budget adaptively.
   * If filter is very selective, fall back to exact search over filtered docs.

5. **Tail merge (unindexed)**

   * Strong: exhaustively search tail docs and merge into top-k.
   * Eventual: only search up to 128MiB tail window. ([turbopuffer][4])

6. **Deduplication (last-write-wins)**

   Because segments are immutable and updates create newer versions:

   * Query MUST treat the highest WAL seq version as authoritative.
   * Implementation: merge results from newest segments/tail first and keep first occurrence per ID.
   * Tombstones (deletes) must be respected: if the newest version is a delete, exclude the doc.

7. **Fetch attributes**

   * Read requested attributes from columnar docs store (segment) and overlay latest values from tail.

8. **Return rows** with `$dist` and selected attributes.

---

## 10. Filtering subsystem

### 10.1 Filter AST and semantics

Filters are expressed as arrays:

* boolean operators:

  * `["And", [f1, f2, ...]]`
  * `["Or",  [f1, f2, ...]]`
  * `["Not", f]` ([turbopuffer][4])
* comparisons:

  * `["attr", "Eq", value]` (value can be null meaning missing attr) ([turbopuffer][4])
  * `NotEq` (null meaning "attribute present") ([turbopuffer][4])
  * `In`, `NotIn` (set membership)
  * `Lt`, `Lte`, `Gt`, `Gte` (strings lexicographic; datetimes numeric in ms) ([turbopuffer][4])
  * array ops: `Contains`, `NotContains`, `ContainsAny`, `NotContainsAny`, `AnyLt`, `AnyLte`, `AnyGt`, `AnyGte` ([turbopuffer][4])
  * `Glob`, `NotGlob`, `IGlob`, `NotIGlob` (Unix-style globset semantics; prefix globs compile to range queries, otherwise scan) ([turbopuffer][4])
  * `Regex` (string): requires `regex: true` in schema; may require exhaustive evaluation; avoid large namespaces unless selective filters exist ([turbopuffer][4])
* full-text-ish filters:

  * `ContainsTokenSequence`
  * `ContainsAllTokens` with optional `{ "last_as_prefix": true }` ([turbopuffer][4])

### 10.2 Index structures

Per attribute (when `filterable: true`):

* **Scalar / exact values:** map value → roaring bitmap of docIDs
* **Range-friendly (numeric/datetime):**

  * either value→bitmap + per-block min/max for pruning
  * or sorted (value, docID) with compressed blocks and galloping intersection
* **Arrays:** inverted mapping element → bitmap
* **Glob:**

  * if prefix-literal exists: implement as range query on sorted term dictionary
  * else: full scan over candidate set
* **Regex** (when `regex: true` in schema):

  * No specialized index; requires exhaustive evaluation over candidate set
  * SHOULD be combined with other selective filters to reduce scan scope
  * Warn in docs: avoid on large namespaces without additional filtering

### 10.3 Recall-aware filtering (ANN + filters)

Per turbopuffer docs: vector queries combine attribute index and ANN index “for best performance and recall” and filtering is “recall-aware” for vector queries. ([turbopuffer][4])

Vex algorithm:

* Estimate filter selectivity:

  * compute bitmap cardinality (approx) for filter candidate set.
* Choose strategy:

  * If candidate_count ≤ exact_threshold → exact vector search over candidates.
  * Else:

    * ANN search with candidate oversampling:

      * target_candidates = top_k * oversample_factor (adaptive)
      * probe clusters until either:

        * enough filtered hits found, or
        * max_probes reached
* Always compute exact distances for final top_k.

---

## 11. Vector indexing subsystem (SPFresh-like, object-store friendly)

### 11.1 Design constraint

Object storage has high RTT; ANN must minimize round trips. turbopuffer uses clustered (centroid-based) indexes (SPFresh) optimized for object storage, where a cold query loads centroids, then fetches a small set of clusters via ranged reads. ([turbopuffer][2])

### 11.2 Vex ANN index: IVF (centroid clusters) with packed cluster data

#### 11.2.1 Files

For each vector segment:

* `vectors.centroids.bin` (small, cache in RAM)
* `vectors.clusters.pack` (large, packed clusters back-to-back)
* `vectors.cluster_offsets.bin` (cluster → (offset, length, doc_count), small)
* Optional quantization file (later): PQ codes for compression.

#### 11.2.2 Query flow (cold)

1. GET centroids + offsets (small objects)
2. Compute nearest `nprobe` centroids
3. Multi-range GET (or sequential range GET) for the union of selected cluster ranges
4. Compute distances and select top_k

This matches the “few RTT” clustered index pattern described in turbopuffer’s architecture. ([turbopuffer][2])

### 11.3 Incremental updates

* New vectors land in WAL tail immediately and are searchable by exhaustive scan (strong).
* Indexer folds vectors into IVF segments asynchronously:

  * L0 segments: build small IVF quickly.
  * Compaction: merge and (optionally) recluster.

### 11.4 Distance metrics

**Compatibility-required:**

* `cosine_distance` (default)
* `euclidean_squared`

These are the only values turbopuffer documents. Vex MUST support both.

**Vex-only extension (optional, must be gated):**

* `dot_product` — if implemented, MUST be gated behind `compat_mode != turbopuffer` config flag and MUST NOT be accepted by default. Not part of turbopuffer compatibility.

(Exact naming must match turbopuffer's public expectations; implement alias mapping as needed.)

### 11.5 Recall measurement

Implement recall endpoint:

* sample `num` vectors
* run ANN top_k and exhaustive top_k
* compute avg_recall, avg_ann_count, avg_exhaustive_count ([turbopuffer][10])

---

## 12. Full-text search (BM25) and hybrid search

> **Implementation order note:** This is phase 2+ per your request; include now for spec completeness.

### 12.1 Enabling full-text search in schema

* `full_text_search: true` or object with:

  * tokenizer (default `word_v3`)
  * case_sensitive
  * language (default english)
  * stemming
  * remove_stopwords
  * ascii_folding
  * BM25 params k1, b ([turbopuffer][11])

### 12.2 BM25 query semantics

* `rank_by` clause: `["field", "BM25", "query", {options}]`
* Supported operators:

  * `Sum`, `Max` combine clause scores
  * `Product` applies boosts/weights
  * filters in rank_by act as conditional boost (score 1/0) ([turbopuffer][4])

### 12.3 Phrase and prefix semantics

* `ContainsTokenSequence` for adjacent ordered token phrases (partial postfilter may reduce recall) ([turbopuffer][4])
* `ContainsAllTokens` regardless of adjacency
* `last_as_prefix: true` supports typeahead prefix in BM25 and filters; BM25 prefix matches score 1.0 ([turbopuffer][4])

### 12.4 Hybrid search

turbopuffer emphasizes multi-queries for hybrid search; results combined client-side with RRF or other fusion. ([turbopuffer][4])

Vex must:

* support multi-query execution on one snapshot
* provide example/reference client fusion strategy (RRF) in docs and SDKs

---

## 13. Caching, warm performance, and routing

### 13.1 Cache tiers (pufferfish-like)

* **Object storage:** cold, durable
* **NVMe SSD cache:** warm
* **RAM cache:** hot

After a cold query, data is cached on NVMe; frequently accessed namespaces move into memory. ([turbopuffer][1])

#### 13.1.1 NVMe disk cache

A content-addressable cache keyed by `(object_key, etag)` → local filepath.

**Eviction policy:**

* Size budget configurable (default 95% of disk)
* LRU by bytes with access-time tracking
* Pinned set for actively warming namespaces (not evictable during warm)

#### 13.1.2 RAM memory cache

* Hot index structures (ANN centroids, posting dictionaries, bitmap shards)
* Small working-set of decoded vectors for tail and recent segments

**Eviction policy:**

* Shard-aware LRU (evict entire shards, not partial structures)
* **Per-namespace budget caps** to protect multi-tenancy (no single namespace can consume > X% of RAM cache)
* Priority tiers: tail data > centroids > filter bitmaps > doc columns

#### 13.1.3 Cache invariant

Eviction MUST never affect correctness: any missing cached object is fetched from object storage. Cold path always works.

### 13.2 Cache-locality routing

Requirement: queries for a namespace should tend to route to the same node, but any node can serve any namespace. ([turbopuffer][1])

**Vex routing design:**

* Use **Rendezvous hashing** on `namespace` across current membership list.
* Each node computes:

  * `home_node(namespace)`
* When a request arrives:

  * If self == home → serve locally
  * Else → proxy to home (best-effort, bounded timeout), with fallback to local serve if proxy fails (correctness first).

**Write routing:**

Same applies for writes: proxying writes to the home node reduces CAS contention on `state.json` by concentrating updates on a single node's microbatcher.

### 13.3 Membership

Min-dependency approach:

* Static list in config (good for small clusters / k8s headless service)
* Optional gossip membership (Hashicorp memberlist) for dynamic clusters

### 13.4 Warm cache hint endpoint

Implement:

* `GET /v1/namespaces/:namespace/hint_cache_warm` ([turbopuffer][7])
  Behavior:
* route to home node
* enqueue a “cache warm task”:

  * fetch state + manifest
  * prefetch:

    * centroids
    * cluster offsets
    * hottest filter bitmaps
    * doc column headers
* Respond immediately (200) with empty success payload.

---

## 14. Namespace metadata and listing

### 14.1 `GET /v1/namespaces/:namespace/metadata`

Return:

* `schema`
* `approx_logical_bytes`
* `approx_row_count`
* `created_at`, `updated_at`
* `encryption` (Vex: default SSE true; CMEK optional future)
* `index` object:

  * status `updating` or `up-to-date`
  * `unindexed_bytes` when updating ([turbopuffer][6])

### 14.2 `GET /v1/namespaces`

Support:

* `cursor`
* `prefix`
* `page_size` default 100, max 1000
* Response:

  * `namespaces: [{id: "..."}]`
  * `next_cursor` if more ([turbopuffer][8])

Implementation:

* Maintain a lightweight namespace catalog in object storage:

  * on first namespace creation, write `catalog/namespaces/<namespace>` (empty object)
* Listing uses `ListObjects` with prefix and pagination token.

### 14.3 Export semantics

No dedicated export endpoint. Exporting "all docs" is done by paging the query API by advancing a filter on `id`:

1. Query with `rank_by: ["id", "asc"]` and `limit: N`
2. For next page: add filter `["id", "Gt", <last_id>]`
3. Repeat until fewer than N results returned

Documents inserted during export will be included (not snapshot isolation across pages). ([turbopuffer][8])

---

## 15. Delete namespace

### 15.1 Behavior

`DELETE /v2/namespaces/:namespace` irreversibly deletes namespace and all documents. ([turbopuffer][9])

### 15.2 Implementation (safe + simple)

Object storage deletes are non-atomic and can be large; Vex should:

1. Write `meta/tombstone.json` with deletion timestamp (CAS-protected).
2. Update `state.json` with `deletion.tombstoned=true`.
3. Immediately begin background GC:

   * delete all keys under `namespaces/<namespace>/` except tombstone/state until final.
4. API returns 200 once tombstone is committed.

Serving behavior:

* After tombstone, all reads/writes return 404-style error (“namespace deleted”).

---

## 16. Limits, backpressure, and guardrails

Match documented turbopuffer limits where practical:

* max upsert batch request size: 256MB ([turbopuffer][13])
* max write batch rate: 1 batch/s per namespace (Vex mirrors) ([turbopuffer][13])
* unindexed data cap: 2GB; write backpressure 429 unless disable_backpressure ([turbopuffer][13])
* query concurrency per namespace: default 16 ([turbopuffer][13])
* max multi-query: 16 ([turbopuffer][13])
* max limit.total / top_k: 10,000 ([turbopuffer][13])

Guardrails for “many namespaces”:

* Per-namespace in-memory state is demand-loaded and evictable.
* Hard caps on:

  * tail materialization memory per namespace
  * concurrent cold cache fills
  * per-request CPU budget (timeouts)

---

## 17. Failure modes and recovery behavior

### 17.1 Object storage unavailable

turbopuffer prioritizes consistency over availability when object storage is unreachable, but can adjust via query configuration. ([turbopuffer][1])

Vex behavior:

* **Writes:** fail (503) if object store write fails.
* **Strong queries:** fail if snapshot/state cannot be refreshed.
* **Eventual queries:** may serve from cache if allowed and snapshot not too stale; otherwise fail.

### 17.2 Partial WAL commit

* WAL upload succeeded but state update failed:

  * writer retries state CAS
  * if writer crashed, **repair task** can:

    * list WAL objects, detect highest contiguous seq
    * advance state head_seq safely
  * maximum staleness should be bounded (target 60s) similar to turbopuffer’s stated bound. ([turbopuffer][1])

### 17.3 Partial index build

* Never publish manifest until all objects uploaded.
* Orphan objects GC later.

### 17.4 Cache eviction

* Eviction never affects correctness:

  * any missing cached object must be fetched from object storage.
* Cache temperature metrics:

  * `hot|warm|cold` based on hit ratio ([turbopuffer][4])

### 17.5 Node churn / routing changes

* Requests can be served by any node, but routing aims for locality.
* On routing change:

  * warm cache may be cold on new node
  * strong consistency still holds (reads state+tail)
  * eventual may become briefly stale until next refresh

---

## 18. Deterministic testing strategy (correctness-first)

### 18.1 Test layers

#### A) Pure unit tests (deterministic)

* Schema inference and type checking
* Filter AST evaluation semantics (including null semantics) ([turbopuffer][4])
* Rank_by expression evaluation (BM25 Sum/Max/Product; filter boosts) ([turbopuffer][4])
* Vector encoding base64 float32 little-endian roundtrip ([turbopuffer][11])

#### B) Golden API tests

* Given a fixed dataset and fixed queries:

  * exact expected JSON responses (golden files)
  * cover include/exclude attributes, limit, aggregation, multi-query response layout ([turbopuffer][4])

#### C) Deterministic simulation tests (state machine)

Create an in-memory deterministic object store + deterministic scheduler:

* simulate:

  * concurrent writes and CAS retries
  * node crashes between WAL upload and state update
  * indexer crashes mid-build
  * cache evictions
* assert invariants:

  * monotonic wal seq
  * snapshot correctness for strong reads
  * no lost updates for conditional writes

#### D) Fault injection tests

* Inject object store errors:

  * transient 500
  * timeouts
  * partial reads
* Ensure:

  * strong queries fail “loudly” when required
  * eventual queries degrade gracefully (where permitted)
  * background repair restores state

### 18.2 Compatibility tests against turbopuffer semantics

Build a "compat suite" that:

* Executes documented request shapes and validates:

  * HTTP status codes (400/401/202/429)
  * response field presence/shape
  * semantic invariants (e.g., `Eq null` matches missing) ([turbopuffer][4])

### 18.3 Compatibility harness against turbopuffer (optional)

If available (CI job with turbopuffer API key):

* Run identical golden tests against turbopuffer and Vex
* Compare:

  * Success/failure behavior (status codes)
  * Error message patterns (tolerant to exact wording)
  * Ordering semantics (filter-based ordering anomalies)
  * `rows_remaining` behavior for allow_partial
  * Consistency mode behavior and tail caps (observational)
  * For ANN: allow recall tolerance (compare recall@k rather than exact result equality)

---

## 19. Observability and operational UX

### 19.1 Logging

* Structured JSON logs with:

  * request_id
  * namespace
  * endpoint
  * cache temperature
  * timings (server_total_ms, query_execution_ms) ([turbopuffer][4])

### 19.2 Metrics (Prometheus)

* Per namespace:

  * query concurrency
  * tail bytes / unindexed bytes
  * cache hits/misses
  * index lag (wal_head - indexed_wal_seq)
* Global:

  * object store ops and latency
  * WAL commit latency

### 19.3 Debug endpoints (non-public)

* `GET /_debug/state/:namespace`
* `GET /_debug/cache/:namespace`
* `GET /_debug/wal/:namespace?from_seq=...`
* gated behind admin auth

---

## 20. Safe upgrades and format versioning

### 20.1 Principles

* Every on-disk/on-object format is versioned.
* Query nodes must be able to read at least N-1 versions for rolling upgrades.
* Indexer publishes new formats only when all query nodes in cluster support them (config flag / rollout step).

### 20.2 Upgrade strategy

* Step 1: deploy new query nodes supporting both old and new formats
* Step 2: deploy new indexers that start writing new format
* Step 3: after retention window, GC old format segments

---

## 21. Recommended implementation order (manageable chunks)

This order front-loads correctness and compatibility, and matches your “start focus” requirements.

### Phase 0 — Foundations (weekend-sized units)

1. **Repository + build**

   * single Go module
   * single binary `vex` with subcommands
2. **Object store abstraction**

   * S3-compatible via AWS SDK
   * local filesystem “object store” for unit tests
3. **API scaffolding**

   * net/http + router
   * auth middleware (Bearer token)
   * gzip request/response support ([turbopuffer][3])

### Phase 1 — WAL + strong reads (core correctness)

4. **Namespace state (`state.json`) + CAS helper**

   * ETag-based CAS loop
5. **Write API (minimal)**

   * upsert_rows + deletes + schema inference
   * WAL entry creation and commit
   * metadata endpoint fields updated (created_at/updated_at)
6. **Tail materialization**

   * query node reads WAL tail and builds in-memory “tail store”
7. **Query API (vector-only, exact)**

   * exact exhaustive vector search (no ANN yet)
   * filters (Eq/In/And/Or/Not + a few array ops)
   * include/exclude attributes, limit/top_k ([turbopuffer][4])
8. **Consistency modes**

   * strong default: refresh state+tail and include all tail
   * eventual: TTL cached snapshot and 128MiB tail cap ([turbopuffer][4])

> At the end of Phase 1 you have a correct, durable, object-store-first vector search engine (exact search), with turbopuffer-like consistency semantics and request shapes.

### Phase 2 — Warm routing + cache (latency architecture)

9. **Routing layer**

   * rendezvous hashing
   * request proxy to home node + fallback
10. **NVMe cache**

* on-disk cache with size budget and LRU metadata

11. **Warm hint endpoint**

* implement `hint_cache_warm` and background prefetch ([turbopuffer][7])

### Phase 3 — ANN indexing pipeline (SPFresh-like)

12. **Indexer process**

* watches state, processes WAL ranges

13. **IVF/centroid ANN segment format**

* centroids + packed clusters + offsets

14. **Query ANN path**

* cold reads via object store range GET
* warm reads from NVMe/RAM

15. **Recall endpoint**

* implement recall computation and reporting ([turbopuffer][10])

### Phase 4 — Filter indexes (performance + native filtering)

16. **Filterable attribute inverted indexes**

* roaring bitmaps
* recall-aware planner for ANN+filters ([turbopuffer][11])

17. **Backpressure**

* enforce 2GiB unindexed cap + disable_backpressure behavior ([turbopuffer][11])

### Phase 5 — Full-text + hybrid

18. **Tokenization + BM25 index segments**
19. **BM25 rank_by expressions**

* Sum/Max/Product, rank_by filter boosts, prefix options ([turbopuffer][4])

20. **Multi-query**

* snapshot isolation, results array ([turbopuffer][4])

21. **Hybrid examples + client helpers**

* RRF in client SDKs

### Phase 6 — “Full turbopuffer-feel”

22. **Order-by attribute queries**
23. **Aggregations + group_by**
24. **List namespaces / delete namespace hardening** ([turbopuffer][8])
25. **Schema index rebuild + HTTP 202 gating** ([turbopuffer][11])

---

## 22. Subsystem contracts (quick reference)

### WAL & durability

* **Contract:** successful write ⇒ WAL entry exists in object store. ([turbopuffer][1])
* **Invariant:** WAL seq strictly increases; WAL entries immutable.
* ACK success only after WAL object PUT + state.json CAS update.
* state.json is authoritative; WAL entries are source of truth for mutations.

### Write semantics

* **Ordering:** delete_by_filter → patch_by_filter → copy_from_namespace → upserts → patches → deletes ([turbopuffer][11])
* `vector` cannot be patched; patches to missing IDs ignored.
* **Duplicate IDs:** `*_rows` uses last-write-wins; `*_columns` returns 400.
* Filter-based writes are two-phase + re-eval (Read Committed anomalies). ([turbopuffer][1])
* `delete_condition` with `$ref_new` supplies null attributes.
* Request idempotency via `request_id` in WAL sub-batches.

### Query consistency

* **Strong (default):** includes all writes committed before query start; refreshes cache. ([turbopuffer][4])
* **Eventual:** may be stale ≤ 60s, scans ≤ 128MiB tail. ([turbopuffer][4])
* Eventual tail cap 128 MiB; disable_backpressure strong-query error > 2GB. ([turbopuffer][11])

### Routing/cache

* **Contract:** routing prefers cache locality; any node can serve any namespace. ([turbopuffer][1])
* **Invariant:** cache eviction never breaks correctness.

### Index publishing

* **Contract:** published manifest references only fully uploaded objects.
* **Invariant:** `indexed_wal_seq` monotonic.

### Schema

* **Invariant:** attribute type immutable; enabling indexes may return 202 until ready. ([turbopuffer][11])

### Multi-tenancy guardrails

* Enforce limits; fairness (per-namespace concurrency). ([turbopuffer][13])
* Max limits: 256MB request, 1 batch/s, 2GB unindexed, 64 byte IDs, 128 char attr names, 256 attrs per namespace.

---

## 23. Appendix: Key invariants checklist

Quick reference for implementation validation:

### Durability invariants

- [ ] WAL entry exists in object store BEFORE returning success to client
- [ ] state.json CAS update succeeds BEFORE returning success to client
- [ ] WAL seq strictly increases (no gaps, no duplicates)
- [ ] WAL entries are immutable once written

### Write semantics invariants

- [ ] Operation ordering: delete_by_filter → patch_by_filter → copy_from → upserts → patches → deletes
- [ ] `vector` attribute cannot be patched (returns 400)
- [ ] Patches to missing IDs are silently ignored (no doc created)
- [ ] Duplicate IDs in `*_rows`: last-write-wins within the array
- [ ] Duplicate IDs in `*_columns`: return 400
- [ ] Filter-based writes use two-phase with re-evaluation (Read Committed)

### Consistency invariants

- [ ] Strong query includes ALL committed WAL entries at query start
- [ ] Eventual query may be stale ≤ 60s, scans ≤ 128 MiB tail
- [ ] Multi-query executes all subqueries against same snapshot
- [ ] `disable_backpressure=true` + unindexed > 2GB → strong queries return 503

### Schema invariants

- [ ] Attribute type cannot change once set (returns 400)
- [ ] Query depending on not-yet-built index returns 202
- [ ] `$` prefix in attribute names rejected (returns 400)

### Index publishing invariants

- [ ] Manifest references only fully-uploaded segment objects
- [ ] `indexed_wal_seq` monotonically increases
- [ ] Orphan objects (from crashed builds) are safe to GC

### Cache invariants

- [ ] Eviction never breaks correctness (cold path always works)
- [ ] Per-namespace memory caps enforced for multi-tenancy fairness

---

If you want, I can also provide:

* A concrete JSON Schema (machine-checkable) for each endpoint request/response
* A suggested Go package layout and interfaces (ObjectStore, NamespaceState, WAL, SegmentReader, Planner, Cache)
* A full deterministic simulation test plan with a “virtual time” scheduler and object store fault injector (very effective for LLM-driven implementation)

[1]: https://turbopuffer.com/docs/guarantees "Guarantees"
[2]: https://turbopuffer.com/architecture "Architecture"
[3]: https://turbopuffer.com/docs/auth "API Overview"
[4]: https://turbopuffer.com/docs/query "Query documents"
[5]: https://turbopuffer.com/docs/write?utm_source=chatgpt.com "Write Documents"
[6]: https://turbopuffer.com/docs/metadata "Metadata"
[7]: https://turbopuffer.com/docs/warm-cache "Warm cache"
[8]: https://turbopuffer.com/docs/namespaces "List namespaces"
[9]: https://turbopuffer.com/docs/delete-namespace "Delete namespace"
[10]: https://turbopuffer.com/docs/recall "Evaluate recall"
[11]: https://turbopuffer.com/docs/write "Write Documents"
[12]: https://turbopuffer.com/architecture?utm_source=chatgpt.com "Architecture"
[13]: https://turbopuffer.com/docs/limits "Limits"
