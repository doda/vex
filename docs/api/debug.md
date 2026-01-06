# Debug APIs

Debug endpoints expose internal state and recall measurements. All debug endpoints
require standard bearer auth, and admin-only endpoints require the admin token.

## Recall measurement

`POST /v1/namespaces/{namespace}/_debug/recall`

Runs an ANN vs exhaustive recall comparison against sampled vectors in the
namespace. Uses strong consistency (tail refresh to WAL head) before sampling.

Request body:

- `num` (int, optional): number of vectors to sample. Default 10.
- `top_k` (int, optional): number of neighbors to compare. Default 10.

If the body is empty or fields are `<= 0`, defaults are applied.

Response fields:

- `avg_recall` (float): average recall across sampled vectors.
- `avg_ann_count` (float): average number of ANN results returned.
- `avg_exhaustive_count` (float): average number of exhaustive results returned.

If no vectors exist in the namespace, the response is
`avg_recall=1.0`, `avg_ann_count=0`, `avg_exhaustive_count=0`.

Example request:

```json
{
  "num": 25,
  "top_k": 20
}
```

Example response:

```json
{
  "avg_recall": 0.92,
  "avg_ann_count": 20,
  "avg_exhaustive_count": 20
}
```

Expected errors:

- 400: invalid JSON body.
- 404: namespace not found or deleted.
- 413: request body exceeds 256MB.
- 503: object store unavailable or recall computation failed.

## Debug namespace state (admin only)

`GET /_debug/state/{namespace}`

Returns the full namespace `state.json` contents.

Response fields (top level):

- `format_version` (int)
- `namespace` (string)
- `created_at` (string, ISO-8601)
- `updated_at` (string, ISO-8601)
- `schema` (object, optional)
- `vector` (object, optional)
- `wal` (object): `head_seq`, `head_key`, `bytes_unindexed_est`, `status`
- `index` (object): `manifest_seq`, `manifest_key`, `indexed_wal_seq`, `status`,
  `pending_rebuilds`
- `namespace_flags` (object): `disable_backpressure`
- `deletion` (object): `tombstoned`, `tombstoned_at`

Example response:

```json
{
  "format_version": 1,
  "namespace": "products",
  "created_at": "2026-01-07T12:00:00.123Z",
  "updated_at": "2026-01-07T12:34:56.789Z",
  "schema": {
    "attributes": {
      "title": {"type": "string", "full_text_search": true},
      "price": {"type": "float", "filterable": true}
    }
  },
  "vector": {
    "dims": 768,
    "dtype": "f32",
    "distance_metric": "cosine_distance",
    "ann": true
  },
  "wal": {
    "head_seq": 120,
    "head_key": "vex/namespaces/products/wal/000000000120.wal.zst",
    "bytes_unindexed_est": 1048576,
    "status": "up-to-date"
  },
  "index": {
    "manifest_seq": 12,
    "manifest_key": "vex/namespaces/products/index/manifest-000000000012.json",
    "indexed_wal_seq": 118,
    "status": "updating",
    "pending_rebuilds": []
  },
  "namespace_flags": {
    "disable_backpressure": false
  },
  "deletion": {
    "tombstoned": false
  }
}
```

Expected errors:

- 401: missing or invalid `Authorization` header.
- 403: admin token not configured or token is invalid.
- 404: namespace not found or deleted.
- 500: state load failed.
- 503: object store unavailable.

## Test state injection (admin only)

`POST /_test/state`

Sets in-memory server state for integration tests (namespaces and object store
availability). Payload fields map to the `ServerState` struct, so keys use the
Go field names (`Namespaces`, `ObjectStore`, etc.).

Request body:

- `Namespaces` (object): map of namespace -> state.
  - `Exists` (bool)
  - `Deleted` (bool)
  - `IndexBuilding` (bool)
  - `UnindexedBytes` (int)
  - `BackpressureOff` (bool)
- `ObjectStore` (object):
  - `Available` (bool)

Example request:

```json
{
  "Namespaces": {
    "demo": {
      "Exists": true,
      "Deleted": false,
      "IndexBuilding": false,
      "UnindexedBytes": 0,
      "BackpressureOff": false
    }
  },
  "ObjectStore": {
    "Available": true
  }
}
```

Example response:

```json
{"status": "ok"}
```

Expected errors:

- 400: invalid JSON body.
- 401: missing or invalid `Authorization` header.
- 403: admin token not configured or token is invalid.
