# Namespace APIs

## List namespaces

`GET /v1/namespaces`

Returns catalog entries for namespaces.

Query parameters:

- `cursor` (string, optional): pagination cursor from `next_cursor`.
- `prefix` (string, optional): only return namespaces whose names start with this prefix.
- `page_size` (int, optional): number of entries to return. Default 100, max 1000.

Response fields:

- `namespaces` (array): list of `{ "id": "<namespace>" }` objects.
- `next_cursor` (string, optional): opaque cursor for the next page. Omitted when no more results.

Example response:

```json
{
  "namespaces": [
    {"id": "alpha"},
    {"id": "beta"}
  ],
  "next_cursor": "catalog/namespaces/beta"
}
```

## Namespace metadata

`GET /v1/namespaces/{namespace}/metadata`

Returns metadata derived from the namespace state.

Response fields:

- `namespace` (string): namespace name.
- `approx_row_count` (int): approximate row count from the latest index manifest (0 if none).
- `approx_logical_bytes` (int): approximate logical bytes from the latest manifest (0 if none).
- `created_at` (string): ISO-8601 timestamp with millisecond precision.
- `updated_at` (string): ISO-8601 timestamp with millisecond precision.
- `encryption` (object): currently `{ "sse": true }`.
- `index` (object): index status information.
  - `status`: `"up-to-date"` or `"updating"` (when WAL head is ahead of indexed WAL).
  - `unindexed_bytes` (int, optional): bytes estimated to be unindexed (present only when updating).
- `schema` (object, optional): schema definition if present.
  - `attributes` (object): map of attribute name to schema.
    - `type` (string): data type (`string`, `int`, `uint`, `float`, `bool`, `datetime`, `uuid`, or array variants).
    - `filterable` (bool, optional): filterable flag.
    - `regex` (bool, optional): regex enabled for the attribute.
    - `full_text_search` (bool/object, optional): full text options.
    - `vector` (object, optional): vector field config (`type`, `ann`).

Example response:

```json
{
  "namespace": "products",
  "approx_row_count": 1240,
  "approx_logical_bytes": 987654,
  "created_at": "2026-01-07T12:00:00.123Z",
  "updated_at": "2026-01-07T12:34:56.789Z",
  "encryption": {"sse": true},
  "index": {
    "status": "updating",
    "unindexed_bytes": 1048576
  },
  "schema": {
    "attributes": {
      "title": {"type": "string", "full_text_search": true},
      "price": {"type": "float", "filterable": true}
    }
  }
}
```

## Warm cache hint

`GET /v1/namespaces/{namespace}/hint_cache_warm`

Asks the server to warm cache entries for the namespace. The request is enqueued
asynchronously and returns immediately.

Response:

- HTTP 200 with an empty body.

## Delete namespace

`DELETE /v2/namespaces/{namespace}`

Deletes a namespace (tombstones it in object storage).

Response:

```json
{"status": "ok"}
```
