# Write API

## Endpoint

`POST /v2/namespaces/{namespace}`

Creates or mutates documents in a namespace. Namespaces are created implicitly on
first write.

This endpoint requires the standard bearer token (see `docs/api/overview.md`).

## Request body

A single JSON object with any combination of mutation fields. All operations in
a request are applied atomically in a single batch.

Top-level fields:

- `request_id` (string, optional): idempotency key. If omitted, the server
  assigns one. If present in the body, it overrides any request id assigned by
  the transport.
- `upsert_rows` (array of objects): row-oriented upserts. Each object must have
  `id` and may include arbitrary attributes plus `vector`.
- `upsert_columns` (object): columnar upserts. Must include `ids`, with each
  attribute key mapping to an array of equal length.
- `patch_rows` (array of objects): row-oriented patches. Each object must have
  `id` and the attributes to update. Missing IDs are ignored. `vector` cannot be
  patched.
- `patch_columns` (object): columnar patches. Must include `ids`, with each
  attribute key mapping to an array of equal length. `vector` cannot be patched.
- `deletes` (array): list of document IDs to delete.
- `delete_by_filter` (filter array): filter expression to select docs to delete.
- `delete_by_filter_allow_partial` (bool, default false): allow partial deletes
  when over the cap.
- `patch_by_filter` (object): `{ "filter": <filter>, "updates": { ... } }`.
  `vector` cannot be patched.
- `patch_by_filter_allow_partial` (bool, default false): allow partial patches
  when over the cap.
- `copy_from_namespace` (string): bulk upsert all docs from another namespace
  using a consistent snapshot.
- `schema` (object): explicit schema updates; see "Schema updates".
- `upsert_condition` (filter array): condition evaluated against the current doc
  to decide whether an upsert applies.
- `patch_condition` (filter array): condition evaluated against the current doc
  to decide whether a patch applies.
- `delete_condition` (filter array): condition evaluated against the current doc
  to decide whether a delete applies.
- `disable_backpressure` (bool, default false): bypass the 2 GiB unindexed data
  threshold.
- `distance_metric` (string, optional): vector distance metric to set on the
  first write with vectors (`cosine_distance`, `euclidean_squared`, or
  `dot_product` when compat mode is `vex`).

## Mutation ordering

Write operations execute in a fixed order within a single request:

1. `delete_by_filter`
2. `patch_by_filter`
3. `copy_from_namespace`
4. explicit upserts (`upsert_rows` / `upsert_columns`)
5. explicit patches (`patch_rows` / `patch_columns`)
6. explicit deletes (`deletes`)

If multiple operations touch the same ID, later phases win (for example,
`delete_by_filter` can be "resurrected" by a later upsert in the same request).

Within a phase:

- `*_rows` arrays are last-write-wins by position (later entries override).
- `*_columns` rejects duplicate IDs with HTTP 400.

## ID formats and constraints

Document IDs accept:

- Unsigned integers (`uint64`) in JSON numeric form (must be whole numbers).
- UUID strings in standard dashed form (36 chars).
- Arbitrary strings up to 64 bytes.

Notes:

- Empty strings are rejected.
- Negative numbers or non-integer numbers are rejected.
- Numeric strings (for example, "123") remain string IDs and are distinct from
  numeric IDs.

## Filter syntax

Filters are JSON arrays using turbopuffer-style operators:

- Boolean: `["And", [f1, f2, ...]]`, `["Or", [f1, f2, ...]]`, `["Not", f]`
- Comparisons: `["attr", "Eq", value]`, `["attr", "NotEq", value]`,
  `["attr", "Lt", value]`, `["attr", "Lte", value]`,
  `["attr", "Gt", value]`, `["attr", "Gte", value]`,
  `["attr", "In", [v1, v2]]`, `["attr", "NotIn", [v1, v2]]`
- Arrays: `["attr", "Contains", value]`, `["attr", "NotContains", value]`,
  `["attr", "ContainsAny", [v1, v2]]`, `["attr", "NotContainsAny", [v1, v2]]`,
  `["attr", "AnyLt", value]`, `["attr", "AnyLte", value]`,
  `["attr", "AnyGt", value]`, `["attr", "AnyGte", value]`
- Glob: `["attr", "Glob", "pattern"]`, `["attr", "NotGlob", "pattern"]`,
  `["attr", "IGlob", "pattern"]`, `["attr", "NotIGlob", "pattern"]`
- Regex: `["attr", "Regex", "pattern"]`, `["attr", "NotRegex", "pattern"]`
- Token ops: `["attr", "ContainsTokenSequence", "value"]`,
  `["attr", "ContainsAllTokens", ["token1", "token2"]]`

Conditional filters (`*_condition`) may use `$ref_new.<attr>` to reference the
incoming write values. For delete conditions, `$ref_new` values resolve to null.

## Filter-based constraints

- `delete_by_filter` hard cap: 5,000,000 rows.
- `patch_by_filter` hard cap: 500,000 rows.

If more rows match than the cap:

- With `*_allow_partial=false` (default): the request fails with HTTP 400.
- With `*_allow_partial=true`: the request succeeds, applies up to the cap, and
  the response includes `rows_remaining: true` so the client can retry.

## Schema updates

The `schema` object maps attribute names to updates. For new attributes, `type`
is required. For existing attributes, `type` cannot change.

Example:

```json
{
  "schema": {
    "title": {
      "type": "string",
      "full_text_search": true
    },
    "category": {
      "type": "string",
      "filterable": true
    },
    "summary": {
      "type": "string",
      "full_text_search": {
        "language": "english",
        "stemming": true
      }
    }
  }
}
```

## Response format

On success, returns HTTP 200 with:

```json
{
  "rows_affected": 3,
  "rows_upserted": 2,
  "rows_patched": 1,
  "rows_deleted": 0,
  "rows_remaining": true
}
```

`rows_remaining` is included only when a filter-based operation hit its cap with
`*_allow_partial=true`.

## Examples

Row-oriented upsert:

```json
{
  "request_id": "3b5a8a7d-3c1a-4f65-9f1f-2a6c4b871a1a",
  "upsert_rows": [
    {"id": 1, "title": "Alpha", "score": 3.2, "vector": [0.1, 0.2, 0.3]},
    {"id": "user-42", "title": "Beta", "active": true}
  ]
}
```

Column-oriented upsert:

```json
{
  "upsert_columns": {
    "ids": [1, 2, 3],
    "title": ["A", "B", "C"],
    "score": [1.1, 2.2, 3.3]
  }
}
```

Delete by filter:

```json
{
  "delete_by_filter": ["status", "Eq", "inactive"],
  "delete_by_filter_allow_partial": true
}
```

Patch by filter:

```json
{
  "patch_by_filter": {
    "filter": ["tier", "Eq", "free"],
    "updates": {"tier": "pro", "upgraded_at": "2025-01-01T00:00:00Z"}
  },
  "patch_by_filter_allow_partial": false
}
```

Copy from another namespace:

```json
{
  "copy_from_namespace": "source_namespace"
}
```

Schema updates with writes:

```json
{
  "schema": {
    "title": {"type": "string", "full_text_search": true},
    "price": {"type": "float", "filterable": true}
  },
  "upsert_rows": [
    {"id": "sku-1", "title": "Widget", "price": 9.99}
  ]
}
```

Disable backpressure:

```json
{
  "disable_backpressure": true,
  "upsert_rows": [
    {"id": 99, "title": "Backpressure bypass"}
  ]
}
```
