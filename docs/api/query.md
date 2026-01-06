# Query API

## Endpoint

`POST /v2/namespaces/{namespace}/query`

Requires the standard bearer token (see `docs/api/overview.md`).

## Request body

A single JSON object with query parameters.

Top-level fields:

- `rank_by` (required unless `aggregate_by` is set): ranking expression.
- `filters` (optional): filter expression array. `filter` is accepted as an alias.
- `include_attributes` (array of strings, optional): whitelist of attribute names to return.
- `exclude_attributes` (array of strings, optional): blacklist of attribute names to drop.
  `include_attributes` and `exclude_attributes` are mutually exclusive.
- `limit` (int, optional): max rows to return. Default 10, max 10,000. Alias: `top_k`.
- `top_k` (int, optional): alias for `limit`.
- `per` (array of strings, optional): diversification keys for attribute order queries.
  When set, returns up to `limit` rows per distinct value of the `per` keys.
- `aggregate_by` (object, optional): aggregation specification.
- `group_by` (array of strings, optional): group-by keys for aggregations.
  Requires `aggregate_by`.
- `vector_encoding` (string, optional): `"float"` (default) or `"base64"`.
- `consistency` (string, optional): `"strong"` (default) or `"eventual"`.

Validation notes:

- `limit`/`top_k` must be between 1 and 10,000 (0 resets to default 10).
- `rank_by` is required unless `aggregate_by` is present.
- `group_by` is only valid with `aggregate_by`.
- `vector_encoding` must be `"float"` or `"base64"`.
- `consistency` must be `"strong"` or `"eventual"`.
- `include_attributes` and `exclude_attributes` cannot both be set.

Consistency behavior:

- `strong` (default): refreshes to WAL head; refresh failures return 503.
- `eventual`: uses a cached snapshot up to 60s stale and caps tail scanning to
  128 MiB; refresh failures do not fail the query.
- If the namespace disabled write backpressure and unindexed bytes exceed 2 GiB,
  strong queries return 503 with `strong query unavailable` (eventual queries
  still run).

## rank_by formats

### Vector ANN

```json
["vector", "ANN", [0.12, 0.54, 0.33]]
```

- Requires vector dims matching the namespace schema.
- When `vector_encoding` is `"base64"`, the third element must be a base64
  float32 string.

### BM25 full-text

```json
["body", "BM25", "search terms"]
```

Or with typeahead:

```json
["body", "BM25", {"query": "search ter", "last_as_prefix": true}]
```

### Attribute order

```json
["price", "asc"]
```

`id` is supported as a sort key. Attribute-order queries do not include `$dist`
in the response.

### Composite ranking

Composite rank_by expressions combine BM25 clauses and filter boosts:

```json
["Sum", [
  ["title", "BM25", "jaguar"],
  ["Product", 0.2, ["body", "BM25", "car"]],
  ["brand", "Eq", "acme"]
]]
```

Supported composite operators:

- `["Sum", [clause, ...]]`
- `["Max", [clause, ...]]`
- `["Product", weight, clause]`

Clauses can be BM25 expressions or filter expressions. Filter clauses yield a
score of 1 when they match and 0 otherwise. Documents with a total score of 0
are omitted.

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

Regex filters require the attribute schema to have `regex: true`.

## Aggregations

`aggregate_by` is an object mapping labels to aggregation expressions:

- `["Count"]`
- `["Sum", "numeric_attribute"]`

Example:

```json
{
  "aggregate_by": {
    "total": ["Count"],
    "revenue": ["Sum", "price"]
  }
}
```

`group_by` can be used to group aggregations:

```json
{
  "aggregate_by": {
    "orders": ["Count"]
  },
  "group_by": ["status", "region"],
  "limit": 100
}
```

When `group_by` is set, `limit` caps the number of aggregation groups returned.

## Multi-query

Provide a `queries` array to execute multiple subqueries against the same
snapshot. The top-level `consistency` applies to all subqueries. Maximum 16
subqueries.

```json
{
  "consistency": "eventual",
  "queries": [
    {"rank_by": ["vector", "ANN", [0.1, 0.2, 0.3]], "limit": 5},
    {"rank_by": ["title", "BM25", "red shoes"], "filters": ["status", "Eq", "active"]}
  ]
}
```

## Responses

Normal rank_by query response:

```json
{
  "rows": [
    {"id": 1, "$dist": 0.12, "title": "Alpha", "price": 9.99},
    {"id": "user-2", "$dist": 0.28, "title": "Beta", "price": 19.99}
  ],
  "billing": {
    "billable_logical_bytes_queried": 0,
    "billable_logical_bytes_returned": 0
  },
  "performance": {
    "cache_temperature": "warm",
    "server_total_ms": 12.3
  }
}
```

Attribute-order queries omit `$dist`.

Aggregation response:

```json
{
  "aggregations": {
    "total": 42,
    "revenue": 1234.5
  },
  "billing": {
    "billable_logical_bytes_queried": 0,
    "billable_logical_bytes_returned": 0
  },
  "performance": {
    "cache_temperature": "cold",
    "server_total_ms": 5.4
  }
}
```

Grouped aggregation response:

```json
{
  "aggregation_groups": [
    {"status": "paid", "region": "us", "orders": 10},
    {"status": "paid", "region": "eu", "orders": 7}
  ],
  "billing": {
    "billable_logical_bytes_queried": 0,
    "billable_logical_bytes_returned": 0
  },
  "performance": {
    "cache_temperature": "cold",
    "server_total_ms": 6.1
  }
}
```

Multi-query response:

```json
{
  "results": [
    {"rows": [{"id": 1, "$dist": 0.04}]},
    {"rows": [{"id": "doc-9", "$dist": 1.1, "title": "Red shoes"}]}
  ],
  "billing": {
    "billable_logical_bytes_queried": 0,
    "billable_logical_bytes_returned": 0
  },
  "performance": {
    "cache_temperature": "cold",
    "server_total_ms": 9.7
  }
}
```

If an index rebuild is in progress and the query depends on it, the server
returns HTTP 202 with:

```json
{
  "status": "accepted",
  "message": "query depends on index still being rebuilt",
  "rows": []
}
```
