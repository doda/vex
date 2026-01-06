# API overview

## Authentication

All API endpoints require a bearer token.

```
Authorization: Bearer <token>
```

Admin/debug endpoints require the admin token; if no admin token is configured the
server returns 403 for those routes.

## Request/response encoding

Requests and responses use JSON payloads. Clients should send
`Content-Type: application/json` and expect JSON responses.

## Compression

- Request bodies: if `Content-Encoding: gzip` is set, the server inflates the body
  before JSON parsing.
- Responses: if `Accept-Encoding` includes `gzip`, the server returns gzipped
  responses with `Content-Encoding: gzip`.

## Request size enforcement

Vex enforces a 256MB request body limit. The limit applies to both the declared
`Content-Length` and the inflated bytes after gzip decompression. Requests that
exceed the limit return 413.

## Error envelope

All non-2xx responses use a consistent error envelope:

```json
{ "status": "error", "error": "an error message" }
```

## Status codes

| Code | Meaning |
| --- | --- |
| 200 | Success |
| 202 | Accepted (query depends on index still building) |
| 400 | Invalid request / schema / types / duplicate IDs |
| 401/403 | Authentication errors |
| 404 | Namespace not found (where applicable) |
| 413 | Payload too large |
| 429 | Rate limiting / backpressure (unindexed > 2GB) |
| 500 | Internal server error |
| 503 | Object store failures / strong query with disable_backpressure > 2GB |

## Global limits

| Limit | Value | Notes |
| --- | --- | --- |
| Max request body size | 256MB | Enforced after gzip decompression. |
| Max write batch rate | 1 batch/sec per namespace | Batching aligns writes to WAL. |
| Unindexed data cap | 2GB | Writes return 429 unless `disable_backpressure=true`. |
| Eventual tail scan cap | 128 MiB | Eventual consistency queries cap unindexed scan. |
| Query concurrency | 16 per namespace | Guardrail for per-namespace parallelism. |
| Max multi-query | 16 | Applies to `queries` arrays. |
| Max `top_k` / `limit.total` | 10,000 | Hard cap for query result size. |

## Example error response

```json
{
  "status": "error",
  "error": "namespace 'demo' not found"
}
```
