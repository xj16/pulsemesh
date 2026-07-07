# Security posture

PulseMesh ships **open by default** so the demo is frictionless, but every
control is one environment variable away from a production posture. All of it
lives in one auditable file: [`infra/security.clj`](../clojure-service/src/pulsemesh/infra/security.clj).

## What's enforced

| Control | Default | Env override |
|---|---|---|
| **Rate limiting** (per-client token bucket) | on, 50 rps / 100 burst | `PULSEMESH_RATE_LIMIT_ENABLED`, `PULSEMESH_RATE_LIMIT_RPS`, `PULSEMESH_RATE_LIMIT_BURST` |
| **Request-size guard** | 64 KiB | `PULSEMESH_MAX_BODY_BYTES` |
| **Command validation** | always on (schema) | — |
| **Bearer-token auth** | **off** | `PULSEMESH_AUTH_SECRET` |
| **CORS** | localhost dashboard origins | `PULSEMESH_CORS_ORIGIN` (`*` or a single origin) |

Rate limiting, the body guard, and CORS run as outer Ring middleware before
the request body is even parsed. Command validation is the existing pure
schema check (`event/schema.clj`).

## Identity: honest handling of `user-id`

Without auth, `POST /api/commands` trusts the `user-id` in the request body —
fine for a reference impl, but stated plainly rather than hidden.

Turn auth on and that stops being true:

```bash
# require a bearer token on writes
PULSEMESH_AUTH_SECRET=super-secret docker compose up
curl -X POST http://localhost:8080/api/commands \
  -H 'authorization: Bearer super-secret' \
  -H 'content-type: application/json' \
  -d '{"type":"join-channel","channel-id":"lobby","user-id":"alice"}'
```

The shared-secret mode gates the transport. The stricter **token→principal**
mode (a map of `{token → user-id}`, configured in code) goes further: it
**overwrites** the body's `user-id` with the token's principal, so a caller can
only ever act as themselves — closing the impersonation gap entirely. The same
token can be threaded onto the WebSocket connect to reject unauthenticated
sockets at the fabric.

## What's intentionally out of scope

Per-user authorization rules, TLS termination (do it at the proxy/ingress),
secret rotation, and multi-tenant isolation. This is a focused reference
implementation; the hooks are here, the full IAM story is not.
