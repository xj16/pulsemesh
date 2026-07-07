<div align="center">

# PulseMesh

**Event-sourced presence & chat backend — a Clojure command/query brain and an
Erlang/OTP persistent-connection fabric, wired over RabbitMQ with PostgreSQL
and Redis.**

[![CI](https://github.com/xj16/pulsemesh/actions/workflows/ci.yml/badge.svg)](https://github.com/xj16/pulsemesh/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/xj16/pulsemesh/branch/main/graph/badge.svg)](https://codecov.io/gh/xj16/pulsemesh)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Live dashboard](https://img.shields.io/badge/live-dashboard-4f8cff.svg)](erlang-fabric/priv/static/index.html)

*Immutable event log · CQRS read models · at-least-once fan-out · a supervised
OTP tree that recovers on its own — with a live in-browser dashboard to watch
it all happen.*

</div>

---

PulseMesh is the backend you'd put behind a chat app's presence and messaging:
who's online in a room, what state they're in, and the stream of messages —
all modeled as an **append-only event log**, with **CQRS read models** for cheap
queries and a **supervised OTP fabric** that fans events out to live WebSocket
connections and keeps running through crashes.

It's small enough to read in an afternoon, but it implements the hard parts for
real: optimistic-concurrency event appends, cache-with-log-fallback reads,
**genuine at-least-once fan-out** over a durable topic queue with
replay-on-reconnect, and a supervision tree where a dead process rehydrates
itself from the log.

## See it live

The fabric serves a **self-contained dashboard at `http://localhost:8090/`**:
a presence roster (avatars flipping online/away/busy/offline), a streaming
message pane, and a raw **event tape** so you literally watch event-sourcing +
fan-out in real time. Drive it from the form, or watch the seeded room chatter.

The same page ships as a **standalone static bundle** (`scripts/build-web.sh` →
`web/dist/index.html`) that runs a built-in simulation when there's no backend
— so it's fully interactive even embedded on a static site with nothing behind
it.

```
┌──────────── Presence ────┐ ┌──── #lobby ─────┐ ┌───── Event tape ─────────┐
│ ● alice        online    │ │ alice  hello mesh│ │ #5 message-posted alice  │
│ ◐ bob          away      │ │ bob    hey 👋    │ │ #4 presence-changed bob  │
│ ○ carol        busy      │ │ …                │ │ #3 member-joined carol   │
└──────────────────────────┘ └─────[ send ]─────┘ └──────────────────────────┘
```

## Why it exists

Most "chat backend" demos are a single process with an in-memory map. PulseMesh
is about the *interesting* engineering that shows up the moment you make it
distributed and durable:

- **Event sourcing** — state is a fold over an append-only log, not a mutable
  row you overwrite. Perfect audit trail, trivially rebuildable projections.
- **CQRS** — writes go through a pure aggregate; reads hit fast Redis
  projections that fall back to replaying the log. Neither side blocks the other.
- **Right tool per job** — Clojure for pure, data-first domain logic; Erlang/OTP
  for the thing it was invented for (millions of long-lived connections with
  supervision-based fault tolerance).
- **Honest failure handling** — every dependency can be down and the system
  degrades predictably instead of corrupting data. And now the docs' failure
  model is what the code actually does.

## Features

- **Commands**: join / leave a channel, post messages, set presence
  (`online` / `away` / `busy` / `offline`).
- **Pure event-sourced aggregate** (`decide` / `apply-event`) — no IO, covered
  by unit **and property-based** tests
  ([`channel.clj`](clojure-service/src/pulsemesh/event/channel.clj)).
- **PostgreSQL event store** with per-stream versioning and optimistic
  concurrency (`UNIQUE (channel_id, version)` + advisory lock + bounded retries),
  schema managed by **Migratus** at boot.
- **Redis read models** (presence hash, capped recent-messages list) that are a
  cache, with **automatic Postgres-replay fallback** when cold.
- **RabbitMQ topic exchange** (`channel.<id>.<type>` routing) as the seam
  between the two services.
- **Erlang/OTP fabric**: Cowboy WebSocket endpoint, ETS channel registry with
  monitor-based cleanup, one supervised presence gen_server per channel, and a
  **durable-queue consumer** that gives real at-least-once fan-out with
  **replay-on-reconnect** rehydration.
- **Live dashboard** served at `/`, plus a standalone static bundle.
- **Observability**: Prometheus metrics at `/metrics`; an `observability`
  compose profile.
- **Security** (all env-toggleable): rate limiting, request-size guard, optional
  bearer-token auth with principal-pinned identity, and CORS — see
  [`docs/SECURITY.md`](docs/SECURITY.md).
- **One-command demo**: `docker compose --profile demo up` comes up populated.
- **docker-compose** full stack with health-gated startup.
- **GitHub Actions CI**: unit + property + integration tests, coverage, uberjar,
  OTP release (warnings-as-errors + EUnit), a full compose smoke test, and a
  built static-demo artifact.

## Architecture at a glance

```
 HTTP  ──►  command-query (Clojure)  ──events──►  RabbitMQ  ──►  fabric (Erlang/OTP)  ──►  WebSocket clients + dashboard
                │   ▲                              (durable                │
          append│   │replay ◄──── GET /events ─────  queue,          presence trackers
                ▼   │            (rehydrate)          at-least-once)  (supervised gen_servers,
             PostgreSQL (event log, source of truth)                  rehydrate from the log)
                │
         project│
                ▼
              Redis (CQRS read models; log is the fallback)
```

Full write-up, failure model, and a bigger diagram: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

## Quick start (Docker — the easy path)

Requires only Docker + Docker Compose.

```bash
git clone https://github.com/xj16/pulsemesh.git
cd pulsemesh

# Bring the whole stack up ALREADY POPULATED, then open the dashboard:
docker compose --profile demo up --build
#   → http://localhost:8090/          live dashboard (seeded rooms)
#   → http://localhost:8080/metrics   Prometheus metrics
#   → http://localhost:15672          RabbitMQ management (guest / guest)
```

Plain (unseeded) stack: `docker compose up --build`, then seed by hand with
`./scripts/seed.sh` or drive it via curl:

```bash
# submit a command
curl -X POST http://localhost:8080/api/commands \
  -H 'content-type: application/json' \
  -d '{"type":"join-channel","channel-id":"lobby","user-id":"alice"}'

# read the presence projection
curl http://localhost:8080/api/channels/lobby/presence

# watch events fan out live — or just open http://localhost:8090/
websocat "ws://localhost:8090/ws?channel=lobby&user=observer"
```

Full endpoint and WebSocket reference: **[docs/API.md](docs/API.md)**.

## Running each service directly (for development)

### Clojure command/query service

Requires a JDK (17+) and the [Clojure CLI](https://clojure.org/guides/install_clojure).
Point it at running Postgres/Redis/RabbitMQ (or `docker compose up postgres
redis rabbitmq`).

```bash
cd clojure-service
clojure -X:test          # pure-domain + property tests (no infra needed)
clojure -X:integration   # integration tests (needs Postgres + Redis)
clojure -M:coverage      # cloverage report
clojure -T:build uber    # build target/pulsemesh.jar
clojure -M:run           # or: java -jar target/pulsemesh.jar
```

Config is environment-driven (see `resources/config.edn`); every value has a
local-friendly default. Key vars: `PULSEMESH_PG_URL`, `PULSEMESH_REDIS_URI`,
`PULSEMESH_AMQP_URI`, `PULSEMESH_HTTP_PORT`, plus the security knobs in
[`docs/SECURITY.md`](docs/SECURITY.md).

### Erlang/OTP fabric

Requires Erlang/OTP (26+) and [rebar3](https://rebar3.org/).

```bash
cd erlang-fabric
rebar3 compile           # warnings-as-errors
rebar3 eunit             # presence-projection tests
rebar3 shell             # run interactively (dashboard at http://localhost:8090/)
rebar3 as prod release   # build a self-contained release
```

Config vars: `PULSEMESH_FABRIC_PORT`, `PULSEMESH_AMQP_URI`,
`PULSEMESH_AMQP_EXCHANGE`, `PULSEMESH_AMQP_QUEUE`, `PULSEMESH_CQ_URL`.

### Standalone dashboard bundle

```bash
./scripts/build-web.sh   # → web/dist/index.html (self-contained, demo mode)
```

## How the write path works (the short version)

1. A command arrives at `POST /api/commands` and is validated.
2. The channel's event stream is loaded from Postgres and **replayed** into
   in-memory state (pure fold).
3. `decide` turns the command + state into new events — or rejects it.
4. Events are **appended** with optimistic concurrency; a lost race retries
   against fresh state.
5. Committed events are **projected** into Redis and **published** to RabbitMQ.
6. The fabric consumes them from a **durable queue**, updates its supervised
   presence trackers (rehydrating from the log on restart), and **fans them
   out** to every subscribed WebSocket and the dashboard.

Because steps 2–3 are pure, the whole domain is tested without a database — and
a property test pins down that write-side state, the fabric projection, and a
fresh replay are always identical.

## Tech stack

| Concern | Choice |
|---|---|
| Write-side service / domain | **Clojure** (tools.deps, Ring + reitit, next.jdbc) |
| Persistent-connection fabric | **Erlang/OTP** (rebar3, Cowboy, amqp_client) |
| Event store / source of truth | **PostgreSQL** (Migratus migrations) |
| Read-model cache | **Redis** (Carmine) |
| Event bus between services | **RabbitMQ** (durable topic exchange + queue) |
| Metrics | **Prometheus** text exposition (dependency-free) |
| Local orchestration | **Docker** + Docker Compose |
| CI | **GitHub Actions** + Codecov |

## Testing & CI

- `clojure -X:test` — fast pure-aggregate + **property-based** tests, plus
  security/metrics units (no infra).
- `clojure -X:integration` — the write path against **real Postgres + Redis**:
  append, forced version-conflict retry, Redis↔log equivalence, cache-cold
  fallback.
- `rebar3 eunit` — the presence projection through the real supervisor +
  registry, including versioned idempotency.
- CI additionally builds both artifacts, runs a **docker-compose smoke test**
  (real command→event→read-model→dashboard flow), uploads **coverage**, and
  publishes the **static demo bundle** as an artifact.

## Scope & status

A focused, working reference implementation — not a hosted product. It
implements the core event-sourced presence/chat backend end to end (commands,
event store, read models, durable event bus, WebSocket fan-out with replay,
supervision, observability, a live dashboard, CI). Intentionally left out to
keep it legible: per-user authorization rules, horizontal sharding of streams,
message search, and cross-node distribution of the fabric (the OTP design is
ready for it, but a multi-node cluster is out of scope here).

## License

MIT © 2026 xj16 — see [LICENSE](LICENSE).
