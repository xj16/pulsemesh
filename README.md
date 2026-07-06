# PulseMesh

**Event-sourced presence & chat backend, built as two cooperating services:
a Clojure command/query brain and an Erlang/OTP persistent-connection fabric,
wired together over RabbitMQ with PostgreSQL and Redis.**

[![CI](https://github.com/xj16/pulsemesh/actions/workflows/ci.yml/badge.svg)](https://github.com/xj16/pulsemesh/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

PulseMesh is the backend you'd put behind a chat app's presence and messaging:
who's online in a room, what state they're in, and the stream of messages —
all modeled as an **immutable event log**, with **CQRS read models** for cheap
queries and a **supervised OTP fabric** that fans events out to live WebSocket
connections and keeps running through crashes.

It's deliberately small enough to read in an afternoon, but it implements the
hard parts for real: optimistic-concurrency event appends, cache-with-log-
fallback reads, at-least-once fan-out over a topic exchange, and a supervision
tree where a dead process recovers on its own.

---

## Why it exists

Most "chat backend" demos are a single process with an in-memory map. PulseMesh
is about the *interesting* engineering that shows up the moment you make it
distributed and durable:

- **Event sourcing** — state is a fold over an append-only log, not a mutable
  row you overwrite. You get a perfect audit trail and trivially rebuildable
  projections for free.
- **CQRS** — writes go through a pure aggregate; reads hit fast Redis
  projections that fall back to replaying the log. Neither side blocks the other.
- **Right tool per job** — Clojure for pure, data-first domain logic; Erlang/OTP
  for the thing it was literally invented for (millions of long-lived
  connections with supervision-based fault tolerance).
- **Honest failure handling** — every dependency can be down and the system
  degrades predictably instead of corrupting data.

## Features

- Commands: **join / leave** a channel, **post messages**, **set presence**
  (`online` / `away` / `busy` / `offline`).
- **Pure event-sourced aggregate** (`decide` / `apply-event`) with full unit-test
  coverage and no IO — see [`channel.clj`](clojure-service/src/pulsemesh/event/channel.clj).
- **PostgreSQL event store** with per-stream versioning and optimistic
  concurrency (`UNIQUE (channel_id, version)` + `SELECT … FOR UPDATE`, with
  bounded retries).
- **Redis read models** (presence hash, capped recent-messages list) that are
  a *cache*, with **automatic Postgres-replay fallback** when cold.
- **RabbitMQ topic exchange** (`channel.<id>.<type>` routing) as the seam
  between the two services.
- **Erlang/OTP fabric**: Cowboy WebSocket endpoint, an ETS-backed channel
  registry with monitor-based cleanup, and one **supervised presence
  gen_server per channel** that recovers automatically on crash.
- **docker-compose** that brings up Postgres, Redis, RabbitMQ, and both
  services with health-gated startup.
- **GitHub Actions CI** that unit-tests the domain, builds the uberjar,
  compiles the OTP release with warnings-as-errors, runs EUnit, and does a
  full docker-compose end-to-end smoke test.

## Architecture at a glance

```
 HTTP  ──►  command-query (Clojure)  ──events──►  RabbitMQ  ──►  fabric (Erlang/OTP)  ──►  WebSocket clients
                │   ▲                                                   │
          append│   │replay                                     presence trackers
                ▼   │                                          (supervised gen_servers)
             PostgreSQL (event log, source of truth)
                │
         project│
                ▼
              Redis (CQRS read models; log is the fallback)
```

Full write-up, failure model, and a bigger diagram: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

## Repository layout

```
pulsemesh/
├── clojure-service/           # command/query service (event sourcing + CQRS)
│   ├── deps.edn  build.clj     # tools.deps + tools.build (uberjar)
│   ├── src/pulsemesh/
│   │   ├── event/              # schema.clj, channel.clj  (pure domain core)
│   │   ├── command/handler.clj # write pipeline (load→decide→append→publish)
│   │   ├── query/handler.clj   # read side (Redis, log fallback)
│   │   ├── infra/              # db, redis, rabbit, config
│   │   ├── api.clj  main.clj    # HTTP surface + composition root
│   ├── resources/              # config.edn, migrations, logback
│   └── test/                   # pure-aggregate unit tests
├── erlang-fabric/             # persistent-connection fabric (OTP)
│   ├── rebar.config
│   ├── src/                    # app, supervisors, presence, registry,
│   │                           #   amqp consumer, ws + health handlers, via-reg
│   ├── config/                 # sys.config(.src), vm.args
│   └── test/                   # EUnit tests for presence projection
├── docker-compose.yml         # full local stack
├── docs/                      # ARCHITECTURE.md, API.md
├── scripts/demo.sh            # end-to-end scripted demo
└── .github/workflows/ci.yml   # CI: Clojure + Erlang + compose smoke test
```

## Quick start (Docker — the easy path)

Requires only Docker + Docker Compose.

```bash
git clone https://github.com/xj16/pulsemesh.git
cd pulsemesh
docker compose up --build
```

This starts Postgres, Redis, RabbitMQ, the Clojure `command-query` service
(`:8080`), and the Erlang `fabric` (`:8090`). RabbitMQ's management UI is at
<http://localhost:15672> (guest / guest).

Then run the scripted demo (joins, a message, a presence change, and the read
models):

```bash
./scripts/demo.sh lobby
```

…or drive it by hand:

```bash
# submit a command
curl -X POST http://localhost:8080/api/commands \
  -H 'content-type: application/json' \
  -d '{"type":"join-channel","channel-id":"lobby","user-id":"alice"}'

# read the presence projection
curl http://localhost:8080/api/channels/lobby/presence

# watch events fan out live over the fabric (needs `websocat`)
websocat "ws://localhost:8090/ws?channel=lobby&user=observer"
```

Full endpoint and WebSocket reference: **[docs/API.md](docs/API.md)**.

## Running each service directly (for development)

### Clojure command/query service

Requires a JDK (17+) and the [Clojure CLI](https://clojure.org/guides/install_clojure).
Point it at running Postgres/Redis/RabbitMQ (or just `docker compose up
postgres redis rabbitmq`).

```bash
cd clojure-service
clojure -X:test            # run the pure-domain unit tests (no infra needed)
clojure -T:build uber      # build target/pulsemesh.jar
clojure -M:run             # or: java -jar target/pulsemesh.jar
```

Configuration is environment-driven (see `resources/config.edn`); every value
has a local-friendly default. Key vars: `PULSEMESH_PG_URL`,
`PULSEMESH_REDIS_URI`, `PULSEMESH_AMQP_URI`, `PULSEMESH_HTTP_PORT`.

### Erlang/OTP fabric

Requires Erlang/OTP (26+) and [rebar3](https://rebar3.org/).

```bash
cd erlang-fabric
rebar3 compile            # warnings-as-errors
rebar3 eunit              # presence-projection tests
rebar3 shell              # run interactively
rebar3 as prod release    # build a self-contained release
```

Config vars: `PULSEMESH_FABRIC_PORT`, `PULSEMESH_AMQP_URI`,
`PULSEMESH_AMQP_EXCHANGE`.

## How the write path works (the short version)

1. A command arrives at `POST /api/commands` and is validated.
2. The channel's event stream is loaded from Postgres and **replayed** into
   in-memory state (pure fold).
3. `decide` turns the command + state into new events — or rejects it.
4. Events are **appended** with optimistic concurrency; a lost race retries
   against fresh state.
5. Committed events are **projected** into Redis and **published** to RabbitMQ.
6. The Erlang fabric consumes them, updates its supervised presence trackers,
   and **fans them out** to every subscribed WebSocket.

Because steps 2–3 are pure, the entire domain is tested without a database.

## Tech stack

| Concern | Choice |
|---|---|
| Write-side service / domain | **Clojure** (tools.deps, Ring + reitit, next.jdbc) |
| Persistent-connection fabric | **Erlang/OTP** (rebar3, Cowboy, amqp_client) |
| Event store / source of truth | **PostgreSQL** |
| Read-model cache | **Redis** (Carmine) |
| Event bus between services | **RabbitMQ** (topic exchange) |
| Local orchestration | **Docker** + Docker Compose |
| CI | **GitHub Actions** |

## Testing & CI

- `clojure -X:test` runs the pure aggregate tests (fast, no infra).
- `rebar3 eunit` runs the presence-projection tests through the real
  supervisor + registry.
- CI additionally builds both artifacts and runs a **docker-compose smoke test**
  that boots the whole stack and drives a real command→event→read-model flow.

## Scope & status

This is a focused, working reference implementation — not a hosted product.
It implements the core event-sourced presence/chat backend end to end
(commands, event store, read models, event bus, WebSocket fan-out, supervision,
CI). Things intentionally left out to keep it legible: auth/authz, horizontal
sharding of streams, message search, and cross-node distribution of the fabric
(the OTP design is ready for it, but a multi-node cluster is out of scope here).

## License

MIT © 2026 xj16 — see [LICENSE](LICENSE).
