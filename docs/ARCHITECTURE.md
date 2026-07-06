# PulseMesh Architecture

PulseMesh is a distributed presence/messaging backend split into two
cooperating services with clearly separated responsibilities, wired together
by an event bus. It is a compact, honest implementation of two patterns that
usually only appear in slideware: **event sourcing with CQRS** (the Clojure
side) and **supervised persistent-connection fabric** (the Erlang side).

```
                         ┌───────────────────────────────────────────┐
   HTTP command/query    │        command-query  (Clojure)           │
  ───────────────────────►                                            │
                         │  1. load stream ◄─────────┐                │
                         │  2. replay (pure aggregate)│  PostgreSQL    │
                         │  3. decide (pure)          │  events table  │
                         │  4. append events ─────────►  (source of    │
                         │  5. project read models ──►│   truth, log)  │
                         │  6. publish ───────┐       └────────────────┘
                         └────────────────────┼────────────┬──────────┘
                                              │            │
                                    ┌─────────▼──┐   ┌─────▼───────┐
                                    │  RabbitMQ  │   │   Redis     │
                                    │ topic exch │   │ read models │
                                    └─────────┬──┘   └─────────────┘
                                              │  channel.<id>.<type>
                         ┌────────────────────▼──────────────────────┐
                         │            fabric  (Erlang/OTP)            │
   WebSocket clients     │                                            │
  ◄──── fan-out ─────────┤  amqp_consumer ─► presence trackers        │
                         │        │           (gen_server per chan)   │
                         │        └────────►  channel_registry ─► WS   │
                         │              (supervised; crashes recover)  │
                         └────────────────────────────────────────────┘
```

## Write side (Clojure) — event sourcing

The unit of consistency is a **channel** (a chat/presence room). All state
transitions are expressed as immutable **events** appended to a per-channel
stream in Postgres.

The domain core is two **pure functions** (`pulsemesh.event.channel`):

- `apply-event : state × event → state` — fold used to rebuild state by replay.
- `decide : state × command → [event] | {:reject reason}` — the command handler.

Neither touches IO, which is why the whole domain is covered by fast unit tests
with no database (`test/pulsemesh/channel_test.clj`).

The orchestration (`pulsemesh.command.handler`) is a load → replay → decide →
append → project → publish pipeline. Appends use **optimistic concurrency**: a
`UNIQUE (channel_id, version)` constraint plus a `SELECT ... FOR UPDATE`
version check means two racing writers can't both extend the same stream; the
loser retries against fresh state.

### CQRS read side

Reads never touch the aggregate. They are served from **Redis** projections
(presence hash + capped recent-messages list) that are updated on every commit.
If Redis is cold or down, the query layer **replays the Postgres log** instead —
so a cache outage costs latency, never correctness.

## Fabric (Erlang/OTP) — persistent connections & fan-out

The fabric owns live client connections and event fan-out. Its supervision tree
(`pulsemesh_fabric_sup`, `rest_for_one`) contains:

| Process | Role |
|---|---|
| `pulsemesh_channel_registry` | ETS-backed pub/sub: channel → subscriber pids, with monitors for automatic cleanup on disconnect. |
| `pulsemesh_presence_sup` | `simple_one_for_one` supervisor; one `pulsemesh_presence` gen_server per active channel. |
| `pulsemesh_presence` | Holds the live presence map for one channel; fed by events, queried for snapshots. |
| `pulsemesh_amqp_consumer` | Subscribes to RabbitMQ (`channel.#`), updates presence trackers, and broadcasts each event to WebSocket subscribers. |

**Supervised processes survive crashes.** A wedged presence tracker is restarted
with fresh state (presence is a projection, so it's rebuildable). A dropped
broker connection crashes the consumer, and the supervisor restarts it — that
*is* the reconnection logic, done the OTP way ("let it crash").

Clients connect over WebSocket at `/ws?channel=<id>&user=<id>` (Cowboy). On
connect they get a presence snapshot; thereafter every committed event for the
channel is pushed as a JSON frame.

## Why two languages?

- **Clojure** shines at the write side: immutable data, pure transformation
  functions, and a natural fit for modeling events as plain maps.
- **Erlang/OTP** is purpose-built for the fabric: millions of cheap processes,
  supervision-based fault tolerance, and battle-tested handling of long-lived
  connections.

RabbitMQ is the seam between them, so each side scales and fails independently.

## Failure model

| Component down | Effect |
|---|---|
| Redis | Reads fall back to Postgres replay. No data loss. |
| RabbitMQ | Writes still commit durably; fan-out pauses. Consumers catch up / replay. |
| A fabric process | Supervisor restarts it; presence rehydrates from subsequent events. |
| Postgres | Writes rejected (it is the source of truth). Reads still serve cached data. |
