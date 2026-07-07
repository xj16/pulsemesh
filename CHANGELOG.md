# Changelog

All notable changes to PulseMesh are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims
to adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-07-07

A polish/hardening pass that closes the gaps between what the docs promised and
what the code did, adds a visible artifact, and broadens test coverage beyond
the pure core.

### Added
- **Live in-browser dashboard.** A single self-contained, dependency-free page
  (`erlang-fabric/priv/static/index.html`) served by the fabric at `GET /`:
  a live presence roster, a streaming message pane, a raw JSON **event tape**,
  and a form that drives commands. When no live fabric is reachable it runs an
  in-browser simulation, so the standalone bundle (`scripts/build-web.sh` →
  `web/dist/`) is fully interactive with **zero backend** — ideal for embedding.
- **Replay endpoint** `GET /api/channels/:id/events?since=N` on the
  command/query service — the durable-log catch-up the fabric uses to
  rehydrate presence.
- **Prometheus metrics** at `GET /metrics` (commands accepted/rejected/invalid,
  optimistic-concurrency retries, end-to-end command latency), plus an
  `observability` compose profile with Prometheus.
- **Security middleware** (dependency-free, all toggleable): per-client
  token-bucket **rate limiting**, a **request-size guard**, an optional
  **bearer-token auth** hook that can pin the acting `user-id` to the
  authenticated principal, and configurable **CORS**. See `docs/SECURITY.md`.
- **Demo/seed mode**: `scripts/seed.sh` and a `demo` compose profile
  (`docker compose --profile demo up`) bring the stack up already populated.
- **Flagship tests**: a `test.check` property test asserting the core invariant
  (write-side aggregate state == fabric presence projection == fresh log
  replay), a gated **integration suite** (real Postgres/Redis) covering the
  append path, the optimistic-concurrency conflict/retry path, Redis↔log
  equivalence, and cache-cold fallback, plus unit tests for the security and
  metrics layers. New EUnit cases for versioned idempotent projection.
- **Coverage** via cloverage (`:coverage` alias) uploaded to Codecov in CI,
  with a coverage badge in the README.

### Changed
- **Fan-out is now genuinely at-least-once.** The fabric's AMQP consumer binds
  a **named, durable, non-exclusive** queue (`pulsemesh.fabric`) with **manual
  acks** and bounded prefetch instead of an `exclusive`/`auto_delete` queue.
  Events published while the fabric is down are retained and delivered on
  reconnect; an event is only acked after it is applied and fanned out.
- **Presence trackers rehydrate from the durable log** on (re)start via the new
  replay endpoint, and apply events **idempotently by version**, so a restarted
  tracker returns correct state immediately instead of waiting for live events.
- **Schema is managed by Migratus at boot** (`main/start-system`). The ad-hoc
  `db/ensure-schema!` DDL is now an explicit, byte-for-byte-equivalent fallback
  rather than a second, competing source of truth.
- Docs (`ARCHITECTURE.md`, `API.md`) updated so the stated failure model
  matches the implemented behavior.

### Fixed
- Rate-limiter token-bucket accounting bug caught by its own new unit test
  (returned `nil` instead of the per-key allow flag, blocking every request).
- `.dockerignore` no longer excludes `rebar.lock`, so a committed lock (when
  present) reaches the image build.

## [0.1.0] — 2026-07-06

### Added
- Initial event-sourced presence/chat backend: pure Clojure aggregate
  (`decide`/`apply-event`), PostgreSQL event store with optimistic concurrency,
  Redis CQRS read models with log fallback, RabbitMQ topic-exchange event bus,
  and a supervised Erlang/OTP fabric (Cowboy WebSocket, ETS channel registry,
  per-channel presence gen_servers).
- `docker-compose` full stack, `scripts/demo.sh`, and GitHub Actions CI
  (domain unit tests, uberjar, OTP release, compose smoke test).

[0.2.0]: https://github.com/xj16/pulsemesh/releases/tag/v0.2.0
[0.1.0]: https://github.com/xj16/pulsemesh/releases/tag/v0.1.0
