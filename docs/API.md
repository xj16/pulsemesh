# PulseMesh API Reference

Two surfaces: the **command/query HTTP API** on the Clojure service
(`:8080`), and the **WebSocket fabric** on the Erlang node (`:8090`).

## HTTP — command/query service

### `GET /health`

```json
{ "status": "ok", "service": "pulsemesh-command-query" }
```

### `POST /api/commands`

Submit a command. The body is a JSON object with a `type` and the fields that
type requires. On success you get back the events the command produced.

| Command `type`   | Required fields                          | Emits (on success)                  |
|------------------|------------------------------------------|-------------------------------------|
| `join-channel`   | `channel-id`, `user-id`                  | `channel-created`?, `member-joined` |
| `leave-channel`  | `channel-id`, `user-id`                  | `member-left`                       |
| `post-message`   | `channel-id`, `user-id`, `body`          | `message-posted`                    |
| `set-presence`   | `channel-id`, `user-id`, `presence`      | `presence-changed`                  |

`presence` must be one of `online` / `away` / `busy` / `offline`.

**Request**

```bash
curl -X POST http://localhost:8080/api/commands \
  -H 'content-type: application/json' \
  -d '{"type":"join-channel","channel-id":"lobby","user-id":"alice"}'
```

**Responses**

| Status | Meaning |
|--------|---------|
| `201`  | Accepted. Body: `{"status":"accepted","events":[...]}` |
| `400`  | Invalid command. Body lists `problems`. |
| `409`  | Domain rejected it (e.g. posting to a channel you haven't joined). |
| `503`  | Optimistic-concurrency conflict after retries; safe to retry. |

Idempotency: re-joining a channel you're already in, or setting the presence
you already have, succeeds with an empty `events` list.

### `GET /api/channels/{channel-id}/presence`

```json
{ "channel-id": "lobby",
  "members": { "alice": "online", "bob": "away" },
  "source": "cache" }
```

`source` is `cache` (served from Redis) or `log` (rebuilt from Postgres).

### `GET /api/channels/{channel-id}/messages?limit=N`

Most-recent-first messages (default 50, max 200).

```json
{ "channel-id": "lobby",
  "messages": [ { "user-id": "alice", "body": "hello mesh!", "at": "2026-07-06T..." } ],
  "source": "cache" }
```

### `GET /api/channels/{channel-id}/history`

The full ordered event stream for a channel (audit / debugging), always from
the durable log.

### `GET /api/channels/{channel-id}/events?since=N`

Replay endpoint: the ordered events for a channel with `version` strictly
greater than `since` (omit or `0` for the whole stream). The Erlang fabric
calls this to rehydrate a presence tracker from the durable log after a crash
or reconnect, closing any fan-out gap.

```json
{ "channel-id": "lobby", "since": 3, "count": 2,
  "events": [
    { "type": "presence-changed", "channel-id": "lobby", "user-id": "bob",
      "presence": "away", "version": 4, "occurred-at": "2026-07-06T..." },
    { "type": "message-posted", "channel-id": "lobby", "user-id": "alice",
      "body": "hey", "version": 5, "occurred-at": "2026-07-06T..." }
  ] }
```

## WebSocket — fabric

Connect:

```
ws://localhost:8090/ws?channel=<channel-id>&user=<user-id>
```

On connect the server sends a welcome frame with the current presence snapshot:

```json
{ "op": "welcome", "channel-id": "lobby", "presence": { "alice": "online" } }
```

Thereafter every committed event for that channel is pushed as a JSON frame,
e.g.:

```json
{ "type": "message-posted", "channel-id": "lobby", "user-id": "alice",
  "body": "hello mesh!", "version": 4, "occurred-at": "2026-07-06T..." }
```

Client → server control frames:

```json
{ "op": "ping" }      // server replies {"op":"pong"}
```

Idle connections are closed after 60s; send a ping to keep alive.

### Try it with websocat

```bash
websocat "ws://localhost:8090/ws?channel=lobby&user=observer"
# in another terminal, POST commands and watch them arrive live
```
