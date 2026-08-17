# harness-redis-connector

A DeepSeek Harness **plugin** that bridges Redis Streams to Harness
Agents/Sessions in both directions. It speaks the wire contract in
[`PROTOCOL.md`](./PROTOCOL.md) with the `flink-agents` project (Java/Flink,
Telegram integration).

- **Inbound** (`harness:inbound`): consumes messages via a consumer group and
  admits them to Harness as `agent.followup(...)` / `agent.steer(...)` /
  `new_session` (fresh agent + rebind). ACKs after safe admission, not after the
  agent finishes responding.
- **Outbound** (`harness:outbound`): projects durable `session/event`
  `assistant/message` text into `assistant_message` envelopes, and `turn/end`
  error reasons + live `agent/error` into `error` envelopes.

It does **not** call an LLM directly, does **not** implement an agent loop, and
imports no concrete loop classes. It drives only the public Harness abstractions
(`ctx.agents`, the `session/event` firehose, and optional
`ctx.sessionPersistence`).

## Architecture

Modules under `src/` are layered, one responsibility each:

- `protocol/` — envelope types + parse/validate/serialize (version check,
  unknown-type handling). No heavy imports.
- `redis-transport/` — `RedisTransport` interface + an `ioredis` implementation
  (XADD, XGROUP CREATE, XREADGROUP, XACK, XPENDING, XAUTOCLAIM), behind an
  interface so tests fake it.
- `binding-store/` — `ConversationBindingStore` interface + in-memory and Redis
  hash implementations (two maps: `conversation_id → session_id` and
  `session_id → conversation_id`), enforcing one active conversation ↔ one
  active session.
- `receipt-store/` — `InboundReceiptStore` interface + in-memory and Redis
  implementations (idempotency on `message_id`, with TTL).
- `session-resolver/` — `conversation_id → session_id → live/persisted/fresh`,
  with binding-consistency enforcement and `new_session` rebinding. A persisted
  session whose header lacks a `cwd` is never resumed: it is replaced by a fresh
  session (with `meta.cwd`) on the next inbound message.
- `inbound-adapter/` — the consumer: parse → idempotency → resolve →
  followup/steer/new_session → XACK.
- `outbound-projector/` — subscribes to the firehose, projects bound sessions.
- `state/` — in-memory per-conversation correlation + metadata passthrough.
- `plugin-entry.ts` — the `apply` function and lifecycle wiring.

The Harness-facing modules depend on **ports** (minimal structural interfaces
for `AgentRegistry`, `Agent`, `AgentHandle`, `SessionPersistence`) rather than
the concrete services, so unit tests run without any Harness runtime import and
only `plugin-entry.ts` imports the real packages (`createUserMessage`,
`SessionId`, the `ctx` types).

## Dependency strategy

This project joins the Harness **pnpm workspace** (the task's strategy 2). The
Harness packages are unpublished and use `workspace:^` internal deps, so a
standalone install would have to re-resolve the entire Harness tree including
native-dependency build scripts. Instead:

- `../deepseek-harness/pnpm-workspace.yaml` has one added line:
  `- ../harness-redis-connector` (a documented change; see the Harness
  `pnpm-workspace.yaml`).
- This connector's `package.json` depends on the Harness packages via
  `workspace:^`, exactly like the Harness packages do.
- All connector source files live here in `harness-redis-connector/`; nothing is
  vendored or copied from the Harness.

Because the workspace root is the Harness checkout, install runs from there:

```sh
cd ../deepseek-harness
pnpm install                       # links this connector + installs ioredis
pnpm --filter harness-redis-connector test
pnpm --filter harness-redis-connector typecheck
```

`pnpm test` / `pnpm typecheck` also run directly from this directory (pnpm links
each workspace package's direct deps into its own `node_modules`).

Only public Harness packages are imported: `@deepseek-ai/dsh-agent`,
`@deepseek-ai/dsh-session`, `@deepseek-ai/dsh-llm`, `@deepseek-ai/cordis`,
`@deepseek-ai/dsh-session-persistence` (type-only), and `@deepseek-ai/schemastery`.

## Configuration

The plugin reads its `Config` from the Harness `cordis.yml` (standard function
plugin config). All keys are optional and have defaults:

| Key | Default | Meaning |
| --- | --- | --- |
| `redisUrl` | `REDIS_URL` env, then ioredis localhost | Redis connection URL |
| `inboundStream` | `harness:inbound` | Inbound stream name |
| `outboundStream` | `harness:outbound` | Outbound stream name |
| `consumerGroup` | `harness-redis-connector` | Consumer group on the inbound stream |
| `consumerName` | `harness-redis-connector` | Consumer name (unique per process to scale) |
| `payloadField` | `json` | Stream entry field holding the JSON envelope |
| `batchSize` | `10` | Max entries per XREADGROUP batch |
| `blockMs` | `5000` | XREADGROUP block time |
| `receiptTtlMs` | `86400000` | Idempotency receipt TTL |
| `bindingKeyPrefix` | `harness-redis-connector:binding` | Key prefix for the binding hashes |
| `receiptKeyPrefix` | `harness-redis-connector:receipt` | Key prefix for receipt keys |
| `provider` / `model` | — | Per-agent options for created/resumed agents |

Environment variable supported as a convenience fallback: `REDIS_URL` (used only
when `config.redisUrl` is absent).

## Wire conventions (summarized from PROTOCOL.md)

- Inbound `type`: `message` | `new_session` | `steer`; outbound `type`:
  `assistant_message` | `error`.
- Envelope fields are `snake_case`; `content` and `metadata` are objects.
- `message_id` is the external idempotency key; `correlation_id` on outbound is
  the originating inbound `message_id`.
- Each stream entry carries the JSON envelope as its single `json` field (the
  reader prefers `json` and falls back to the first field value, matching the
  Flink consumer).
- `metadata` round-trips unchanged (including `chatId`) so Flink can route.

## Troubleshooting

For the operational failure modes of the Telegram bridge (`{{cwd}}` /
`{{model}}` prompt assembly errors, session rebinding, delivery triage,
diagnostic commands, and recovery procedures), see
[`TROUBLESHOOTING.md`](./TROUBLESHOOTING.md).

## Known Limitations and Deferred Work

- `correlation_id` is the most recent inbound `message_id` for the conversation
  (correlation metadata), not a strict per-turn request→one-response id. See
  PROTOCOL.md's note that it is "correlation metadata, NOT a strict
  request→one-response id".
- The binding and receipt stores are process-local in-memory state for
  correlation/metadata passthrough only; durable identity lives in Redis. A
  restart loses the in-memory correlation/metadata cache but recovers bindings
  and receipts from Redis.
- The Redis binding store uses a Lua script for atomic rebind; the in-memory
  store is the unit-tested canonical implementation of the same invariant.
- Pending-entry recovery (`XPENDING`/`XAUTOCLAIM`) is best-effort on startup; a
  dedicated dead-letter queue for poison messages is deferred (they are logged
  and acknowledged).
- `new_session` delivers its `content.text` as the first `followup` to the new
  agent (so the text is not lost); PROTOCOL.md only specifies the rebind.
- The Redis implementations (`ioredis` transport, Redis binding/receipt stores)
  are not exercised against a live Redis in the unit suite; tests use in-memory
  fakes.
- A mismatched explicit `session_id` on a `message`/`steer` is rejected (logged +
  outbound `error`) rather than silently rebinding; a `new_session` always
  rebinds.
