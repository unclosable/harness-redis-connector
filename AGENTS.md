# AGENTS.md — harness-redis-connector

Conventions and architecture decisions for this standalone TypeScript project.
It is a DeepSeek Harness **plugin** that bridges Redis Streams to Harness
Agents/Sessions. `PROTOCOL.md` is the authoritative wire contract; implement it
exactly.

## Conventions

- **One responsibility per module.** Do not grow a single connector class. The
  layering is `protocol/` → `redis-transport/` → `binding-store/` +
  `receipt-store/` → `session-resolver/` → `inbound-adapter/` +
  `outbound-projector/` → `plugin-entry.ts`.
- **Harness deps stay behind ports.** Modules except `plugin-entry.ts` use
  `import type` for Harness types (`Agent`, `Session`, `SessionEvent`,
  `ContentBlock`) and structural ports (`AgentRegistryLike`, `AgentHandleLike`,
  `SessionPersistenceLike`, `ConversationBindingStore`, `InboundReceiptStore`)
  so unit tests need no Harness runtime. Only `plugin-entry.ts` imports the real
  `createUserMessage`, `SessionId`, and the `ctx` type augmentations.
- **No LLM calls, no agent loop, no concrete-loop imports.** The connector only
  drives `ctx.agents` and observes the durable `session/event` firehose.
- **Plugin form:** function plugin with named exports `name` / `inject` /
  `Config` / `apply`, and no default export. `sessionPersistence` is OPTIONAL:
  read it via `ctx.get('sessionPersistence')`, never list it in `inject`.
- **ESM + NodeNext.** Relative imports use `.js` extensions; the package is
  `"type": "module"`. `verbatimModuleSyntax` is on, so type-only imports are
  always `import type`.
- **Validate at the wire boundary** (`protocol/validate.ts`): version checked
  before `type`; a missing/unsupported version is malformed (`error`), an
  unknown `type` is `ignored` (forward compatibility). Same-process typed
  values are trusted.
- **Wire field name is `json`.** Stream entries carry the envelope JSON under the
  `json` field (matching the Flink/Lettuce producer `XADD stream * json <json>`).
  The outbound sender writes `json`; the inbound reader prefers `json` and falls
  back to the first field value (the Flink consumer's reader convention).
- **Tests describe behavior.** Unit tests use in-memory fakes
  (`tests/helpers/fakes.ts`); the in-memory `ConversationBindingStore` and
  `InboundReceiptStore` are the canonical, unit-tested implementations of the
  one-active and idempotency invariants, which the Redis backends mirror.
- **ACK after admission, never after response.** The adapter acknowledges a
  message only after parse/validate + idempotency + resolve + followup/steer/rebind
  succeed, or after a permanently-invalid message (malformed, unknown type,
  duplicate, binding conflict) is logged and dropped. Transient admission
  failures stay pending and are retried on the reclaim cadence — never ACKed,
  and never after the agent produces a response.

## Architecture decisions

- **Dependency strategy (2): join the Harness pnpm workspace.** The Harness
  packages are unpublished with `workspace:^` internal deps; a standalone
  install would re-resolve the whole tree including native build scripts.
  `../deepseek-harness/pnpm-workspace.yaml` gained one line
  (`- ../harness-redis-connector`) and this package uses `workspace:^` deps like
  the Harness packages. Install runs from the Harness root.
- **`correlation_id` is per-conversation, not per-turn.** The outbound
  `correlation_id` is the most recent inbound `message_id` for the conversation
  (`ConversationState`), matching PROTOCOL.md's "correlation metadata, NOT a
  strict request→one-response id". A per-turn map would add correlation state
  with no consumer that needs it.
- **`new_session` delivers its text as a followup.** PROTOCOL.md specifies only
  the rebind; delivering `content.text` to the fresh agent avoids silently
  dropping the first user message.
- **Mismatched explicit `session_id` is a hard reject.** A normal `message`/
  `steer` never moves a conversation between sessions; `new_session` is the only
  rebind path.
- **Ownership tracking.** The resolver returns `owned: true` + a handle for
  create/resume; `plugin-entry.ts` tracks owned handles and disposes only those
  on teardown (never an agent it merely reused).

## Commands

```sh
# from the Harness checkout (workspace root)
pnpm install
pnpm --filter harness-redis-connector test
pnpm --filter harness-redis-connector typecheck
pnpm --filter harness-redis-connector build
```

The same `test`/`typecheck` scripts also run from this directory.
