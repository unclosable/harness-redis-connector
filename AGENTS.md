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
- **Resolve optional Cordis services lazily.** Non-injected services
  (`sessionPersistence`, `agentPresets`, `workspaceRegistry`) must be resolved
  with `ctx.get(...)` inside the `create`/`resume` request closures, never once
  at `apply()` time: a strict lookup returns `undefined` until that service's
  fiber is ACTIVE.
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
  and never after the agent produces a response. `system_command` is the
  deliberate exception to generic duplicate ACK behavior: it stores a
  serialized `system_command_result` before emitting it, then ACKs; duplicate
  deliveries replay that stored result and never repeat the control transition.

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
- **System control commands.** `renew_session` uses `resolver.renew()` (the
  same fresh create + bind path as `new_session`) without `followup`/`steer` or
  any LLM turn. It inherits the replaced session's provider, model, maxTokens,
  cwd, and agent preset from the live registry, else folds
  `sessionPersistence.load(...)`; titles the replacement `bot-<botId>`; and
  attaches it to the workspace resolved from its cwd. `resume` likewise restores
  persisted model/preset configuration so
  GUI-created sessions survive a web restart. The result-store record is the
  idempotent external outcome; future commands must reuse its store → emit → ACK
  ordering.

## Interactive questions

The connector registers one untagged listener on the native agent-scoped
`user-questions/request` Cordis waterfall (dsh ≥ 0.1.2), inside the startup
try/catch:

  ctx.on('user-questions/request', async (request, next) => { … })

An untagged listener is admitted for every agent; `next` takes no argument.

Routing rule: for each request the listener reads the originating session's
title and answers via Redis only if `title.startsWith('bot-')` (exact match on
prefix). Otherwise it calls `next()` once so the Web answerer runs (Web behavior
unchanged).

Redis path and mapping:
- `conversation_id` is taken from the existing `ConversationBindingStore`.
- `correlation_id` is generated with `randomUUID()` (opaque, echoed by Flink).
- DSH questions map into an outbound `question_request` envelope with
  `content.questions: [{ id, text, options?: string[], multi_select?: boolean }]`.
- A pending registry entry is kept while waiting for the inbound `question_answer`.
- Inbound `question_answer` is mapped to `AskUserQuestionAnswer` (`value:string` → `selected:[value]`, `string[]` → `selected`).

Behavioral notes:
- `question_answer` is dispatched before session resolution and NEVER calls
  `session.prompt`/`followup`/`steer` — it resolves the pending `ask()` only.

Dependency: none on DSH-core — the integration depends only on the native
`user-questions/request` waterfall shipped by `@deepseek-ai/dsh-user-questions`
(dsh ≥ 0.1.2). Connector `inject` is
`['agents','userQuestions','sessionTitle','approval']` (`userQuestions` remains a
real service and is kept for dependency wiring; the listener itself is a plain
event registration).

## Commands

```sh
# from the Harness checkout (workspace root)
pnpm install
pnpm --filter harness-redis-connector test
pnpm --filter harness-redis-connector typecheck
pnpm --filter harness-redis-connector build
```

The same `test`/`typecheck` scripts also run from this directory.

## DSH-core dependency

The connector depends on native dsh features only — the `user-questions/request`
and `approval/request` waterfalls and `ctx.agents.create` — all of which still
exist upstream (dsh ≥ 0.1.2). There is no local DSH-core patch and nothing to
re-apply after an upgrade. See `docs/dsh-integration.md` for the exact API and
the runtime failure signatures.
