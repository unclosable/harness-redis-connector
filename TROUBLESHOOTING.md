# Telegram Bridge Troubleshooting Runbook

Operational runbook for the Telegram ⇄ Harness bridge: the `harness-redis-connector`
plugin, the `flink-agents` Telegram jobs, and the shared Harness session store.
Write-up targets onboarding (understand the moving parts) and triage (given a
symptom, find and fix the cause). The wire contract is
[`PROTOCOL.md`](./PROTOCOL.md); behavior conventions live in
[`AGENTS.md`](./AGENTS.md); this file records the failure modes seen in
production and their recovery procedures.

## 1. System map

```
Telegram user
   │  text message
   ▼
flink-agents: TelegramToHarnessRedisPipeline   (Java/Flink, long-polling source)
   │  XADD harness:inbound * json <envelope>
   ▼
Redis Streams  harness:inbound   (consumer group: harness-redis-connector)
   │  XREADGROUP → parse → idempotency → resolve session → followup/steer/new_session
   ▼
harness-redis-connector  (this plugin, inside a dsh deployment)
   │  creates/resumes agents; observes session/event firehose
   ▼
Harness Agent/Session   (persisted under DSH_HOME, see §5)
   │  assistant/message + agent/error
   ▼
Redis Streams  harness:outbound  (consumer group: flink-harness-inbound)
   │  XREADGROUP
   ▼
flink-agents: HarnessRedisToTelegramPipeline  →  Telegram chat
```

**Deployment facts (this workspace, 2026-08):**

| Thing | Value / location |
|---|---|
| Redis | `redis://10.27.122.182:30379` (connector `redisUrl` in `apps/cli/config/agent-presets/standard/agent.cordis.yml`; Flink `REDIS_URI` in `flink-agents/scripts/flink-job.env`) |
| Connector config | standard preset `agent.cordis.yml`, row `harness-redis-connector`: `provider`, `model`, optional `cwd` |
| Inbound stream / group | `harness:inbound` / `harness-redis-connector` |
| Outbound stream / group | `harness:outbound` / `flink-harness-inbound` |
| Stream entry field | single field `json` (envelope JSON string) |
| Conversation id | `telegram:chat:<chatId>` (e.g. `telegram:chat:7365099493`) |
| Binding keys (Redis hashes) | `harness-redis-connector:binding:conversation_to_session` and `...:session_to_conversation` |
| Session persistence | `~/.dsh/sessions/<cwd-slug>/<sessionId>/session.jsonl.zstd`; sessions without a `cwd` land under `~/.dsh/sessions/_no-cwd/` |
| Web GUI | same session store (`~/.dsh`); sessions listed via `session.list` RPC (see §5) |

The deployment persona (rendered as the `deployment:persona` prompt section)
is a template: `{{model}}` and `{{cwd}}` must resolve or prompt assembly fails.
`{{model}}` comes from the agent route; `{{cwd}}` comes from
`agent.session.header.cwd`.

## 2. Error catalog

### E1 — `prompt variable "{{model}}" has no value for this assembly (section "deployment:persona")`

**Cause:** the connector creates agents without a model route. `agentOptions`
in `src/plugin-entry.ts` forwards `provider`/`model` from config; if either is
missing the persona cannot render.

**Fix / triage:**
1. Check the connector config row sets `provider` and `model`
   (`agent.cordis.yml` preset, e.g. `deepseek-official` / `deepseek-v4-flash`).
2. The agent keeps its own route only for **new** sessions. A session created
   before the route was configured may still fail until it is re-created
   (see R1).
3. `pnpm --filter harness-redis-connector build` then restart the connector
   process (see R3) — the running process must load the new `lib/`.

### E2 — `prompt variable "{{cwd}}" has no value for this assembly (section "deployment:persona")`

The recurring one. `{{cwd}}` resolves from `agent.session.header.cwd`, which is
set **only at session creation** from `meta.cwd`. Two independent causes:

**Cause 2a — session created by an old connector without `meta.cwd`.**
Before `meta: { cwd: config.cwd ?? process.cwd() }` was added to the create path
(`src/plugin-entry.ts`), every session the connector created had no `cwd` in its
header. Those sessions persist under `~/.dsh/sessions/_no-cwd/`.

**Cause 2b — a no-`cwd` session is resumed.** `resume` keeps the persisted
header; it never repairs a missing `cwd`. Even with the fixed lib running, a
conversation still bound to a pre-fix session fails on every message.

**Diagnosis chain (see §4/§5 for commands):**
1. Read the binding: which session is `telegram:chat:<chatId>` bound to?
2. Inspect that session's persisted header: does it have a `cwd` field?
3. If it lives under `_no-cwd/` or the header lacks `cwd` → this error.

**Fixes:**
- **Heal the conversation (immediate, protocol-sanctioned):** append a
  `new_session` envelope to `harness:inbound` (R1). The connector creates a
  fresh session **with** `meta.cwd` and rebinds the conversation.
- **Prevent recurrence (hardening, already shipped):** the session resolver
  never resumes a persisted session whose header lacks `cwd` — it treats it as
  not persisted and falls through to a fresh create. Requires the rebuilt lib
  **and** a connector restart to be live (R3).
- **Choose the session working directory:** the fresh session's `cwd` defaults
  to the connector process's `process.cwd()`. Set the `cwd` config key in the
  connector preset to pin it (e.g. `/Users/zhengwei/Documents/private-workspace`).

### E3 — code changes appear to have no effect

**Cause:** the connector is a built ESM plugin; the running process loads
`lib/`, not `src/`, and keeps modules in memory until restarted.

**Fix:** after any `src/` change: `pnpm --filter harness-redis-connector build`
from the harness root, then restart the connector process (R3). Verify the
running process started after the rebuild (Redis `CLIENT LIST` shows the
ioredis consumer's connection age, §4).

### E4 — replies come from the wrong session, or a conversation must use a specific session

The connector maps one conversation → one session via the Redis binding hashes.
`message`/`steer` never move a conversation between sessions (an explicit
mismatched `session_id` is rejected); the only protocol rebind is `new_session`,
which always creates a **fresh** session. To bind a conversation to a
**specific existing** session (e.g. a web GUI session), update the binding
hashes directly with the connector's own rebind script — see R2.

**Pitfall:** a session bound to both the web GUI and the Telegram bridge can be
driven by two processes at once; turns may interleave. Prefer idle sessions and
do not chat from both surfaces at the same time.

### E5 — no reply at all (delivery chain triage)

Walk the chain in order:
1. **Inbound:** `XINFO GROUPS harness:inbound` — is the connector's group
   consuming (`lag` ≈ 0, `pending` small)? If `pending` grows, the connector is
   not ACKing (admission failing) or not running.
2. **Session:** is the agent actually turning? Watch the session log under
   `~/.dsh/sessions/` for new events, or the web GUI.
3. **Outbound:** `XINFO GROUPS harness:outbound` — is `flink-harness-inbound`
   consuming? If `pending` grows, the Flink outbound job is down.
4. **Telegram:** if outbound is consumed but no message arrives, check the
   Flink job's Telegram sink (bot token, default chat id).

## 3. Recovery procedures

### R1 — heal a conversation bound to a broken (no-`cwd`) session

Append a `new_session` envelope for that conversation. The connector parses it
(version must be `1`, `type` ∈ `message|new_session|steer`, `content.text` a
string), creates a fresh session, rebinds, and delivers `content.text` to the
fresh agent as a followup:

```json
{"version":1,"conversation_id":"telegram:chat:<chatId>","session_id":null,
 "message_id":"telegram-heal-<unique>","type":"new_session",
 "content":{"text":"New session started."},"metadata":{}}
```

Push with ioredis (`XADD harness:inbound * json <json>`), then verify:
- `conversation_to_session` now points at a new session id;
- the new session's header has a `cwd` (not under `_no-cwd/`);
- the connector ACKs (`pending` back to 0) and an `assistant_message` lands on
  `harness:outbound`.

### R2 — rebind a conversation to a specific existing session

Run the connector's own atomic rebind Lua script (identical to
`RedisConversationBindingStore.bind`, preserving the one-active invariant —
stale reverse/forward entries are removed):

```js
const script = `
local oldSession = redis.call('HGET', KEYS[1], ARGV[1])
local oldConversation = redis.call('HGET', KEYS[2], ARGV[2])
if oldSession and oldSession ~= ARGV[2] then redis.call('HDEL', KEYS[2], oldSession) end
if oldConversation and oldConversation ~= ARGV[1] then redis.call('HDEL', KEYS[1], oldConversation) end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[1])
return 1`
// KEYS[1] = harness-redis-connector:binding:conversation_to_session
// KEYS[2] = harness-redis-connector:binding:session_to_conversation
// ARGV[1] = telegram:chat:<chatId>, ARGV[2] = <target sessionId>
```

The binding store reads through Redis on every message (no cache), so this takes
effect immediately — no restart. The target session must be persisted **with a
`cwd` header** (the hardened resolver refuses to resume no-`cwd` sessions). Do
not bind a session that is live/running in the web GUI (§E4 pitfall).

### R3 — restart the connector after a rebuild

1. From the harness checkout: `pnpm --filter harness-redis-connector build`
   (typecheck/test first: `pnpm --filter harness-redis-connector typecheck test`).
2. Stop and start the connector process (the terminal/deployment that runs the
   dsh composition mounting `harness-redis-connector`).
3. Sanity check: Redis `CLIENT LIST` shows a fresh ioredis consumer age, and
   `XINFO GROUPS harness:inbound` shows the group consuming again.

## 4. Diagnostic snippets

Redis is not on localhost in this workspace; use ioredis from the harness
workspace (`require('ioredis')` from the harness root) against
`10.27.122.182:30379`:

```js
const r = new Redis({ host: '10.27.122.182', port: 30379 })
await r.hgetall('harness-redis-connector:binding:conversation_to_session')
await r.xinfo('GROUPS', 'harness:inbound')          // connector group: lag/pending
await r.xinfo('GROUPS', 'harness:outbound')         // flink group: lag/pending
await r.xinfo('CONSUMERS', 'harness:inbound', 'harness-redis-connector')
await r.xrevrange('harness:inbound', '+', '-', 'COUNT', 3)   // recent envelopes
await r.call('CLIENT', 'LIST')                      // find ioredis XREADGROUP consumer + age
```

Session files (zstd-compressed JSONL, first line is the header):

```sh
zstd -dc ~/.dsh/sessions/--<cwd-slug>--/<sessionId>/session.jsonl.zstd | head -1
ls ~/.dsh/sessions/_no-cwd/   # any session here has no cwd header → E2
```

Session ids from the web GUI (see also the web-side session discovery):

```sh
# current session
JSON.parse(localStorage.getItem('dsh.sessions.current')).sessionId   # browser console
# list all sessions
curl -s -X POST http://127.0.0.1:3080/api/session.list \
  -H 'content-type: application/json' \
  -d '{"type":"client-request","rpcId":"probe","method":"session.list","payload":{}}'
# items[].sessionId | .cwd | .agentPreset | .projections.values.title
```

## 5. Invariants to preserve

- **One conversation ↔ one active session.** Only `new_session` or an explicit
  rebind (R2) moves a conversation; never hand-edit half of the binding pair.
- **`cwd` is set at creation only.** Sessions without `cwd` in the header are
  permanently broken for `{{cwd}}`; heal by re-creating, never by editing the
  log header of a live session.
- **ACK after admission, never after response.** A growing `pending` on
  `harness:inbound` means admission is failing (malformed envelope, binding
  conflict, resolve error), not that the agent is slow.
- **Wire field is `json`; version is `1`; unknown `type` is ignored (not an
  error); missing/unsupported version is malformed.**
- **Rebuilt `lib/` needs a restart** (E3) — `src/` edits alone change nothing
  for a running deployment.

## 6. Incident log (2026-08-17/18) — the `{{cwd}}` outage

Useful as a triage pattern match:

1. `{{model}}` failure on Telegram replies → fixed by configuring
   `provider`/`model` on the connector row.
2. Then `{{cwd}}` failure: the connector created agents **without** `meta.cwd`
   (old lib). A session created that day (`83f34ac2-…`, header without `cwd`)
   was bound to `telegram:chat:7365099493`; every message resumed it and prompt
   assembly failed.
3. Evidence trail: Redis binding → session header on disk (under `_no-cwd/`,
   header had no `cwd`) → `{{cwd}}` = `agent.session.header.cwd` (undefined).
4. Fixes shipped: `meta.cwd` on create (`config.cwd ?? process.cwd()`);
   resolver hardening (never resume a no-`cwd` session); runtime heal via
   `new_session` envelope; rebind of the conversation to a healthy web session
   (`session-3fdee977-…`, cwd `/Users/zhengwei/Documents/private-workspace`).
5. Operational lesson: after fixing the plugin, **restart the connector** — the
   pre-fix binding survived the rebuild and kept failing until the conversation
   was re-created/rebound.
