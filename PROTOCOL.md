# DeepSeek Harness ↔ Redis Streams Interop Protocol

Single source of truth for the wire contract between the `flink-agents` project
(Java/Flink, Telegram integration) and the `harness-redis-connector` project
(TypeScript, DeepSeek Harness plugin). Both sides implement exactly this.

## Transport

Redis **Streams** (not the legacy Redis Lists used elsewhere in `flink-agents`).

- `harness:inbound`  — external/Flink → Harness. The Harness connector consumes this via a consumer group.
- `harness:outbound` — Harness → external/Flink. The Flink inbound job consumes this.

Names are configurable on both sides via env/config; the defaults above are the
agreed values. Consumer groups:

- `harness-redis-connector` — the Harness connector's group on `harness:inbound`.
- `flink-harness-inbound`   — the Flink inbound job's group on `harness:outbound`.

## Serialization

Each stream entry is one Redis hash with a single field whose value is the
envelope JSON string. The canonical field name is **`json`**: producers write
`XADD <stream> * json <envelope-json>`. Consumers read the `json` field and MAY
fall back to the first (only) field value for tolerance.

Field names inside the envelope are snake_case at the envelope level. `metadata`
is an object; `content` is normally an object, with the outbound markdown form
below as an additive exception.

### Inbound (Flink → Harness)

```json
{
  "version": 1,
  "conversation_id": "telegram:chat:12345",
  "session_id": null,
  "message_id": "telegram-update-1001",
  "type": "message",
  "content": { "text": "帮我检查一下这个项目" },
  "metadata": {}
}
```

`type` ∈ `message` | `new_session` | `steer` | `question_answer` | `system_command`.

- `message`       → `agent.followup(...)`
- `steer`         → `agent.steer(...)`
- `new_session`   → create a fresh Harness Agent/Session and rebind the conversation to it (do NOT delete the prior session).
- `question_answer` → a structured answer to a prior `question_request` (see Outbound). This is NOT a chat message: it resolves a pending `ask()` on the connector side and MUST never start a new turn (do not call `session.prompt`/`followup`/`steer`).
- `system_command` → a frontend-independent control request. It never starts an agent turn and receives a deterministic `system_command_result` (see below).

Example `system_command` (Flink → Harness):

```json
{
  "version": 1,
  "conversation_id": "telegram:<botId>:chat:<chatId>",
  "session_id": null,
  "message_id": "telegram-update-1001",
  "type": "system_command",
  "content": { "command": "renew_session" },
  "metadata": { "chatId": "<chatId>", "botId": "<botId>" }
}
```

`content.command` must be a non-empty string. Validation deliberately does not
restrict its value: the connector handler decides whether a command is
implemented, allowing future additive commands at version 1.

Example `question_answer` (Flink → Harness):

```json
{
  "version": 1,
  "type": "question_answer",
  "correlation_id": "b2f5e8c6-...",
  "conversation_id": "telegram:chat:12345",
  "session_id": "harness-session-id",
  "message_id": "telegram-callback-42",
  "content": { "answers": [{ "id": "q1", "value": "选项A" }] },
  "metadata": {}
}
```

`content.answers` is an array of { id, value } objects. `value` is a string for single-select answers and a string[] for multi-select answers.

### Outbound (Harness → Flink)

```json
{
  "version": 1,
  "conversation_id": "telegram:chat:12345",
  "session_id": "harness-session-id",
  "correlation_id": "telegram-update-1001",
  "type": "assistant_message",
  "content": { "text": "..." },
  "metadata": { "chatId": "12345" }
}
```

`type` ∈ `assistant_message` | `error` | `question_request` | `system_command_result`.

- `assistant_message` → normal assistant output.
- `error`             → error routing/processing info.
- `question_request`  → request that the external side render a question to the user and return a `question_answer` (see Inbound).
- `system_command_result` → deterministic, frontend-independent result of a
  `system_command`; the external side renders `content.message` directly.

### System control commands

Version 1 currently supports only `renew_session`. It creates and initializes a
fresh Harness session through the normal connector creation path, atomically
rebinds the conversation to it, and does **not** call `followup`, `steer`, or
otherwise invoke an LLM/agent turn. The prior session is not deleted.
The fresh session inherits the source session's provider, model, cwd, and agent
preset, and is titled `bot-<botId>`; these are connector-side execution details,
not new wire fields.

On success the connector emits:

```json
{
  "version": 1,
  "conversation_id": "telegram:<botId>:chat:<chatId>",
  "session_id": "<new-session-id>",
  "message_id": "<echoed message_id>",
  "type": "system_command_result",
  "content": { "command": "renew_session", "status": "ok", "session_id": "<new-session-id>", "message": "Session renewed." },
  "metadata": { "chatId": "<chatId>", "botId": "<botId>" }
}
```

On a create/bind failure it leaves the active binding unchanged and emits
`status: "error"`, the active `session_id` (or `null`), and
`content.message: "Session renewal failed."`. An unrecognized command emits
an error result without creating or driving an agent. `content.status` is always
`ok` or `error`.

Command handling is at-least-once but transition-at-most-once per `message_id`:
the connector first checks a Redis command-result record, re-emitting its stored
serialized result on duplicate delivery. On the first delivery it claims the
normal receipt, performs the command once, stores the serialized result with an
expiry **before** XADD, then emits and ACKs. If XADD fails the entry remains
pending and redelivery replays the stored result. If a prior attempt has a
receipt but no result record (for example a crash in the small interval before
storage), it emits `"Session renewal could not be confirmed."` and never repeats
the transition. Future system commands use this same result-store/receipt model.

`/renew` takes precedence over an interactive free-text reply: when it is sent in
reply to a pending question, it is treated as a `system_command` and does not
consume the pending free-text answer.

`assistant_message` may add top-level `content_type: "text/markdown"` and use a
plain string `content` (rather than `{ "text": "..." }`). This is additive at
version 1: legacy object-shaped content remains accepted. The Flink bridge
converts its supported CommonMark subset to Telegram MarkdownV2.

Example `question_request` (Harness → Flink):

```json
{
  "version": 1,
  "type": "question_request",
  "correlation_id": "b2f5e8c6-...",
  "conversation_id": "telegram:chat:12345",
  "session_id": "harness-session-id",
  "content": {
    "questions": [
      { "id": "q1", "text": "Which option?", "options": ["A","B"], "multi_select": false }
    ]
  },
  "metadata": {}
}
```

`content.questions` is an array of { id, text, options?: string[], multi_select?: boolean }. Option-less questions are free-text prompts; `options` are label strings and `multi_select` (optional) enables multi-select (single-select default). `correlation_id` is an opaque UUID chosen by the Harness side and echoed back on the `question_answer`.

## Field semantics (mapping to the existing flink-agents contract)

The existing `flink-agents` contract is raw JSON in Redis Lists with no envelope.
The new envelope is a minimal, documented extension that reuses its fields. The
Flink producer (`HarnessEnvelope.fromInbound`) derives these from the normalized
`TelegramInboundMessage` fields:

| Envelope field    | Flink producer source                                                    |
|-------------------|--------------------------------------------------------------------------|
| `conversation_id` | `source + ":chat:" + chatId` (e.g. `telegram:chat:<chatId>`)             |
| `message_id`      | `telegram-update-<updateId>`, else `telegram-message-<messageId>`         |
| `content.text`    | the Telegram message `text`                                               |
| `metadata`        | passthrough of the normalized update (`chatId`, `source`, `sourceType`, `updateId`, `messageId`, `fromUserId`, `fromUsername`, `fromFirstName`, `messageDate`, `chatType`, `chatTitle`) |
| `correlation_id`  | the originating inbound `message_id` — correlation metadata, NOT a strict request→one-response id |

`conversation_id` and `session_id` are always both present on outbound so the
Flink side can route the response (recover `chatId` from `metadata.chatId`, or by
parsing the `telegram:chat:` prefix of `conversation_id`).

## Versioning / unknown types

- A missing/unsupported `version` is a malformed event: log, emit `error` if
  routing is known, otherwise drop, and do not retry forever.
- An unknown `type` is logged and dropped (safe-ignore). This is forward
  compatibility, not an error.

## Delivery semantics

- At-least-once. The external `message_id` is the application-level idempotency key.
  Its `telegram-update-<updateId>` form is not bot-scoped, a known pre-existing
  limitation shared with normal message deduplication.
- The Harness connector XACKs after the message is safely admitted to Harness
  (i.e. after idempotency check + session resolution + `followup`/`steer`/rebind),
  NOT after the agent finishes producing a response.
- For `system_command`, the command result is durably stored before outbound
  emission; the entry is ACKed only after result emission succeeds.
- Pending-entry recovery (`XPENDING` / `XAUTOCLAIM`) is used for consumer restart.
- Poison messages (malformed, permanently invalid) are logged and eventually
  acknowledged or parked; a dedicated DLQ is deferred for MVP.

## Approvals (reserved question id)

The connector also uses the interactive question wire to route approvals for
bot sessions. Approvals are implemented as a special question request with a
single question id of `"approval"` and two options `"Approve"` / `"Deny"`.
The external side (Flink/Telegram) should render this request as a confirm
prompt to the user and emit a `question_answer` with the same `correlation_id`.

Mapping on answer resolution (Harness connector → Flink → Harness reaction):
- Answer `Approve`  → mapped to resolved approval result `allowed-once`.
- Answer `Deny`     → mapped to resolved approval result `rejected`.
- Unknown/timeout/abort/send-failure → `unavailable` / `cancelled` as
  appropriate (connector will handle lifecycle cleanup and error mapping).

The connector keeps pending registry entries discriminated by kind
(`question` | `approval`) so approval answers are resolved as approvals and
question answers as questions.
