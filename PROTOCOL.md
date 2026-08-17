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

Field names inside the envelope are snake_case at the envelope level. `content`
and `metadata` are objects.

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

`type` ∈ `message` | `new_session` | `steer`.

- `message`     → `agent.followup(...)`
- `steer`       → `agent.steer(...)`
- `new_session` → create a fresh Harness Agent/Session and rebind the conversation to it (do NOT delete the prior session).

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

`type` ∈ `assistant_message` | `error`.

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
- The Harness connector XACKs after the message is safely admitted to Harness
  (i.e. after idempotency check + session resolution + `followup`/`steer`/rebind),
  NOT after the agent finishes producing a response.
- Pending-entry recovery (`XPENDING` / `XAUTOCLAIM`) is used for consumer restart.
- Poison messages (malformed, permanently invalid) are logged and eventually
  acknowledged or parked; a dedicated DLQ is deferred for MVP.
