/**
 * Redis Streams interop wire contract between `flink-agents` and this connector.
 *
 * Field names are snake_case at the envelope level; `content` and `metadata`
 * are plain objects. The authoritative source of truth is `PROTOCOL.md` at the
 * connector root; this module is the TypeScript projection of that contract.
 *
 * @module harness-redis-connector/protocol
 */

/** The single supported envelope version (see `PROTOCOL.md#versioning`). */
export const PROTOCOL_VERSION = 1

/** Known inbound message kinds. An unknown value is forward-compatible, not an error. */
export const INBOUND_TYPES = ['message', 'new_session', 'steer'] as const

/** Inbound `type` value: `message` | `new_session` | `steer`. */
export type InboundType = (typeof INBOUND_TYPES)[number]

/** Known outbound message kinds. */
export const OUTBOUND_TYPES = ['assistant_message', 'error'] as const

/** Outbound `type` value: `assistant_message` | `error`. */
export type OutboundType = (typeof OUTBOUND_TYPES)[number]

/** Text content carried by an inbound envelope. */
export interface InboundContent {
  /** The message text (the Telegram instruction). */
  text: string
}

/**
 * A validated inbound envelope (Flink → Harness).
 * `session_id` is `null` when the sender leaves session routing to the binding.
 */
export interface InboundEnvelope {
  version: number
  conversation_id: string
  session_id: string | null
  message_id: string
  type: InboundType
  content: InboundContent
  metadata: Record<string, unknown>
}

/** Text content carried by an outbound envelope. */
export interface OutboundContent {
  text: string
}

/** An outbound envelope (Harness → Flink). */
export interface OutboundEnvelope {
  version: number
  conversation_id: string
  session_id: string | null
  correlation_id: string | null
  type: OutboundType
  content: OutboundContent
  metadata: Record<string, unknown>
}
