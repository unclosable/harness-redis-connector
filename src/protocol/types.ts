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
export const INBOUND_TYPES = ['message', 'new_session', 'steer', 'question_answer'] as const

/** Inbound `type` value. */
export type InboundType = (typeof INBOUND_TYPES)[number]

/** Known outbound message kinds. */
export const OUTBOUND_TYPES = ['assistant_message', 'error', 'question_request'] as const

/** Outbound `type` value. */
export type OutboundType = (typeof OUTBOUND_TYPES)[number]

/** Text content carried by an inbound message envelope. */
export interface InboundContentMessage {
  /** The message text (the Telegram instruction). */
  text: string
}

/** Content carried by an inbound question_answer envelope. */
export interface InboundContentQuestionAnswer {
  answers: { id: string; value: string | string[] }[]
}

/**
 * A validated inbound envelope (Flink → Harness).
 * `session_id` is `null` when the sender leaves session routing to the binding.
 * `correlation_id` is present for answers that correlate to prior requests.
 */
export interface InboundEnvelope {
  version: number
  conversation_id: string
  session_id: string | null
  message_id: string
  correlation_id?: string | null
  type: InboundType
  content: InboundContentMessage | InboundContentQuestionAnswer
  metadata: Record<string, unknown>
}

/** One question in an outbound `question_request`. */
export interface OutboundQuestion {
  id: string
  text: string
  options?: string[]
  multi_select?: boolean
}

/** Content for outbound assistant/error messages or a question_request. */
export type OutboundContent =
  | { text: string }
  | { questions: OutboundQuestion[] }

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
