/**
 * In-memory runtime state shared between the inbound adapter (which records the
 * latest inbound correlation and metadata per conversation) and the outbound
 * projector (which reads them to route responses). This state is process-local
 * and rebuilt on restart; it is routing convenience, not durable truth.
 *
 * @module harness-redis-connector/state
 */

export interface ConversationState {
  /** Record the latest inbound message for a conversation (correlation + metadata passthrough). */
  recordInbound(conversationId: string, messageId: string, metadata: Record<string, unknown>): void

  /** The most recent inbound `message_id` for a conversation, if any. */
  correlationFor(conversationId: string): string | undefined

  /** The metadata to round-trip on outbound envelopes for a conversation. */
  metadataFor(conversationId: string): Record<string, unknown>
}

/** Map-backed {@link ConversationState}. */
export class InMemoryConversationState implements ConversationState {
  private readonly correlation = new Map<string, string>()
  private readonly metadata = new Map<string, Record<string, unknown>>()

  recordInbound(conversationId: string, messageId: string, metadata: Record<string, unknown>): void {
    this.correlation.set(conversationId, messageId)
    this.metadata.set(conversationId, metadata)
  }

  correlationFor(conversationId: string): string | undefined {
    return this.correlation.get(conversationId)
  }

  metadataFor(conversationId: string): Record<string, unknown> {
    const stored = this.metadata.get(conversationId)
    if (stored !== undefined) return stored
    const chatId = deriveChatId(conversationId)
    return chatId !== undefined ? { chatId } : {}
  }
}

/** Recover the chat id from a `telegram:chat:<chatId>` conversation id. */
export function deriveChatId(conversationId: string): string | undefined {
  const match = /^telegram:chat:(.+)$/.exec(conversationId)
  return match?.[1]
}
