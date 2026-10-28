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
    const routing = deriveTelegramRoutingMetadata(conversationId)
    return routing ?? {}
  }
}

/** Recover the chat id from a legacy or multi-bot Telegram conversation id. */
export function deriveChatId(conversationId: string): string | undefined {
  return deriveTelegramRoutingMetadata(conversationId)?.chatId
}

/**
 * Recover routing metadata when process-local inbound state is empty after a
 * restart, so a multi-bot conversation's first reply keeps its original bot.
 */
function deriveTelegramRoutingMetadata(conversationId: string): { chatId: string; botId?: string } | undefined {
  const multiBotMatch = /^telegram:([^:]+):chat:(.+)$/.exec(conversationId)
  if (multiBotMatch !== null) {
    return { chatId: multiBotMatch[2]!, botId: multiBotMatch[1]! }
  }

  const legacyMatch = /^telegram:chat:(.+)$/.exec(conversationId)
  return legacyMatch !== null ? { chatId: legacyMatch[1]! } : undefined
}
