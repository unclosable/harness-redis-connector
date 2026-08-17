import type { ConversationBindingStore } from './types.js'

/**
 * In-memory {@link ConversationBindingStore}. This is the canonical,
 * unit-tested implementation of the one-active invariant; the Redis backend
 * mirrors the same transitions in a Lua script.
 */
export class InMemoryConversationBindingStore implements ConversationBindingStore {
  private readonly byConversation = new Map<string, string>()
  private readonly bySession = new Map<string, string>()

  async bind(conversationId: string, sessionId: string): Promise<void> {
    const oldSession = this.byConversation.get(conversationId)
    const oldConversation = this.bySession.get(sessionId)
    if (oldSession !== undefined && oldSession !== sessionId) this.bySession.delete(oldSession)
    if (oldConversation !== undefined && oldConversation !== conversationId) this.byConversation.delete(oldConversation)
    this.byConversation.set(conversationId, sessionId)
    this.bySession.set(sessionId, conversationId)
  }

  async sessionIdFor(conversationId: string): Promise<string | undefined> {
    return this.byConversation.get(conversationId)
  }

  async conversationIdFor(sessionId: string): Promise<string | undefined> {
    return this.bySession.get(sessionId)
  }
}
