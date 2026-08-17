/**
 * Bidirectional conversation ↔ session binding. Invariant: one active
 * conversation maps to at most one active session, and one active session maps
 * to at most one active conversation. Rebinding either side clears the other
 * side's prior association.
 *
 * @module harness-redis-connector/binding-store
 */

export interface ConversationBindingStore {
  /**
   * Bind `conversationId` to `sessionId`, clearing any prior association on
   * either side first (one-active invariant).
   */
  bind(conversationId: string, sessionId: string): Promise<void>

  /** The session currently bound to `conversationId`, if any. */
  sessionIdFor(conversationId: string): Promise<string | undefined>

  /** The conversation currently bound to `sessionId`, if any. */
  conversationIdFor(sessionId: string): Promise<string | undefined>
}
