import type { Redis } from 'ioredis'
import type { ConversationBindingStore } from './types.js'

/**
 * Rebinds one conversation↔session pair atomically, preserving the one-active
 * invariant:
 * - KEYS[1] = conversation → session hash, KEYS[2] = session → conversation hash
 * - ARGV[1] = conversationId, ARGV[2] = sessionId
 *
 * Removes the old session's reverse mapping when the conversation moves, the old
 * conversation's forward mapping when the session moves, then writes both sides.
 */
const REBIND_SCRIPT = `
local oldSession = redis.call('HGET', KEYS[1], ARGV[1])
local oldConversation = redis.call('HGET', KEYS[2], ARGV[2])
if oldSession and oldSession ~= ARGV[2] then
  redis.call('HDEL', KEYS[2], oldSession)
end
if oldConversation and oldConversation ~= ARGV[1] then
  redis.call('HDEL', KEYS[1], oldConversation)
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[1])
return 1
`

/**
 * Redis-hash {@link ConversationBindingStore} using two hashes under a shared
 * key prefix. Rebinding runs atomically through {@link REBIND_SCRIPT}.
 */
export class RedisConversationBindingStore implements ConversationBindingStore {
  private readonly conversationKey: string
  private readonly sessionKey: string

  constructor(private readonly client: Redis, keyPrefix: string) {
    this.conversationKey = `${keyPrefix}:conversation_to_session`
    this.sessionKey = `${keyPrefix}:session_to_conversation`
  }

  async bind(conversationId: string, sessionId: string): Promise<void> {
    await this.client.eval(REBIND_SCRIPT, 2, this.conversationKey, this.sessionKey, conversationId, sessionId)
  }

  async sessionIdFor(conversationId: string): Promise<string | undefined> {
    const value = await this.client.hget(this.conversationKey, conversationId)
    return value ?? undefined
  }

  async conversationIdFor(sessionId: string): Promise<string | undefined> {
    const value = await this.client.hget(this.sessionKey, sessionId)
    return value ?? undefined
  }
}
