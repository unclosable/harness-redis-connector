import { describe, expect, it } from 'vitest'
import { InMemoryConversationBindingStore } from '../src/binding-store/index.js'

describe('conversation binding', () => {
  it('binds and reverse-looks-up', async () => {
    const store = new InMemoryConversationBindingStore()
    await store.bind('telegram:chat:1', 'session-a')
    expect(await store.sessionIdFor('telegram:chat:1')).toBe('session-a')
    expect(await store.conversationIdFor('session-a')).toBe('telegram:chat:1')
  })

  it('rebinds on new_session without a dangling reverse mapping', async () => {
    const store = new InMemoryConversationBindingStore()
    await store.bind('telegram:chat:1', 'session-a')
    await store.bind('telegram:chat:1', 'session-b')
    expect(await store.sessionIdFor('telegram:chat:1')).toBe('session-b')
    expect(await store.conversationIdFor('session-a')).toBeUndefined()
    expect(await store.conversationIdFor('session-b')).toBe('telegram:chat:1')
  })

  it('enforces one-active: moving a session to another conversation unlinks the old one', async () => {
    const store = new InMemoryConversationBindingStore()
    await store.bind('conv-1', 'session-x')
    await store.bind('conv-2', 'session-x')
    expect(await store.sessionIdFor('conv-1')).toBeUndefined()
    expect(await store.sessionIdFor('conv-2')).toBe('session-x')
    expect(await store.conversationIdFor('session-x')).toBe('conv-2')
  })
})
