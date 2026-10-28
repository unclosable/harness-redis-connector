import { describe, expect, it } from 'vitest'
import { InMemoryConversationState } from '../src/state/conversation-state.js'

describe('in-memory conversation state', () => {
  it('derives chat and bot routing metadata for an operator multi-bot conversation', () => {
    const state = new InMemoryConversationState()

    expect(state.metadataFor('telegram:operator:chat:7365099493')).toEqual({
      chatId: '7365099493',
      botId: 'operator',
    })
  })

  it('derives chat and bot routing metadata for a personal multi-bot conversation', () => {
    const state = new InMemoryConversationState()

    expect(state.metadataFor('telegram:personal:chat:7365099493')).toEqual({
      chatId: '7365099493',
      botId: 'personal',
    })
  })

  it('derives only chat routing metadata for a legacy Telegram conversation', () => {
    const state = new InMemoryConversationState()
    const metadata = state.metadataFor('telegram:chat:7365099493')

    expect(metadata).toEqual({ chatId: '7365099493' })
    expect(metadata).not.toHaveProperty('botId')
  })

  it.each(['garbage', 'telegram:', ''])('returns empty metadata for malformed conversation id %j', conversationId => {
    const state = new InMemoryConversationState()

    expect(state.metadataFor(conversationId)).toEqual({})
  })

  it('returns stored inbound metadata unchanged for a multi-bot conversation', () => {
    const state = new InMemoryConversationState()
    const metadata = { chatId: 'override-chat', botId: 'override-bot', source: 'inbound' }
    state.recordInbound('telegram:operator:chat:7365099493', 'message-1', metadata)

    expect(state.metadataFor('telegram:operator:chat:7365099493')).toBe(metadata)
  })
})
