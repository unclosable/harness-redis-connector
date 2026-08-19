import { describe, expect, it } from 'vitest'
import { InMemoryConversationBindingStore } from '../src/binding-store/index.js'
import { InboundAdapter } from '../src/inbound-adapter/index.js'
import { InMemoryReceiptStore } from '../src/receipt-store/index.js'
import { InMemoryConversationState } from '../src/state/conversation-state.js'
import type { StreamEntry } from '../src/redis-transport/types.js'
import { FakeAgentRegistry, FakeRedisTransport } from './helpers/fakes.js'

function makeAnswerEntry(messageId: string, overrides: Record<string, unknown> = {}): StreamEntry {
  return {
    id: `1-${messageId}`,
    fields: {
      json: JSON.stringify({
        version: 1,
        conversation_id: 'telegram:chat:1',
        session_id: null,
        message_id: messageId,
        correlation_id: overrides.correlation_id ?? 'corr-1',
        type: 'question_answer',
        content: { answers: overrides.answers ?? [{ id: 'q1', value: 'yes' }] },
        metadata: {},
      }),
    },
  }
}

function setup() {
  const transport = new FakeRedisTransport()
  const bindings = new InMemoryConversationBindingStore()
  const receipts = new InMemoryReceiptStore({ ttlMs: 60_000 })
  const state = new InMemoryConversationState()
  const agents = new FakeAgentRegistry()
  let next = 0
  const resolver = undefined as any
  const ownedHandles = new Set()
  const logger = { warn: () => {}, error: () => {}, info: () => {} }
  const pending = new Map<string, any>()
  const adapter = new InboundAdapter({
    transport,
    // resolver not needed for question_answer path
    resolver: { resolve: async () => { throw new Error('not used') } },
    receipts,
    bindings,
    state,
    ownedHandles,
    createUserMessage: text => ({ id: `msg-${next}`, text }),
    logger,
    inboundStream: 'harness:inbound',
    consumerGroup: 'g',
    consumerName: 'c',
    pendingQuestions: pending,
  })
  return { adapter, transport, pending }
}

describe('user-questions inbound handling', () => {
  it('resolves a pending ask when a question_answer arrives', async () => {
    const { adapter, transport, pending } = setup()

    let resolved: any = null
    const p = new Promise(resolve => { resolved = resolve })
    pending.set('corr-1', { conversationId: 'telegram:chat:1', resolve: (a: any) => resolve(a), reject: (e: any) => resolve({ err: e }) })

    const outcome = await adapter.processEntry(makeAnswerEntry('m1', { correlation_id: 'corr-1', answers: [{ id: 'q1', value: ['a','b'] }] }))
    // awaiting a microtask to ensure pending resolution propagated
    await Promise.resolve()

    expect(outcome.admitted).toBe(true)
    // pending should have been removed
    expect(pending.has('corr-1')).toBe(false)
  })

  it('unknown correlation_id is acked and ignored', async () => {
    const { adapter, transport } = setup()
    const outcome = await adapter.processEntry(makeAnswerEntry('m2', { correlation_id: 'unknown' }))
    expect(outcome.admitted).toBe(false)
    expect(outcome.acked).toBe(true)
  })
})
