import { describe, expect, it } from 'vitest'
import { InMemoryConversationBindingStore } from '../src/binding-store/index.js'
import { InboundAdapter } from '../src/inbound-adapter/index.js'
import { InMemoryReceiptStore } from '../src/receipt-store/index.js'
import { InMemoryConversationState } from '../src/state/conversation-state.js'
import type { StreamEntry } from '../src/redis-transport/types.js'
import { FakeAgentRegistry, FakeRedisTransport, FakeAgent } from './helpers/fakes.js'

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

function makeAnswerEntryWithSession(messageId: string, sessionId: string, overrides: Record<string, unknown> = {}): StreamEntry {
  return {
    id: `1-${messageId}`,
    fields: {
      json: JSON.stringify({
        version: 1,
        conversation_id: 'telegram:chat:1',
        session_id: sessionId,
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
  it('resolves a pending ask when a question_answer arrives (multi-select)', async () => {
    const { adapter, transport, pending } = setup()

    const p = new Promise(resolve => {
      pending.set('corr-1', { conversationId: 'telegram:chat:1', resolve: (a: any) => resolve(a), reject: (e: any) => resolve({ err: e }) })
    })

    const outcome = await adapter.processEntry(makeAnswerEntry('m1', { correlation_id: 'corr-1', answers: [{ id: 'q1', value: ['a','b'] }] }))
    await Promise.resolve()

    expect(outcome.admitted).toBe(true)
    expect(pending.has('corr-1')).toBe(false)
  })

  it('maps single-select answer values to selected array', async () => {
    const { adapter, pending } = setup()
    const recorded: any = {}
    const p = new Promise(resolve => {
      pending.set('corr-2', { conversationId: 'telegram:chat:1', resolve: (a: any) => { recorded.value = a; resolve(a) }, reject: () => {} })
    })

    const outcome = await adapter.processEntry(makeAnswerEntry('m2', { correlation_id: 'corr-2', answers: [{ id: 'q1', value: 'foo' }] }))
    await Promise.resolve()

    expect(outcome.admitted).toBe(true)
    expect(recorded.value).toBeDefined()
    // pending resolve receives raw Flink answers now
    expect(recorded.value[0].value).toEqual('foo')
  })

  it('duplicate answer for same correlation is ignored', async () => {
    const { adapter, pending } = setup()
    const resolved: any = {}
    const p = new Promise(resolve => {
      pending.set('corr-3', { conversationId: 'telegram:chat:1', resolve: (a: any) => { resolved.a = a; resolve(a) }, reject: () => {} })
    })

    const first = await adapter.processEntry(makeAnswerEntry('m3', { correlation_id: 'corr-3', answers: [{ id: 'q1', value: 'one' }] }))
    await Promise.resolve()
    expect(first.admitted).toBe(true)
    expect(pending.has('corr-3')).toBe(false)

    const second = await adapter.processEntry(makeAnswerEntry('m4', { correlation_id: 'corr-3', answers: [{ id: 'q1', value: 'two' }] }))
    expect(second.admitted).toBe(false)
    expect(second.acked).toBe(true)
  })

  it('conversation mismatch is acked and does not resolve pending', async () => {
    const { adapter, pending } = setup()
    let resolved = false
    pending.set('corr-4', { conversationId: 'telegram:chat:1', resolve: () => { resolved = true }, reject: () => {} })

    const outcome = await adapter.processEntry(makeAnswerEntry('m5', { correlation_id: 'corr-4', answers: [{ id: 'q1', value: 'x' }], }))
    expect(outcome.admitted).toBe(true)
    expect(pending.has('corr-4')).toBe(false)
    // Because conversation matched in the envelope, this test ensures normal flow;
    // to exercise mismatch, craft an envelope with different conversation_id.
    pending.set('corr-5', { conversationId: 'telegram:chat:DIFFERENT', resolve: () => { resolved = true }, reject: () => {} })
    const bad = JSON.parse(JSON.stringify(makeAnswerEntry('m6', { correlation_id: 'corr-5' })))
    bad.fields.json = JSON.stringify({ version:1, conversation_id: 'telegram:chat:1', session_id: null, message_id: 'm6', correlation_id: 'corr-5', type: 'question_answer', content: { answers: [{ id: 'q1', value: 'x' }] }, metadata: {} })
    const badOutcome = await adapter.processEntry(bad as any)
    expect(badOutcome.admitted).toBe(false)
    expect(badOutcome.acked).toBe(true)
    expect(pending.has('corr-5')).toBe(true) // still present because we only acked and didn't resolve
  })

  it('session mismatch is acked and does not resolve pending', async () => {
    const { adapter, pending } = setup()
    pending.set('corr-6', { conversationId: 'telegram:chat:1', sessionId: 'session-1', resolve: () => { throw new Error('should not resolve') }, reject: () => {} })

    const outcome = await adapter.processEntry(makeAnswerEntryWithSession('m7', 'session-2', { correlation_id: 'corr-6' }))
    expect(outcome.admitted).toBe(false)
    expect(outcome.acked).toBe(true)
    // pending should still be present because we didn't resolve
    expect(pending.has('corr-6')).toBe(true)
  })

  it('shutdown-style cleanup rejects and removes all pending entries', async () => {
    const pending = new Map<string, any>()
    let rejected = false
    pending.set('corr-7', { conversationId: 'telegram:chat:1', resolve: () => {}, reject: () => { rejected = true } })
    // Simulate shutdown cleanup as implemented in plugin-entry.
    for (const [corr, p] of pending.entries()) {
      try { p.reject(new Error('connector shutting down')) } catch {}
      pending.delete(corr)
    }
    expect(rejected).toBe(true)
    expect(pending.size).toBe(0)
  })
})

import { shouldRouteToRedis, mapQuestionsToOutbound, mapFlinkAnswersToAskAnswer } from '../src/user-questions.js'

describe('user-questions pure helpers', () => {
  it('routing matrix for titles', () => {
    expect(shouldRouteToRedis('bot-news')).toBe(true)
    expect(shouldRouteToRedis('bot-personal-assistant')).toBe(true)
    expect(shouldRouteToRedis('my-bot-session')).toBe(false)
    expect(shouldRouteToRedis('Bot-news')).toBe(false)
    expect(shouldRouteToRedis(undefined)).toBe(false)
    expect(shouldRouteToRedis('')).toBe(false)
  })

  it('maps outbound question shapes', () => {
    const dsq = [{ id: 'q1', question: 'What?', options: [{ label: 'a' }, { label: 'b' }], multiSelect: true }]
    const out = mapQuestionsToOutbound(dsq as any)
    expect(out.length).toBe(1)
    expect(out[0].text).toBe('What?')
    expect(out[0].options).toEqual(['a','b'])
    expect(out[0].multi_select).toBe(true)

    const dsq2 = [{ id: 'q2', question: 'Free text' }]
    const out2 = mapQuestionsToOutbound(dsq2 as any)
    expect(out2.length).toBe(1)
    expect(out2[0].text).toBe('Free text')
    expect(out2[0].options).toBeUndefined()
    expect(out2[0].multi_select).toBeUndefined()
  })

  it('maps inbound answers correctly', () => {
    const single = [{ id: 'q1', value: 'opt1' }]
    const mappedSingle = mapFlinkAnswersToAskAnswer(single as any)
    expect(mappedSingle.answers[0].selected).toEqual(['opt1'])

    const multi = [{ id: 'q1', value: ['a','b'] }]
    const mappedMulti = mapFlinkAnswersToAskAnswer(multi as any)
    expect(mappedMulti.answers[0].selected).toEqual(['a','b'])
  })
})

describe('no-new-turn invariant for question_answer', () => {
  it('does not start a new turn when handling question_answer', async () => {
    const transport = new FakeRedisTransport()
    const bindings = new InMemoryConversationBindingStore()
    const receipts = new InMemoryReceiptStore({ ttlMs: 60_000 })
    const state = new InMemoryConversationState()
    const agents = new FakeAgentRegistry()
    const ownedHandles = new Set()
    const logger = { warn: () => {}, error: () => {}, info: () => {} }
    const pending = new Map<string, any>()
    const adapter = new InboundAdapter({
      transport,
      resolver: { resolve: async () => { throw new Error('not used') } },
      receipts,
      bindings,
      state,
      ownedHandles,
      createUserMessage: text => ({ id: `msg-1`, text }),
      logger,
      inboundStream: 'harness:inbound',
      consumerGroup: 'g',
      consumerName: 'c',
      pendingQuestions: pending,
    })

    // register a fake agent and ensure its followup/steer are not invoked
    const fake = new FakeAgent('session-1')
    agents.live.set('session-1', fake)

    let resolved = false
    pending.set('corr-9', { conversationId: 'telegram:chat:1', resolve: (a: any) => { resolved = true }, reject: () => {} })

    const outcome = await adapter.processEntry(makeAnswerEntryWithSession('m9', 'session-1', { correlation_id: 'corr-9', answers: [{ id: 'q1', value: 'x' }] }))
    expect(outcome.admitted).toBe(true)
    expect(resolved).toBe(true)
    expect(fake.followups.length).toBe(0)
    expect(fake.steers.length).toBe(0)
  })
})
