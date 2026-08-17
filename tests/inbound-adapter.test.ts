import { describe, expect, it } from 'vitest'
import { InMemoryConversationBindingStore } from '../src/binding-store/index.js'
import { InboundAdapter } from '../src/inbound-adapter/index.js'
import { InMemoryReceiptStore } from '../src/receipt-store/index.js'
import { BindingConflictError, SessionResolver, type AgentHandleLike, type ResolvedAgent, type SessionResolverLike } from '../src/session-resolver/index.js'
import { InMemoryConversationState } from '../src/state/conversation-state.js'
import type { StreamEntry } from '../src/redis-transport/types.js'
import { FakeAgent, FakeAgentRegistry, FakeRedisTransport } from './helpers/fakes.js'

function makeEntry(messageId: string, overrides: Record<string, unknown> = {}): StreamEntry {
  return {
    id: `1-${messageId}`,
    fields: {
      json: JSON.stringify({
        version: 1,
        conversation_id: 'telegram:chat:1',
        session_id: null,
        message_id: messageId,
        type: 'message',
        content: { text: 'hello' },
        metadata: { chatId: '1' },
        ...overrides,
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
  const resolver = new SessionResolver({ agents, bindings, freshSessionId: () => `session-${++next}` })
  const ownedHandles = new Set<AgentHandleLike>()
  const logger = { warn: () => {}, error: () => {}, info: () => {} }
  const errors: Array<{ conversationId?: string; messageId?: string; text: string }> = []
  const adapter = new InboundAdapter({
    transport,
    resolver,
    receipts,
    bindings,
    state,
    ownedHandles,
    createUserMessage: text => ({ id: `msg-${next}`, text }),
    logger,
    inboundStream: 'harness:inbound',
    consumerGroup: 'g',
    consumerName: 'c',
    onError: details => { errors.push(details) },
  })
  return { adapter, transport, agents, bindings, state, ownedHandles, errors }
}

describe('inbound adapter', () => {
  it('routes message → followup and ACKs after admission', async () => {
    const { adapter, transport, agents } = setup()
    const outcome = await adapter.processEntry(makeEntry('m1'))

    expect(outcome.admitted).toBe(true)
    const agent = agents.live.get('session-1')
    expect(agent?.followups).toHaveLength(1)
    expect((agent?.followups[0] as { text: string }).text).toBe('hello')
    expect(transport.acked).toHaveLength(1)
    expect(transport.acked[0]?.ids).toContain('1-m1')
  })

  it('reads the json field, falling back to the first field value when absent', async () => {
    const { adapter, agents } = setup()
    const envelope = {
      version: 1,
      conversation_id: 'telegram:chat:1',
      session_id: null,
      message_id: 'm-fallback',
      type: 'message',
      content: { text: 'fallback' },
      metadata: {},
    }
    const entry: StreamEntry = { id: '1-fallback', fields: { otherfield: JSON.stringify(envelope) } }

    await adapter.processEntry(entry)

    expect(agents.live.get('session-1')?.followups).toHaveLength(1)
    expect((agents.live.get('session-1')?.followups[0] as { text: string }).text).toBe('fallback')
  })

  it('routes steer → steer', async () => {
    const { adapter, agents } = setup()
    await adapter.processEntry(makeEntry('m1', { type: 'steer' }))

    const agent = agents.live.get('session-1')
    expect(agent?.steers).toHaveLength(1)
    expect(agent?.followups).toHaveLength(0)
  })

  it('new_session rebinds to a fresh session and does not dispose the prior one', async () => {
    const { adapter, agents, bindings } = setup()
    await adapter.processEntry(makeEntry('m1'))
    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-1')

    await adapter.processEntry(makeEntry('m2', { type: 'new_session' }))

    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-2')
    expect(agents.live.has('session-1')).toBe(true)
  })

  it('processes a duplicate message_id only once', async () => {
    const { adapter, agents } = setup()
    const first = await adapter.processEntry(makeEntry('m1'))
    const second = await adapter.processEntry(makeEntry('m1'))

    expect(first.admitted).toBe(true)
    expect(second.duplicate).toBe(true)
    expect(second.admitted).toBe(false)
    expect(agents.live.get('session-1')?.followups).toHaveLength(1)
  })

  it('logs and ACKs malformed messages (no infinite retry)', async () => {
    const { adapter, transport, errors } = setup()
    const outcome = await adapter.processEntry({ id: '1-bad', fields: { json: '{not json' } })

    expect(outcome.admitted).toBe(false)
    expect(transport.acked.map(a => a.ids[0])).toContain('1-bad')
    expect(errors.length).toBe(0) // no recoverable routing information
  })

  it('ACKs an unknown type (forward-compatible drop)', async () => {
    const { adapter, transport } = setup()
    const outcome = await adapter.processEntry(makeEntry('m1', { type: 'future_type' }))

    expect(outcome.admitted).toBe(false)
    expect(transport.acked.map(a => a.ids[0])).toContain('1-m1')
  })

  it('followup precedes ack (ACK happens only after admission)', async () => {
    const order: string[] = []
    const transport = new FakeRedisTransport()
    transport.onAck = () => { order.push('ack') }
    const agent = new FakeAgent('s1')
    agent.followup = (message: unknown) => { order.push('followup'); agent.followups.push(message) }
    const resolver: SessionResolverLike = {
      resolve: async (): Promise<ResolvedAgent> => ({ agent, owned: false }),
    }
    const adapter = new InboundAdapter({
      transport,
      resolver,
      receipts: new InMemoryReceiptStore({ ttlMs: 60_000 }),
      bindings: new InMemoryConversationBindingStore(),
      state: new InMemoryConversationState(),
      ownedHandles: new Set(),
      createUserMessage: () => ({ id: 'msg-1' }),
      logger: { warn: () => {}, error: () => {}, info: () => {} },
      inboundStream: 'harness:inbound',
      consumerGroup: 'g',
      consumerName: 'c',
    })

    await adapter.processEntry(makeEntry('m1'))

    expect(order).toEqual(['followup', 'ack'])
  })

  it('does not ACK a transient admission failure (leaves it pending for retry)', async () => {
    const transport = new FakeRedisTransport()
    const resolver: SessionResolverLike = {
      resolve: async () => { throw new Error('temporarily unavailable') },
    }
    const adapter = new InboundAdapter({
      transport,
      resolver,
      receipts: new InMemoryReceiptStore({ ttlMs: 60_000 }),
      bindings: new InMemoryConversationBindingStore(),
      state: new InMemoryConversationState(),
      ownedHandles: new Set(),
      createUserMessage: () => ({ id: 'msg-1' }),
      logger: { warn: () => {}, error: () => {}, info: () => {} },
      inboundStream: 'harness:inbound',
      consumerGroup: 'g',
      consumerName: 'c',
    })

    const outcome = await adapter.processEntry(makeEntry('m1'))

    expect(outcome.admitted).toBe(false)
    expect(outcome.acked).toBe(false)
    expect(transport.acked).toHaveLength(0)
  })

  it('ACKs a binding conflict (permanent hard-reject)', async () => {
    const transport = new FakeRedisTransport()
    const resolver: SessionResolverLike = {
      resolve: async () => { throw new BindingConflictError('session_id conflict') },
    }
    const adapter = new InboundAdapter({
      transport,
      resolver,
      receipts: new InMemoryReceiptStore({ ttlMs: 60_000 }),
      bindings: new InMemoryConversationBindingStore(),
      state: new InMemoryConversationState(),
      ownedHandles: new Set(),
      createUserMessage: () => ({ id: 'msg-1' }),
      logger: { warn: () => {}, error: () => {}, info: () => {} },
      inboundStream: 'harness:inbound',
      consumerGroup: 'g',
      consumerName: 'c',
    })

    const outcome = await adapter.processEntry(makeEntry('m1'))

    expect(outcome.admitted).toBe(false)
    expect(outcome.acked).toBe(true)
    expect(transport.acked).toHaveLength(1)
  })
})
