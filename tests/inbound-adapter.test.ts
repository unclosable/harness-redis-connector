import { describe, expect, it } from 'vitest'
import { InMemoryConversationBindingStore } from '../src/binding-store/index.js'
import { InboundAdapter } from '../src/inbound-adapter/index.js'
import { InMemoryReceiptStore } from '../src/receipt-store/index.js'
import { InMemoryCommandResultStore } from '../src/command-result-store/index.js'
import { BindingConflictError, SessionResolver, type AgentHandleLike, type ResolvedAgent, type SessionResolverLike } from '../src/session-resolver/index.js'
import { InMemoryConversationState } from '../src/state/conversation-state.js'
import type { InboundContentMessage } from '../src/protocol/index.js'
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
  const resultStore = new InMemoryCommandResultStore()
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
    resultStore,
    bindings,
    state,
    ownedHandles,
    buildUserMessage: async (_agent, content) => ({ id: `msg-${next}`, text: content.text }),
    logger,
    inboundStream: 'harness:inbound',
    consumerGroup: 'g',
    consumerName: 'c',
    onError: details => { errors.push(details) },
    emitOutbound: envelope => transport.xadd('harness:outbound', { json: JSON.stringify(envelope) }),
    commandResultTtlSeconds: 60,
  })
  return { adapter, transport, agents, bindings, receipts, state, ownedHandles, errors }
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

  it('passes text and images to the async message builder once', async () => {
    const transport = new FakeRedisTransport()
    const bindings = new InMemoryConversationBindingStore()
    const receipts = new InMemoryReceiptStore({ ttlMs: 60_000 })
    const resultStore = new InMemoryCommandResultStore()
    const state = new InMemoryConversationState()
    const agents = new FakeAgentRegistry()
    const resolver = new SessionResolver({ agents, bindings, freshSessionId: () => 'session-1' })
    const seen: InboundContentMessage[] = []
    const adapter = new InboundAdapter({
      transport,
      resolver,
      receipts,
      resultStore,
      bindings,
      state,
      ownedHandles: new Set<AgentHandleLike>(),
      buildUserMessage: async (_agent, content) => {
        seen.push(content)
        return { id: 'msg-1', text: content.text }
      },
      logger: { warn: () => {}, error: () => {}, info: () => {} },
      inboundStream: 'harness:inbound',
      consumerGroup: 'g',
      consumerName: 'c',
      emitOutbound: async () => {},
      commandResultTtlSeconds: 60,
    })

    await adapter.processEntry(makeEntry('m-images', {
      content: {
        text: '',
        images: [{ media_type: 'image/png', data: 'iVBORw0KGgo=', name: 'sample.png' }],
      },
    }))

    expect(seen).toHaveLength(1)
    expect(seen[0]).toEqual({
      text: '',
      images: [{ media_type: 'image/png', data: 'iVBORw0KGgo=', name: 'sample.png' }],
    })
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

  it('renews a session with a metadata-derived bot title without driving an agent turn', async () => {
    const { adapter, transport, agents, bindings } = setup()
    await bindings.bind('telegram:chat:1', 'session-old')
    agents.live.set('session-old', new FakeAgent('session-old'))

    const outcome = await adapter.processEntry(makeEntry('renew-1', {
      type: 'system_command',
      content: { command: 'renew_session' },
      metadata: { chatId: '123', botId: 'operator' },
    }))

    expect(outcome.acked).toBe(true)
    expect(agents.created).toHaveLength(1)
    expect(agents.created[0]?.title).toBe('bot-operator')
    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-1')
    expect(agents.live.get('session-1')?.followups).toHaveLength(0)
    expect(agents.live.get('session-1')?.steers).toHaveLength(0)
    const result = JSON.parse(transport.added[0]!.fields.json) as Record<string, any>
    expect(result).toMatchObject({
      type: 'system_command_result', message_id: 'renew-1', session_id: 'session-1',
      content: { command: 'renew_session', status: 'ok', session_id: 'session-1', message: 'Session renewed.' },
    })
  })

  it('uses the bot id in conversation_id when renewal metadata has no bot id', async () => {
    const { adapter, agents } = setup()

    await adapter.processEntry(makeEntry('renew-conversation-bot', {
      conversation_id: 'telegram:operator:chat:123',
      type: 'system_command',
      content: { command: 'renew_session' },
      metadata: { chatId: '123' },
    }))

    expect(agents.created[0]?.title).toBe('bot-operator')
  })

  it('replays the same stored system-command result without a second renewal', async () => {
    const { adapter, transport, agents } = setup()
    await adapter.processEntry(makeEntry('renew-duplicate', {
      type: 'system_command', content: { command: 'renew_session' },
    }))
    await adapter.processEntry(makeEntry('renew-duplicate', {
      type: 'system_command', content: { command: 'renew_session' },
    }))

    expect(agents.created).toHaveLength(1)
    expect(transport.added).toHaveLength(2)
    expect(transport.added[1]!.fields.json).toBe(transport.added[0]!.fields.json)
  })

  it('emits a confirmation error when a system-command receipt was claimed without a stored result', async () => {
    const { adapter, transport, agents, receipts } = setup()
    await receipts.claim('renew-claimed')

    await adapter.processEntry(makeEntry('renew-claimed', {
      type: 'system_command', content: { command: 'renew_session' },
    }))

    expect(agents.created).toHaveLength(0)
    const result = JSON.parse(transport.added[0]!.fields.json) as Record<string, any>
    expect(result).toMatchObject({
      type: 'system_command_result',
      content: { status: 'error', message: 'Session renewal could not be confirmed.' },
    })
  })

  it('keeps the prior binding and emits an error when renewal creation fails', async () => {
    const { adapter, transport, agents, bindings } = setup()
    await bindings.bind('telegram:chat:1', 'session-old')
    agents.create = async () => { throw new Error('create failed') }

    await adapter.processEntry(makeEntry('renew-failure', {
      type: 'system_command', content: { command: 'renew_session' },
    }))

    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-old')
    const result = JSON.parse(transport.added[0]!.fields.json) as Record<string, any>
    expect(result).toMatchObject({ session_id: 'session-old', type: 'system_command_result', content: { status: 'error', message: 'Session renewal failed.' } })
  })

  it('rejects an unrecognized system command without creating or driving an agent', async () => {
    const { adapter, transport, agents } = setup()
    await adapter.processEntry(makeEntry('unknown-command', {
      type: 'system_command', content: { command: 'future_command' },
    }))

    expect(agents.created).toHaveLength(0)
    expect(transport.added).toHaveLength(1)
    const result = JSON.parse(transport.added[0]!.fields.json) as Record<string, any>
    expect(result.content).toMatchObject({ command: 'future_command', status: 'error', message: 'Unsupported system command.' })
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
      renew: async () => { throw new Error('not used') },
    }
    const adapter = new InboundAdapter({
      transport,
      resolver,
      receipts: new InMemoryReceiptStore({ ttlMs: 60_000 }),
      resultStore: new InMemoryCommandResultStore(),
      bindings: new InMemoryConversationBindingStore(),
      state: new InMemoryConversationState(),
      ownedHandles: new Set(),
      buildUserMessage: async () => ({ id: 'msg-1' }),
      logger: { warn: () => {}, error: () => {}, info: () => {} },
      inboundStream: 'harness:inbound',
      consumerGroup: 'g',
      consumerName: 'c',
      emitOutbound: async () => {},
      commandResultTtlSeconds: 60,
    })

    await adapter.processEntry(makeEntry('m1'))

    expect(order).toEqual(['followup', 'ack'])
  })

  it('does not ACK a transient admission failure (leaves it pending for retry)', async () => {
    const transport = new FakeRedisTransport()
    const resolver: SessionResolverLike = {
      resolve: async () => { throw new Error('temporarily unavailable') },
      renew: async () => { throw new Error('not used') },
    }
    const adapter = new InboundAdapter({
      transport,
      resolver,
      receipts: new InMemoryReceiptStore({ ttlMs: 60_000 }),
      resultStore: new InMemoryCommandResultStore(),
      bindings: new InMemoryConversationBindingStore(),
      state: new InMemoryConversationState(),
      ownedHandles: new Set(),
      buildUserMessage: async () => ({ id: 'msg-1' }),
      logger: { warn: () => {}, error: () => {}, info: () => {} },
      inboundStream: 'harness:inbound',
      consumerGroup: 'g',
      consumerName: 'c',
      emitOutbound: async () => {},
      commandResultTtlSeconds: 60,
    })

    const outcome = await adapter.processEntry(makeEntry('m1'))

    expect(outcome.admitted).toBe(false)
    expect(outcome.acked).toBe(false)
    expect(transport.acked).toHaveLength(0)
  })

  it('ACKs and skips followup when the message builder rejects the inbound payload', async () => {
    const transport = new FakeRedisTransport()
    const agents = new FakeAgentRegistry()
    const blocking = new InboundAdapter({
      transport,
      resolver: new SessionResolver({
        agents,
        bindings: new InMemoryConversationBindingStore(),
        freshSessionId: () => 'session-1',
      }),
      receipts: new InMemoryReceiptStore({ ttlMs: 60_000 }),
      resultStore: new InMemoryCommandResultStore(),
      bindings: new InMemoryConversationBindingStore(),
      state: new InMemoryConversationState(),
      ownedHandles: new Set(),
      buildUserMessage: async () => undefined,
      logger: { warn: () => {}, error: () => {}, info: () => {} },
      inboundStream: 'harness:inbound',
      consumerGroup: 'g',
      consumerName: 'c',
      emitOutbound: async () => {},
      commandResultTtlSeconds: 60,
    })
    const outcome = await blocking.processEntry(makeEntry('m-block'))
    expect(outcome.acked).toBe(true)
    expect(outcome.admitted).toBe(false)
    expect(transport.acked).toHaveLength(1)
    expect(agents.live.get('session-1')?.followups ?? []).toHaveLength(0)
  })

  it('ACKs a binding conflict (permanent hard-reject)', async () => {
    const transport = new FakeRedisTransport()
    const resolver: SessionResolverLike = {
      resolve: async () => { throw new BindingConflictError('session_id conflict') },
      renew: async () => { throw new Error('not used') },
    }
    const adapter = new InboundAdapter({
      transport,
      resolver,
      receipts: new InMemoryReceiptStore({ ttlMs: 60_000 }),
      resultStore: new InMemoryCommandResultStore(),
      bindings: new InMemoryConversationBindingStore(),
      state: new InMemoryConversationState(),
      ownedHandles: new Set(),
      buildUserMessage: async () => ({ id: 'msg-1' }),
      logger: { warn: () => {}, error: () => {}, info: () => {} },
      inboundStream: 'harness:inbound',
      consumerGroup: 'g',
      consumerName: 'c',
      emitOutbound: async () => {},
      commandResultTtlSeconds: 60,
    })

    const outcome = await adapter.processEntry(makeEntry('m1'))

    expect(outcome.admitted).toBe(false)
    expect(outcome.acked).toBe(true)
    expect(transport.acked).toHaveLength(1)
  })
})
