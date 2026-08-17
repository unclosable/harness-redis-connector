import { describe, expect, it } from 'vitest'
import type { SessionEvent } from '@deepseek-ai/dsh-session'
import { InMemoryConversationBindingStore } from '../src/binding-store/index.js'
import { OutboundProjector, OutboundSender } from '../src/outbound-projector/index.js'
import { InMemoryConversationState } from '../src/state/conversation-state.js'
import { FakeRedisTransport } from './helpers/fakes.js'

function assistantEvent(blocks: Array<{ type: string; text?: string }>): SessionEvent {
  return {
    type: 'assistant/message',
    seq: 3,
    time: 0,
    data: {
      turn: 1,
      step: 1,
      message: {
        id: 'a1',
        role: 'assistant',
        source: { kind: 'model', provider: 'p', model: 'm' },
        content: blocks,
      },
    },
  } as unknown as SessionEvent
}

function turnEndErrorEvent(message: string): SessionEvent {
  return {
    type: 'turn/end',
    seq: 4,
    time: 0,
    data: { turn: 1, reason: { kind: 'error', error: { message, code: 'E' } } },
  } as unknown as SessionEvent
}

function setup() {
  const transport = new FakeRedisTransport()
  const bindings = new InMemoryConversationBindingStore()
  const state = new InMemoryConversationState()
  const sender = new OutboundSender(transport, 'harness:outbound', 'json')
  const projector = new OutboundProjector({
    bindings,
    state,
    sender,
    logger: { warn: () => {}, error: () => {}, info: () => {} },
  })
  return { transport, bindings, state, projector }
}

function lastPayload(transport: FakeRedisTransport): Record<string, unknown> {
  const entry = transport.added[transport.added.length - 1]
  return JSON.parse(entry!.fields['json']!) as Record<string, unknown>
}

describe('outbound projector', () => {
  it('projects assistant/message to an outbound envelope with correlation and text', async () => {
    const { transport, bindings, state, projector } = setup()
    await bindings.bind('telegram:chat:1', 'session-1')
    state.recordInbound('telegram:chat:1', 'telegram-update-1001', { chatId: '1' })

    await projector.project('session-1', assistantEvent([{ type: 'text', text: 'hello' }]))

    expect(transport.added).toHaveLength(1)
    expect(lastPayload(transport)).toMatchObject({
      type: 'assistant_message',
      conversation_id: 'telegram:chat:1',
      session_id: 'session-1',
      correlation_id: 'telegram-update-1001',
    })
    expect(lastPayload(transport).content).toEqual({ text: 'hello' })
    expect(lastPayload(transport).metadata).toEqual({ chatId: '1' })
  })

  it('extracts text from text blocks only, ignoring reasoning blocks', async () => {
    const { transport, bindings, projector } = setup()
    await bindings.bind('telegram:chat:1', 'session-1')

    await projector.project('session-1', assistantEvent([
      { type: 'text', text: 'A' },
      { type: 'reasoning', text: 'hidden' },
      { type: 'text', text: 'B' },
    ]))

    expect(lastPayload(transport).content).toEqual({ text: 'AB' })
  })

  it('ignores sessions that are not bound', async () => {
    const { transport, projector } = setup()
    await projector.project('session-unbound', assistantEvent([{ type: 'text', text: 'hi' }]))
    expect(transport.added).toHaveLength(0)
  })

  it('projects turn/end with an error reason to an error envelope', async () => {
    const { transport, bindings, projector } = setup()
    await bindings.bind('telegram:chat:1', 'session-1')

    await projector.project('session-1', turnEndErrorEvent('boom'))

    const payload = lastPayload(transport)
    expect(payload.type).toBe('error')
    expect(payload.content).toEqual({ text: 'boom' })
  })

  it('projects agent/error to an error envelope', async () => {
    const { transport, bindings, projector } = setup()
    await bindings.bind('telegram:chat:1', 'session-1')

    await projector.handleAgentError('session-1', new Error('agent failed'))

    const payload = lastPayload(transport)
    expect(payload.type).toBe('error')
    expect(payload.content).toEqual({ text: 'agent failed' })
  })
})
