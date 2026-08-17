import { describe, expect, it } from 'vitest'
import { parseInboundEnvelope, serializeOutboundEnvelope, type OutboundEnvelope } from '../src/protocol/index.js'

function inbound(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({
    version: 1,
    conversation_id: 'telegram:chat:12345',
    session_id: null,
    message_id: 'telegram-update-1001',
    type: 'message',
    content: { text: '帮我检查一下这个项目' },
    metadata: { chatId: '12345' },
    ...overrides,
  })
}

describe('protocol validation', () => {
  it('parses a valid envelope', () => {
    const result = parseInboundEnvelope(inbound())
    expect(result.kind).toBe('ok')
    if (result.kind === 'ok') {
      expect(result.envelope.version).toBe(1)
      expect(result.envelope.conversation_id).toBe('telegram:chat:12345')
      expect(result.envelope.message_id).toBe('telegram-update-1001')
      expect(result.envelope.type).toBe('message')
      expect(result.envelope.session_id).toBeNull()
      expect(result.envelope.content.text).toBe('帮我检查一下这个项目')
      expect(result.envelope.metadata).toEqual({ chatId: '12345' })
    }
  })

  it('rejects a missing version', () => {
    const result = parseInboundEnvelope(inbound({ version: undefined }))
    expect(result.kind).toBe('error')
  })

  it('rejects an unsupported version', () => {
    expect(parseInboundEnvelope(inbound({ version: 2 })).kind).toBe('error')
  })

  it('rejects malformed JSON', () => {
    expect(parseInboundEnvelope('{not json').kind).toBe('error')
  })

  it('rejects a missing conversation_id', () => {
    expect(parseInboundEnvelope(inbound({ conversation_id: '' })).kind).toBe('error')
  })

  it('rejects a non-string content.text', () => {
    expect(parseInboundEnvelope(inbound({ content: { text: 42 } })).kind).toBe('error')
  })

  it('safe-ignores an unknown type (forward compatibility)', () => {
    const result = parseInboundEnvelope(inbound({ type: 'future_type' }))
    expect(result.kind).toBe('ignored')
  })

  it('rejects a missing or blank type as malformed', () => {
    expect(parseInboundEnvelope(inbound({ type: undefined })).kind).toBe('error')
    expect(parseInboundEnvelope(inbound({ type: '' })).kind).toBe('error')
    expect(parseInboundEnvelope(inbound({ type: 42 })).kind).toBe('error')
  })

  it('serializes an outbound envelope with snake_case envelope fields', () => {
    const envelope: OutboundEnvelope = {
      version: 1,
      conversation_id: 'telegram:chat:12345',
      session_id: 'harness-session-id',
      correlation_id: 'telegram-update-1001',
      type: 'assistant_message',
      content: { text: 'done' },
      metadata: { chatId: '12345' },
    }
    const parsed = JSON.parse(serializeOutboundEnvelope(envelope)) as Record<string, unknown>
    expect(parsed).toMatchObject({
      version: 1,
      conversation_id: 'telegram:chat:12345',
      session_id: 'harness-session-id',
      correlation_id: 'telegram-update-1001',
      type: 'assistant_message',
      content: { text: 'done' },
    })
  })

  it('serializes an error envelope with null session_id/correlation_id', () => {
    const envelope: OutboundEnvelope = {
      version: 1,
      conversation_id: 'telegram:chat:12345',
      session_id: null,
      correlation_id: null,
      type: 'error',
      content: { text: 'boom' },
      metadata: {},
    }
    const parsed = JSON.parse(serializeOutboundEnvelope(envelope)) as Record<string, unknown>
    expect(parsed['session_id']).toBeNull()
    expect(parsed['correlation_id']).toBeNull()
  })
})
