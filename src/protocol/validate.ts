import { INBOUND_TYPES, PROTOCOL_VERSION, type InboundEnvelope, type InboundType } from './types.js'

/**
 * Result of parsing one inbound JSON payload. The three variants map to the
 * `PROTOCOL.md` delivery semantics: `ok` admits the message, `ignored` drops an
 * unknown `type` (forward compatibility), and `error` is a malformed event that
 * is logged and, when routing is known, projected as an outbound `error`.
 */
export type InboundParseResult =
  | { kind: 'ok'; envelope: InboundEnvelope }
  | { kind: 'ignored'; reason: string }
  | { kind: 'error'; reason: string }

/**
 * Parse and validate one inbound JSON payload.
 * @param raw - the JSON envelope string read from the inbound stream.
 * @returns a discriminated parse result.
 */
export function parseInboundEnvelope(raw: string): InboundParseResult {
  let value: unknown
  try {
    value = JSON.parse(raw)
  } catch {
    return { kind: 'error', reason: 'payload is not valid JSON' }
  }
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return { kind: 'error', reason: 'envelope is not a JSON object' }
  }
  const record = value as Record<string, unknown>

  // Version is checked before `type`: a missing/unsupported version is malformed,
  // while an unknown `type` on a supported version is safe-ignored.
  if (record['version'] !== PROTOCOL_VERSION) {
    return { kind: 'error', reason: `unsupported version ${String(record['version'])}` }
  }

  const type = record['type']
  if (typeof type !== 'string' || type.length === 0) {
    return { kind: 'error', reason: `type must be a non-empty string (got ${String(type)})` }
  }
  if (!(INBOUND_TYPES as readonly string[]).includes(type)) {
    return { kind: 'ignored', reason: `unknown type ${type}` }
  }

  const conversation_id = record['conversation_id']
  const message_id = record['message_id']
  const session_id = record['session_id']
  const correlation_id = record['correlation_id']
  const content = record['content']
  const metadata = record['metadata']

  if (typeof conversation_id !== 'string' || conversation_id.length === 0) {
    return { kind: 'error', reason: 'conversation_id must be a non-empty string' }
  }
  if (typeof message_id !== 'string' || message_id.length === 0) {
    return { kind: 'error', reason: 'message_id must be a non-empty string' }
  }
  if (session_id !== null && typeof session_id !== 'string') {
    return { kind: 'error', reason: 'session_id must be a string or null' }
  }
  if (correlation_id !== undefined && correlation_id !== null && typeof correlation_id !== 'string') {
    return { kind: 'error', reason: 'correlation_id must be a string or null' }
  }

  // Type-specific content validation.
  if (type === 'question_answer') {
    if (typeof content !== 'object' || content === null || Array.isArray(content)) {
      return { kind: 'error', reason: 'content must be an object for question_answer' }
    }
    const answers = (content as Record<string, unknown>)['answers']
    if (!Array.isArray(answers) || answers.length === 0) {
      return { kind: 'error', reason: 'content.answers must be a non-empty array' }
    }
    for (const ans of answers) {
      if (typeof ans !== 'object' || ans === null || Array.isArray(ans)) return { kind: 'error', reason: 'each answer must be an object' }
      const id = (ans as Record<string, unknown>)['id']
      const value = (ans as Record<string, unknown>)['value']
      if (typeof id !== 'string' || id.length === 0) return { kind: 'error', reason: 'answer.id must be a non-empty string' }
      if (!(typeof value === 'string' || (Array.isArray(value) && value.every(v => typeof v === 'string')))) {
        return { kind: 'error', reason: 'answer.value must be a string or array of strings' }
      }
    }
  } else {
    if (
      typeof content !== 'object'
      || content === null
      || Array.isArray(content)
      || typeof (content as Record<string, unknown>)['text'] !== 'string'
    ) {
      return { kind: 'error', reason: 'content.text must be a string' }
    }
  }

  if (metadata !== undefined && (typeof metadata !== 'object' || metadata === null || Array.isArray(metadata))) {
    return { kind: 'error', reason: 'metadata must be an object' }
  }

  return {
    kind: 'ok',
    envelope: {
      version: PROTOCOL_VERSION,
      conversation_id,
      session_id: session_id === null ? null : (session_id as string),
      message_id,
      correlation_id: correlation_id === undefined ? undefined : (correlation_id === null ? null : String(correlation_id)),
      type: type as InboundType,
      content: type === 'question_answer'
        ? ({ answers: (content as Record<string, unknown>)['answers'] as { id: string; value: string | string[] }[] })
        : ({ text: (content as Record<string, unknown>)['text'] as string }),
      metadata: (metadata ?? {}) as Record<string, unknown>,
    },
  }
}
