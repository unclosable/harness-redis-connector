import { PROTOCOL_VERSION, type OutboundEnvelope } from '../protocol/index.js'

/** Inputs for building an `assistant_message` envelope. */
export interface AssistantEnvelopeArgs {
  conversationId: string
  sessionId: string
  correlationId: string | undefined
  text: string
  metadata: Record<string, unknown>
}

/** Inputs for building an `error` envelope. */
export interface ErrorEnvelopeArgs {
  conversationId: string
  sessionId?: string
  correlationId?: string
  text: string
  metadata: Record<string, unknown>
}

/** Build an outbound `assistant_message` envelope. */
export function buildAssistantEnvelope(args: AssistantEnvelopeArgs): OutboundEnvelope {
  return {
    version: PROTOCOL_VERSION,
    conversation_id: args.conversationId,
    session_id: args.sessionId,
    correlation_id: args.correlationId ?? null,
    type: 'assistant_message',
    contentType: 'text/markdown',
    content: args.text,
    metadata: args.metadata,
  }
}

/** Build an outbound `error` envelope. */
export function buildErrorEnvelope(args: ErrorEnvelopeArgs): OutboundEnvelope {
  return {
    version: PROTOCOL_VERSION,
    conversation_id: args.conversationId,
    session_id: args.sessionId ?? null,
    correlation_id: args.correlationId ?? null,
    type: 'error',
    content: { text: args.text },
    metadata: args.metadata,
  }
}
