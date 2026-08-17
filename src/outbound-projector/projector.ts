import type { ContentBlock } from '@deepseek-ai/dsh-llm'
import type { SessionEvent } from '@deepseek-ai/dsh-session'
import type { ConversationBindingStore } from '../binding-store/types.js'
import type { ConversationState } from '../state/conversation-state.js'
import { describeError, type LoggerLike } from '../util.js'
import { buildAssistantEnvelope, buildErrorEnvelope } from './envelopes.js'
import type { OutboundSender } from './sender.js'

/**
 * Extract visible text from assistant content blocks: `text` blocks only,
 * ignoring `reasoning` and every other block type.
 * @param blocks - the assistant message content.
 * @returns the joined visible text.
 */
export function assistantText(blocks: readonly ContentBlock[]): string {
  return blocks
    .filter((block): block is Extract<ContentBlock, { type: 'text' }> => block.type === 'text')
    .map(block => block.text)
    .join('')
}

export interface OutboundProjectorOptions {
  readonly bindings: ConversationBindingStore
  readonly state: ConversationState
  readonly sender: OutboundSender
  readonly logger: LoggerLike
}

/**
 * Projects Harness session events onto the outbound stream. Only sessions
 * bound via `session_id → conversation_id` are projected; `assistant/message`
 * becomes an `assistant_message` envelope, and `turn/end` with an error reason
 * becomes an `error` envelope.
 */
export class OutboundProjector {
  private readonly bindings: ConversationBindingStore
  private readonly state: ConversationState
  private readonly sender: OutboundSender
  private readonly logger: LoggerLike

  constructor(options: OutboundProjectorOptions) {
    this.bindings = options.bindings
    this.state = options.state
    this.sender = options.sender
    this.logger = options.logger
  }

  /**
   * Project one durable `session/event`.
   * @param sessionId - the session that produced the event.
   * @param event - the appended session event.
   */
  async project(sessionId: string, event: SessionEvent): Promise<void> {
    const conversationId = await this.bindings.conversationIdFor(sessionId)
    if (conversationId === undefined) return
    switch (event.type) {
      case 'assistant/message': {
        const text = assistantText(event.data.message.content)
        if (text.length === 0) return
        await this.safeSend(buildAssistantEnvelope({
          conversationId,
          sessionId,
          correlationId: this.state.correlationFor(conversationId),
          text,
          metadata: this.state.metadataFor(conversationId),
        }))
        return
      }
      case 'turn/end': {
        if (event.data.reason.kind === 'error') {
          await this.safeSend(buildErrorEnvelope({
            conversationId,
            sessionId,
            correlationId: this.state.correlationFor(conversationId),
            text: event.data.reason.error.message,
            metadata: this.state.metadataFor(conversationId),
          }))
        }
        return
      }
      default:
        // Merge-extensible session event vocabulary: non-projected types fall through.
        return
    }
  }

  /**
   * Project a live `agent/error` notification for a bound session.
   * @param sessionId - the agent's session id.
   * @param error - the failure, verbatim.
   */
  async handleAgentError(sessionId: string, error: unknown): Promise<void> {
    const conversationId = await this.bindings.conversationIdFor(sessionId)
    if (conversationId === undefined) return
    await this.safeSend(buildErrorEnvelope({
      conversationId,
      sessionId,
      correlationId: this.state.correlationFor(conversationId),
      text: describeError(error),
      metadata: this.state.metadataFor(conversationId),
    }))
  }

  private async safeSend(envelope: Parameters<OutboundSender['send']>[0]): Promise<void> {
    try {
      await this.sender.send(envelope)
    } catch (error: unknown) {
      this.logger.warn(`outbound projection failed: ${describeError(error)}`)
    }
  }
}
