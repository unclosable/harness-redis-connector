import { parseInboundEnvelope, type InboundEnvelope } from '../protocol/index.js'
import type { ConversationBindingStore } from '../binding-store/types.js'
import type { InboundReceiptStore } from '../receipt-store/types.js'
import type { RedisTransport, StreamEntry } from '../redis-transport/types.js'
import { BindingConflictError, type AgentHandleLike, type SessionResolverLike } from '../session-resolver/index.js'
import type { ConversationState } from '../state/conversation-state.js'
import { describeError, sleep, type LoggerLike } from '../util.js'

/** Redis Stream entry field carrying the envelope JSON (fixed by PROTOCOL.md). */
const PAYLOAD_FIELD = 'json'
/** Cadence for reclaiming stale pending entries (at-least-once retry). */
const RECLAIM_INTERVAL_MS = 30_000

/** Minimal agent surface the adapter drives: `followup` and `steer`. */
export interface DriveAgentLike {
  readonly id: string
  followup(message: UserMessageLike): void
  steer(message: UserMessageLike): void
}

/** A user message the adapter builds (via an injected factory) and delivers. */
export interface UserMessageLike {
  readonly id: string
}

/** Per-message processing outcome. */
export interface ProcessOutcome {
  /** Whether the entry was acknowledged. Transient admission failures stay pending for retry. */
  acked: boolean
  /** Whether the message was safely admitted to Harness. */
  admitted: boolean
  /** Whether the message was a duplicate `message_id` already processed. */
  duplicate: boolean
  /** Human-readable reason when not admitted. */
  reason?: string
}

/** Error-emission callback wired to the outbound sender in `plugin-entry`. */
export interface ErrorEmitter {
  (details: { conversationId?: string; messageId?: string; text: string }): void | Promise<void>
}

export interface InboundAdapterOptions {
  readonly transport: RedisTransport
  readonly resolver: SessionResolverLike
  readonly receipts: InboundReceiptStore
  readonly bindings: ConversationBindingStore
  readonly state: ConversationState
  readonly ownedHandles: Set<AgentHandleLike>
  readonly createUserMessage: (text: string) => UserMessageLike
  readonly logger: LoggerLike
  readonly inboundStream: string
  readonly consumerGroup: string
  readonly consumerName: string
  readonly batchSize?: number
  readonly blockMs?: number
  /** Optional outbound error projection for malformed/conflicting messages. */
  readonly onError?: ErrorEmitter
  /** Optional pending question map managed by the plugin to resolve ask() calls. */
  readonly pendingQuestions?: Map<string, {
    conversationId: string
    sessionId?: string | null
    resolve: (answer: { answers: { id: string; selected: string[]; custom?: string }[] }) => void
    reject: (err: unknown) => void
  }>
}

/**
 * Consumes the inbound stream: parse/validate → idempotency check → session
 * resolution → `followup`/`steer`/`new_session` → XACK. ACKs happen after safe
 * admission (or after a logged, permanently-invalid message is dropped), never
 * after the agent finishes responding.
 */
export class InboundAdapter {
  private readonly transport: RedisTransport
  private readonly resolver: SessionResolverLike
  private readonly receipts: InboundReceiptStore
  private readonly state: ConversationState
  private readonly ownedHandles: Set<AgentHandleLike>
  private readonly createUserMessage: (text: string) => UserMessageLike
  private readonly logger: LoggerLike
  private readonly inboundStream: string
  private readonly consumerGroup: string
  private readonly consumerName: string
  private readonly batchSize: number
  private readonly blockMs: number
  private readonly onError: ErrorEmitter | undefined
  private readonly pendingQuestions: Map<string, {
    conversationId: string
    sessionId?: string | null
    resolve: (answer: { answers: { id: string; selected: string[]; custom?: string }[] }) => void
    reject: (err: unknown) => void
  }>

  constructor(options: InboundAdapterOptions) {
    this.transport = options.transport
    this.resolver = options.resolver
    this.receipts = options.receipts
    this.state = options.state
    this.ownedHandles = options.ownedHandles
    this.createUserMessage = options.createUserMessage
    this.logger = options.logger
    this.inboundStream = options.inboundStream
    this.consumerGroup = options.consumerGroup
    this.consumerName = options.consumerName
    this.batchSize = options.batchSize ?? 10
    this.blockMs = options.blockMs ?? 5000
    this.onError = options.onError
    this.pendingQuestions = options.pendingQuestions ?? new Map()
  }

  /**
   * Consume until `signal` aborts: ensure the group, recover pending entries,
   * then block on XREADGROUP and process each batch.
   * @param signal - shutdown signal.
   */
  async run(signal: AbortSignal): Promise<void> {
    await this.ensureGroup()
    await this.recoverPending()
    let lastReclaim = 0
    while (!signal.aborted) {
      // Reclaim stale pending entries (transient failures, dead consumers) on a
      // fixed cadence so they are retried without waiting for a restart.
      const now = Date.now()
      if (now - lastReclaim >= RECLAIM_INTERVAL_MS) {
        lastReclaim = now
        await this.recoverPending()
      }
      let entries: StreamEntry[]
      try {
        entries = await this.transport.xreadGroup(this.consumerGroup, this.consumerName, this.inboundStream, {
          count: this.batchSize,
          blockMs: this.blockMs,
          id: '>',
        })
      } catch (error: unknown) {
        this.logger.error(`xreadgroup failed: ${describeError(error)}`)
        await sleep(Math.min(this.blockMs, 1000), signal)
        continue
      }
      for (const entry of entries) {
        try {
          await this.processEntry(entry)
        } catch (error: unknown) {
          // Transient transport/store failure (e.g. Redis down): leave the entry
          // pending; the periodic reclaim pass retries it once Redis recovers.
          this.logger.error(`unexpected failure for entry ${entry.id}: ${describeError(error)}`)
        }
      }
    }
  }

  /**
   * Process one raw entry. The core, unit-tested admission boundary.
   * @param entry - the stream entry to process.
   */
  async processEntry(entry: StreamEntry): Promise<ProcessOutcome> {
    const payload = extractPayload(entry.fields)
    if (payload === undefined) {
      this.logger.warn(`inbound ${entry.id}: missing payload field "${PAYLOAD_FIELD}"`)
      await this.safeAck(entry.id)
      return { acked: true, admitted: false, duplicate: false, reason: 'missing payload field' }
    }

    const parsed = parseInboundEnvelope(payload)
    if (parsed.kind === 'error') {
      const ids = tryEnvelopeIds(payload)
      this.logger.warn(`inbound ${entry.id}: malformed — ${parsed.reason}`)
      await this.emitError({ conversationId: ids.conversationId, messageId: ids.messageId, text: `malformed inbound message: ${parsed.reason}` })
      await this.safeAck(entry.id)
      return { acked: true, admitted: false, duplicate: false, reason: parsed.reason }
    }
    if (parsed.kind === 'ignored') {
      this.logger.warn(`inbound ${entry.id}: ${parsed.reason} — dropped`)
      await this.safeAck(entry.id)
      return { acked: true, admitted: false, duplicate: false, reason: 'unknown type' }
    }
    return this.admit(entry, parsed.envelope)
  }

  private async admit(entry: StreamEntry, envelope: InboundEnvelope): Promise<ProcessOutcome> {
    const claimed = await this.receipts.claim(envelope.message_id)
    if (!claimed) {
      await this.safeAck(entry.id)
      return { acked: true, admitted: false, duplicate: true }
    }
    try {
      // Special-case: question_answer answers do not start agent turns. Handle
      // them here, before session resolution, by resolving the pending ask().
      if (envelope.type === 'question_answer') {
        const corr = (envelope as any).correlation_id
        if (corr === undefined || corr === null || typeof corr !== 'string') {
          this.logger.warn(`inbound ${entry.id}: question_answer missing correlation_id`)
          await this.emitError({ conversationId: envelope.conversation_id, messageId: envelope.message_id, text: 'malformed question_answer: missing correlation_id' })
          await this.safeAck(entry.id)
          return { acked: true, admitted: false, duplicate: false, reason: 'missing correlation_id' }
        }
        const pending = this.pendingQuestions.get(corr)
        if (pending === undefined) {
          this.logger.warn(`inbound ${entry.id}: question_answer for unknown correlation ${corr} — dropping`)
          await this.safeAck(entry.id)
          return { acked: true, admitted: false, duplicate: false, reason: 'unknown correlation_id' }
        }
        // Validate conversation/session identity matches the pending record.
        if (pending.conversationId !== envelope.conversation_id) {
          this.logger.warn(`inbound ${entry.id}: conversation mismatch for correlation ${corr} (expected ${pending.conversationId} got ${envelope.conversation_id})`)
          // Treat as stale / wrong-target: ack & drop without resolving the pending promise.
          await this.safeAck(entry.id)
          return { acked: true, admitted: false, duplicate: false, reason: 'conversation mismatch' }
        }
        // If the envelope contains a session_id, ensure it matches the pending sessionId
        // (when the pending record had a sessionId). A mismatch indicates a stale or
        // mis-routed answer and should be acked & dropped without resolving.
        const envSession = (envelope as any).session_id
        if (envSession !== undefined && envSession !== null && pending.sessionId != null && String(envSession) !== String(pending.sessionId)) {
          this.logger.warn(`inbound ${entry.id}: session mismatch for correlation ${corr} (expected ${String(pending.sessionId)} got ${String(envSession)})`)
          await this.safeAck(entry.id)
          return { acked: true, admitted: false, duplicate: false, reason: 'session mismatch' }
        }

        // Map answers into AskUserQuestionAnswer shape.
        const content = envelope.content as any
        const answersRaw = content.answers as { id: string; value: string | string[] }[]
        const mapped = answersRaw.map(a => ({ id: a.id, selected: Array.isArray(a.value) ? [...a.value] : [a.value], ...(typeof (a as any).custom === 'string' ? { custom: (a as any).custom } : {}) }))
        // Remove pending atomically and resolve.
        this.pendingQuestions.delete(corr)
        try {
          pending.resolve({ answers: mapped })
        } catch (err) {
          this.logger.warn(`resolving pending question ${corr} threw: ${String(err)}`)
        }
        await this.safeAck(entry.id)
        return { acked: true, admitted: true, duplicate: false }
      }

      const fresh = envelope.type === 'new_session'
      const resolved = await this.resolver.resolve(envelope.conversation_id, envelope.session_id, fresh)
      const agent = resolved.agent as DriveAgentLike
      if (resolved.owned && resolved.handle !== undefined) this.ownedHandles.add(resolved.handle)
      const message = this.createUserMessage((envelope.content as any).text)
      if (envelope.type === 'steer') agent.steer(message)
      else agent.followup(message)
      this.state.recordInbound(envelope.conversation_id, envelope.message_id, envelope.metadata)
      await this.safeAck(entry.id)
      return { acked: true, admitted: true, duplicate: false }
    } catch (error: unknown) {
      if (error instanceof BindingConflictError) {
        // Permanent hard-reject: never silently overwrite a binding. ACK so it is
        // dropped rather than retried forever.
        this.logger.warn(`inbound ${entry.id}: ${describeError(error)}`)
        await this.emitError({
          conversationId: envelope.conversation_id,
          messageId: envelope.message_id,
          text: `inbound message rejected: ${describeError(error)}`,
        })
        await this.safeAck(entry.id)
        return { acked: true, admitted: false, duplicate: false, reason: describeError(error) }
      }
      // Transient failure (resolve/create/resume/followup/steer): leave the entry
      // pending so it is retried on the next reclaim pass.
      this.logger.error(`inbound ${entry.id}: failed to admit — ${describeError(error)}`)
      await this.emitError({
        conversationId: envelope.conversation_id,
        messageId: envelope.message_id,
        text: `failed to admit inbound message: ${describeError(error)}`,
      })
      return { acked: false, admitted: false, duplicate: false, reason: describeError(error) }
    }
  }

  private async ensureGroup(): Promise<void> {
    await this.transport.xgroupCreate(this.inboundStream, this.consumerGroup, '0')
  }

  private async recoverPending(): Promise<void> {
    try {
      const summary = await this.transport.xpending(this.inboundStream, this.consumerGroup)
      if (summary.count === 0) return
      let startId = '0-0'
      for (let pass = 0; pass < 100; pass++) {
        const { nextStartId, entries } = await this.transport.xautoclaim(
          this.inboundStream, this.consumerGroup, this.consumerName, 60_000, startId, this.batchSize,
        )
        for (const entry of entries) await this.processEntry(entry)
        if (nextStartId === startId || entries.length === 0) break
        startId = nextStartId
      }
    } catch (error: unknown) {
      this.logger.warn(`pending recovery failed: ${describeError(error)}`)
    }
  }

  private async safeAck(id: string): Promise<void> {
    try {
      await this.transport.xack(this.inboundStream, this.consumerGroup, [id])
    } catch (error: unknown) {
      this.logger.warn(`ack failed for ${id}: ${describeError(error)}`)
    }
  }

  private async emitError(details: { conversationId?: string; messageId?: string; text: string }): Promise<void> {
    if (details.conversationId === undefined || this.onError === undefined) return
    try {
      await this.onError(details)
    } catch (error: unknown) {
      this.logger.warn(`error emission failed: ${describeError(error)}`)
    }
  }
}

/**
 * Read the envelope JSON from an entry: prefer the canonical `json` field
 * (matching the Flink producer), falling back to the first field value — the
 * same reader convention the Flink consumer uses.
 * @param fields - the entry's field/value map.
 * @returns the envelope JSON string, or undefined when the entry has no fields.
 */
function extractPayload(fields: Record<string, string>): string | undefined {
  const preferred = fields[PAYLOAD_FIELD]
  if (preferred !== undefined) return preferred
  return Object.values(fields)[0]
}

/** Best-effort extraction of `conversation_id`/`message_id` from a malformed payload for error routing. */
function tryEnvelopeIds(raw: string): { conversationId?: string; messageId?: string } {
  try {
    const value: unknown = JSON.parse(raw)
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      const record = value as Record<string, unknown>
      const conversationId = record['conversation_id']
      const messageId = record['message_id']
      return {
        ...typeof conversationId === 'string' && conversationId.length > 0 ? { conversationId } : {},
        ...typeof messageId === 'string' && messageId.length > 0 ? { messageId } : {},
      }
    }
  } catch {
    // Not JSON: no routing information is recoverable.
  }
  return {}
}
