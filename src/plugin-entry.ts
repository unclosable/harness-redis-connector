/**
 * The DeepSeek Harness function-plugin entry point for the Redis Streams
 * connector. Wires config → Redis client + stores + resolver + adapter +
 * projector, starts the consumer loop, and tears everything down on dispose.
 *
 * It never calls an LLM, never implements an agent loop, and imports no concrete
 * loop classes — it drives only the public `ctx.agents` registry and the
 * durable `session/event` firehose.
 *
 * @module harness-redis-connector/plugin-entry
 */

import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import { randomUUID } from 'crypto'
import { SessionId, type Session, type SessionEvent } from '@deepseek-ai/dsh-session'
import type { UserQuestionRouter, AskUserQuestionRequest, AskUserQuestionAnswer } from '@deepseek-ai/dsh-user-questions'
import Schema from '@deepseek-ai/schemastery'
import type {} from '@deepseek-ai/dsh-session-persistence'
import { RedisConversationBindingStore } from './binding-store/index.js'
import { InboundAdapter, type ErrorEmitter } from './inbound-adapter/index.js'
import { OutboundProjector, OutboundSender, buildErrorEnvelope } from './outbound-projector/index.js'
import { RedisReceiptStore } from './receipt-store/index.js'
import { IoredisRedisTransport, createRedisClient } from './redis-transport/index.js'
import { SessionResolver, type AgentHandleLike, type AgentRegistryLike } from './session-resolver/index.js'
import { InMemoryConversationState } from './state/conversation-state.js'
import { describeError } from './util.js'

export const name = 'harness-redis-connector'

/** The connector creates and drives agents; everything else is carried by the agent composition. */
export const inject = ['agents']

const DEFAULT_INBOUND_STREAM = 'harness:inbound'
const DEFAULT_OUTBOUND_STREAM = 'harness:outbound'
const DEFAULT_CONSUMER_GROUP = 'harness-redis-connector'
const DEFAULT_CONSUMER_NAME = 'harness-redis-connector'
const DEFAULT_BATCH_SIZE = 10
const DEFAULT_BLOCK_MS = 5000
const DEFAULT_RECEIPT_TTL_MS = 86_400_000
const DEFAULT_BINDING_KEY_PREFIX = 'harness-redis-connector:binding'
const DEFAULT_RECEIPT_KEY_PREFIX = 'harness-redis-connector:receipt'

/** Plugin config: Redis connection, stream/consumer names, and per-agent options. */
export interface Config {
  /** Redis connection URL; falls back to `REDIS_URL` then ioredis' localhost default. */
  redisUrl?: string
  /** Inbound stream name (Flink → Harness). */
  inboundStream?: string
  /** Outbound stream name (Harness → Flink). */
  outboundStream?: string
  /** Consumer group on the inbound stream. */
  consumerGroup?: string
  /** Consumer name within the group (unique per process for scaling). */
  consumerName?: string
  /** Maximum entries per XREADGROUP batch. */
  batchSize?: number
  /** XREADGROUP block time in milliseconds. */
  blockMs?: number
  /** Idempotency receipt TTL in milliseconds. */
  receiptTtlMs?: number
  /** Redis key prefix for the binding hashes. */
  bindingKeyPrefix?: string
  /** Redis key prefix for receipt keys. */
  receiptKeyPrefix?: string
  /** Provider route for created/resumed agents. */
  provider?: string
  /** Model name for created/resumed agents. */
  model?: string
  /** Working directory for newly created agents; defaults to `process.cwd()`. */
  cwd?: string
}

export const Config: Schema<Config> = Schema.object({
  redisUrl: Schema.string(),
  inboundStream: Schema.string().default(DEFAULT_INBOUND_STREAM),
  outboundStream: Schema.string().default(DEFAULT_OUTBOUND_STREAM),
  consumerGroup: Schema.string().default(DEFAULT_CONSUMER_GROUP),
  consumerName: Schema.string().default(DEFAULT_CONSUMER_NAME),
  batchSize: Schema.number().default(DEFAULT_BATCH_SIZE),
  blockMs: Schema.number().default(DEFAULT_BLOCK_MS),
  receiptTtlMs: Schema.number().default(DEFAULT_RECEIPT_TTL_MS),
  bindingKeyPrefix: Schema.string().default(DEFAULT_BINDING_KEY_PREFIX),
  receiptKeyPrefix: Schema.string().default(DEFAULT_RECEIPT_KEY_PREFIX),
  provider: Schema.string(),
  model: Schema.string(),
  cwd: Schema.string(),
})

/** Per-agent options from plugin config, without assigning absent optional fields. */
function agentOptions(config: Config): { provider?: string; model?: string } {
  return {
    ...config.provider !== undefined ? { provider: config.provider } : {},
    ...config.model !== undefined ? { model: config.model } : {},
  }
}

/**
 * Mount the Redis Streams connector.
 * @param ctx - Cordis context carrying the agent registry and session events.
 * @param config - Redis connection/stream configuration.
 */
export function apply(ctx: Context, config: Config): void {
  const logger = ctx.logger('harness-redis-connector')
  const cwd = config.cwd ?? process.cwd()

  const redisUrl = config.redisUrl ?? process.env['REDIS_URL']
  const client = createRedisClient(redisUrl)
  const transport = new IoredisRedisTransport(client)
  const bindings = new RedisConversationBindingStore(client, config.bindingKeyPrefix ?? DEFAULT_BINDING_KEY_PREFIX)
  const receipts = new RedisReceiptStore(
    client,
    config.receiptKeyPrefix ?? DEFAULT_RECEIPT_KEY_PREFIX,
    Math.max(1, Math.floor((config.receiptTtlMs ?? DEFAULT_RECEIPT_TTL_MS) / 1000)),
  )
  const state = new InMemoryConversationState()
  const ownedHandles = new Set<AgentHandleLike>()

  const agentsPort: AgentRegistryLike = {
    get: id => ctx.agents.get(SessionId(id)),
    create: options => ctx.agents.create({ sessionId: SessionId(options.sessionId), agentOptions: agentOptions(config), meta: { cwd } }),
    resume: options => ctx.agents.resume({ resumeSessionId: SessionId(options.resumeSessionId), agentOptions: agentOptions(config) }),
  }

  const resolver = new SessionResolver({
    agents: agentsPort,
    bindings,
    persistence: ctx.get('sessionPersistence') ?? undefined,
  })

  const outboundStream = config.outboundStream ?? DEFAULT_OUTBOUND_STREAM
  const sender = new OutboundSender(transport, outboundStream)
  const projector = new OutboundProjector({ bindings, state, sender, logger })

  const emitError: ErrorEmitter = details => {
    if (details.conversationId === undefined) return
    return sender.send(buildErrorEnvelope({
      conversationId: details.conversationId,
      correlationId: details.messageId,
      text: details.text,
      metadata: state.metadataFor(details.conversationId),
    }))
  }

  // Pending question map shared between the inbound adapter and the
  // UserQuestion router registered below. Keyed by correlation_id.
  const pendingQuestions = new Map<string, {
    conversationId: string
    sessionId?: string | null
    resolve: (answer: { answers: { id: string; selected: string[]; custom?: string }[] }) => void
    reject: (err: unknown) => void
  }>()

  const adapter = new InboundAdapter({
    transport,
    resolver,
    receipts,
    bindings,
    state,
    ownedHandles,
    createUserMessage: text => createUserMessage({ content: [{ type: 'text', text }], source: { kind: 'user' } }),
    logger,
    inboundStream: config.inboundStream ?? DEFAULT_INBOUND_STREAM,
    consumerGroup: config.consumerGroup ?? DEFAULT_CONSUMER_GROUP,
    consumerName: config.consumerName ?? DEFAULT_CONSUMER_NAME,
    batchSize: config.batchSize ?? DEFAULT_BATCH_SIZE,
    blockMs: config.blockMs ?? DEFAULT_BLOCK_MS,
    onError: emitError,
    pendingQuestions,
  })

  // Project durable assistant text and turn errors for bound sessions only.
  const disposeSessionEventListener = ctx.root.on('session/event', (session: Session, event: SessionEvent) => {
    void projector.project(session.id, event).catch((error: unknown) => {
      logger.warn(`outbound projection failed: ${describeError(error)}`)
    })
  })

  // Project live agent errors that may have no in-turn position.
  const disposeAgentErrorListener = ctx.root.on('agent/error', ({ agent, error }: { agent: Agent; turn: number; step: number; error: unknown }) => {
    void projector.handleAgentError(agent.session.id, error).catch((err: unknown) => {
      logger.warn(`agent error projection failed: ${describeError(err)}`)
    })
  })

  // Register a UserQuestion router that routes asks for sessions whose
  // sessionTitle starts with "bot-" to Redis via the outbound stream.
  try {
    // Obtain typed Cordis services without using `any` casts.
    const userQuestions = ctx.get('userQuestions') as { registerRouter: (router: UserQuestionRouter) => () => void } | undefined
    const sessionTitleSvc = ctx.get('sessionTitle') as { get(session: Session): { title?: string } | undefined } | undefined

    const disposer = userQuestions?.registerRouter({
      ask: async (request: AskUserQuestionRequest, next: (req: AskUserQuestionRequest) => Promise<AskUserQuestionAnswer>) => {
        // If no agent provided, or no session on the agent, fall through.
        const agent = request.agent
        if (agent === undefined) return next(request)
        const session = (agent as unknown as { session?: Session }).session
        if (session === undefined) return next(request)

        const titleSnapshot = sessionTitleSvc?.get(session)
        const title = titleSnapshot?.title
        if (title === undefined || !title.startsWith('bot-')) return next(request)

        // Route to Redis: require a conversation binding for this session.
        const sessionId = String(session.id)
        // Race/abort safety: ensure caller hasn't already aborted.
        if (request.signal?.aborted) throw new Error('ask aborted')

        const conversationId = await bindings.conversationIdFor(sessionId)
        if (request.signal?.aborted) throw new Error('ask aborted')

        if (conversationId === undefined) {
          const err = new Error(`no conversation bound to session ${sessionId}`)
          logger.warn(`ask(): ${String(err)}`)
          throw err
        }

        // Build envelope payload.
        const correlationId = randomUUID()
        const questions = request.questions.map(q => ({ id: q.id, text: q.question, ...(q.options ? { options: q.options.map(o => o.label) } : {}), ...(q.multiSelect ? { multi_select: true } : {}) }))
        const envelope = {
          version: 1,
          conversation_id: conversationId,
          session_id: sessionId,
          correlation_id: correlationId,
          type: 'question_request',
          content: { questions },
          metadata: state.metadataFor(conversationId),
        }

        // Create pending entry and attach abort handling. The pending entry must
        // exist before awaiting responses from the adapter; however, if the
        // outbound send fails, the pending entry must be removed to avoid leaks.
        let settled = false
        const pendingPromise = new Promise<{ answers: { id: string; selected: string[]; custom?: string }[] }>((resolve, reject) => {
          const onAbort = () => {
            if (!settled && pendingQuestions.delete(correlationId)) {
              settled = true
              reject(new Error('ask aborted'))
            }
          }
          // Insert pending before sending so the inbound adapter can resolve it
          // if an answer races in immediately after send returns. The abort
          // listener is attached to remove pending if the request is aborted.
          pendingQuestions.set(correlationId, { conversationId, sessionId, resolve, reject })
          request.signal?.addEventListener('abort', onAbort, { once: true })
        })

        try {
          await sender.send(envelope as any)
        } catch (err) {
          // Remove pending and propagate error.
          if (pendingQuestions.delete(correlationId)) {
            settled = true
          }
          throw err
        }

        try {
          const answer = await pendingPromise
          return { answers: answer.answers }
        } finally {
          // Ensure no leaked pending in any case.
          pendingQuestions.delete(correlationId)
        }
      },
    })

    // Ensure router is disposed on plugin shutdown and that pending asks are rejected.
    ctx.effect(() => async () => {
      disposer?.()
      // Reject and remove all pending questions on shutdown.
      for (const [corr, p] of pendingQuestions.entries()) {
        try { p.reject(new Error('connector shutting down')) } catch {}
        pendingQuestions.delete(corr)
      }
    }, 'harness-redis-connector.user-questions-dispose')
  } catch (err) {
    logger.warn(`user-questions integration not registered: ${String(err)}`)
  }

  const controller = new AbortController()
  const loop = adapter.run(controller.signal).catch((error: unknown) => {
    logger.error(`consumer loop stopped: ${describeError(error)}`)
  })

  ctx.effect(() => async () => {
    controller.abort()
    disposeSessionEventListener()
    disposeAgentErrorListener()
    // Unblock the consumer read, then wait for the loop to settle so no handle
    // can be added after the disposal snapshot below.
    await transport.close()
    await loop
    const disposals = await Promise.allSettled([...ownedHandles].map(handle => handle.dispose()))
    ownedHandles.clear()
    for (const result of disposals) {
      if (result.status === 'rejected') logger.warn(`owned agent disposal failed: ${describeError(result.reason)}`)
    }
  }, 'harness-redis-connector.dispose')
}
