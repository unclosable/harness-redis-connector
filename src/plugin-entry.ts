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
import { installModelSelection, type Agent, type ModelSelectionRef } from '@deepseek-ai/dsh-agent'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import { randomUUID } from 'crypto'
import { SessionId, type Session, type SessionEvent } from '@deepseek-ai/dsh-session'
import type { AskUserQuestionRequest, AskUserQuestionAnswer } from '@deepseek-ai/dsh-user-questions'

import type { ApprovalRequest, ApprovalOutcome } from '@deepseek-ai/dsh-user-approval'
import type {} from '@deepseek-ai/dsh-user-approval'   // Context augmentation: ctx.approval + 'approval/request' event
import Schema from '@deepseek-ai/schemastery'
import type {} from '@deepseek-ai/dsh-session-persistence'
import { RedisConversationBindingStore } from './binding-store/index.js'
import { InboundAdapter, type ErrorEmitter } from './inbound-adapter/index.js'
import { OutboundProjector, OutboundSender, buildErrorEnvelope } from './outbound-projector/index.js'
import { RedisReceiptStore } from './receipt-store/index.js'
import { RedisCommandResultStore } from './command-result-store/index.js'
import { IoredisRedisTransport, createRedisClient } from './redis-transport/index.js'
import { SessionResolver, type AgentHandleLike, type AgentRegistryLike } from './session-resolver/index.js'
import { InMemoryConversationState } from './state/conversation-state.js'
import { describeError } from './util.js'
import type { OutboundEnvelope } from './protocol/types.js'
import { shouldRouteToRedis, mapQuestionsToOutbound, mapFlinkAnswersToAskAnswer } from './user-questions.js'

export const name = 'harness-redis-connector'

/** The connector creates and drives agents; everything else is carried by the agent composition. */
export const inject = ['agents', 'userQuestions', 'sessionTitle', 'approval']

const DEFAULT_INBOUND_STREAM = 'harness:inbound'
const DEFAULT_OUTBOUND_STREAM = 'harness:outbound'
const DEFAULT_CONSUMER_GROUP = 'harness-redis-connector'
const DEFAULT_CONSUMER_NAME = 'harness-redis-connector'
const DEFAULT_BATCH_SIZE = 10
const DEFAULT_BLOCK_MS = 5000
const DEFAULT_RECEIPT_TTL_MS = 86_400_000
const DEFAULT_BINDING_KEY_PREFIX = 'harness-redis-connector:binding'
const DEFAULT_RECEIPT_KEY_PREFIX = 'harness-redis-connector:receipt'
const DEFAULT_COMMAND_RESULT_KEY_PREFIX = 'harness-redis-connector:command-result'

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
  /** Redis key prefix for durable system-command result records. */
  commandResultKeyPrefix?: string
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
  commandResultKeyPrefix: Schema.string().default(DEFAULT_COMMAND_RESULT_KEY_PREFIX),
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

/** Public structural view of the optional agent-preset service. */
interface AgentPresetsLike {
  resolve?(id?: string): Promise<{ id: string }>
  mount(agentCtx: Context, id?: string): Promise<unknown>
}

/** Public structural view of the optional workspace registry service. */
interface WorkspaceRegistryLike {
  resolveByPath(path: string): Promise<{
    attachSession(sessionId: SessionId): Promise<void>
  } | undefined>
}

/** The latest durable preset selection wins over the creation header. */
function selectedAgentPreset(session: Session): string | undefined {
  const events = session.snapshotEvents()
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index] as { type?: string; data?: { agentPreset?: unknown } } | undefined
    if (event?.type === 'agent-preset/selected' && typeof event.data?.agentPreset === 'string') {
      return event.data.agentPreset
    }
  }
  return session.header.agentPreset
}

/** The runtime settings needed to recreate or resume an existing session. */
interface StoredSessionConfig {
  provider?: string
  model?: string
  maxTokens?: number
  cwd?: string
  agentPreset?: string
}

/** Narrow unknown event payloads without depending on private session internals. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
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
  const commandResultTtlSeconds = Math.max(1, Math.floor((config.receiptTtlMs ?? DEFAULT_RECEIPT_TTL_MS) / 1000))
  const resultStore = new RedisCommandResultStore(
    client,
    config.commandResultKeyPrefix ?? DEFAULT_COMMAND_RESULT_KEY_PREFIX,
  )
  const state = new InMemoryConversationState()
  const ownedHandles = new Set<AgentHandleLike>()
  const sessionTitleSvc = ctx.get('sessionTitle') as unknown as {
    get(session: Session): { title?: string } | undefined
    rename(session: Session, title: string): unknown
  } | undefined

  /**
   * Read a session's recreation settings from the live registries when present,
   * otherwise fold its durable snapshot. A persistence failure intentionally
   * leaves callers with their ordinary connector-config fallback.
   */
  const readSessionConfig = async (sourceId: SessionId): Promise<StoredSessionConfig> => {
    const sourceSession = ctx.agents.get(sourceId)?.session
      ?? (ctx.get('sessions') as unknown as { get(id: SessionId): Session | undefined } | undefined)?.get(sourceId)
    if (sourceSession !== undefined) {
      const sourceConfig = sourceSession.requestHeader()?.config
      const agentPreset = selectedAgentPreset(sourceSession)
      return {
        ...(sourceConfig?.provider !== undefined ? { provider: sourceConfig.provider } : {}),
        ...(sourceConfig?.model !== undefined ? { model: sourceConfig.model } : {}),
        ...(sourceConfig?.maxTokens !== undefined ? { maxTokens: sourceConfig.maxTokens } : {}),
        ...(sourceSession.header.cwd !== undefined ? { cwd: sourceSession.header.cwd } : {}),
        ...(agentPreset !== undefined ? { agentPreset } : {}),
      }
    }

    try {
      const persistence = ctx.get('sessionPersistence')
      if (persistence === undefined) return {}
      const { meta, events } = await persistence.load(sourceId)
      let requestHeaderFound = false
      let agentPresetFound = false
      let provider: string | undefined
      let model: string | undefined
      let maxTokens: number | undefined
      let agentPreset: string | undefined = meta.agentPreset

      for (let index = events.length - 1; index >= 0; index -= 1) {
        const event: SessionEvent = events[index]!
        // The runtime can contain newer public event variants than this
        // connector's compiled SessionEvent union, so inspect their payloads
        // defensively without importing DSH's private event internals.
        const persistedEvent = event as unknown as { type?: unknown; data?: unknown }
        if (!requestHeaderFound && event.type === 'request/header') {
          requestHeaderFound = true
          const data = isRecord(persistedEvent.data) ? persistedEvent.data : undefined
          const header = data !== undefined && isRecord(data['header']) ? data['header'] : undefined
          const requestConfig = header !== undefined && isRecord(header['config']) ? header['config'] : undefined
          provider = typeof requestConfig?.['provider'] === 'string' ? requestConfig['provider'] : undefined
          model = typeof requestConfig?.['model'] === 'string' ? requestConfig['model'] : undefined
          maxTokens = typeof requestConfig?.['maxTokens'] === 'number' ? requestConfig['maxTokens'] : undefined
        }
        if (!agentPresetFound && persistedEvent.type === 'agent-preset/selected') {
          agentPresetFound = true
          const data = isRecord(persistedEvent.data) ? persistedEvent.data : undefined
          if (typeof data?.['agentPreset'] === 'string') agentPreset = data['agentPreset']
        }
        if (requestHeaderFound && agentPresetFound) break
      }

      return {
        ...(provider !== undefined ? { provider } : {}),
        ...(model !== undefined ? { model } : {}),
        ...(maxTokens !== undefined ? { maxTokens } : {}),
        ...(meta.cwd !== undefined ? { cwd: meta.cwd } : {}),
        ...(agentPreset !== undefined ? { agentPreset } : {}),
      }
    } catch (error: unknown) {
      logger.warn(`could not load persisted configuration for session ${sourceId}: ${describeError(error)}`)
      return {}
    }
  }

  /** Resolve and mount a persisted preset, preserving its selected model route. */
  const presetSetup = async (
    sourceId: SessionId,
    agentPreset: string | undefined,
    provider: string | undefined,
    model: string | undefined,
  ): Promise<{ resolvedPreset?: string; setup?: (agentCtx: Context) => Promise<void> }> => {
    const presets = ctx.get('agentPresets') as unknown as AgentPresetsLike | undefined
    if (agentPreset === undefined || presets === undefined) {
      logger.warn(`session ${sourceId} cannot inherit its agent preset; continuing without preset composition`)
      return {}
    }
    const resolvedPreset = presets.resolve === undefined ? agentPreset : (await presets.resolve(agentPreset)).id
    const modelSelection: ModelSelectionRef | undefined = provider !== undefined && model !== undefined
      ? { current: { provider, model }, assembled: undefined }
      : undefined
    return {
      resolvedPreset,
      setup: async (agentCtx: Context) => {
        // ApiProxy's installSelection is private. This is its public equivalent.
        if (modelSelection !== undefined) installModelSelection(agentCtx, modelSelection)
        await presets.mount(agentCtx, resolvedPreset)
      },
    }
  }

  const agentsPort: AgentRegistryLike = {
    get: id => ctx.agents.get(SessionId(id)),
    create: async options => {
      let handle: Awaited<ReturnType<typeof ctx.agents.create>>
      if (options.sourceSessionId === undefined) {
        handle = await ctx.agents.create({ sessionId: SessionId(options.sessionId), agentOptions: agentOptions(config), meta: { cwd } })
      } else {
        const sourceId = SessionId(options.sourceSessionId)
        const sourceConfig = await readSessionConfig(sourceId)
        const fallback = agentOptions(config)
        const provider = sourceConfig?.provider ?? fallback.provider
        const model = sourceConfig?.model ?? fallback.model
        const maxTokens = sourceConfig?.maxTokens
        const inheritedOptions = {
          ...(provider !== undefined ? { provider } : {}),
          ...(model !== undefined ? { model } : {}),
          ...(maxTokens !== undefined ? { maxTokens } : {}),
        }
        const inheritedCwd = sourceConfig.cwd ?? cwd
        const { resolvedPreset, setup } = await presetSetup(sourceId, sourceConfig.agentPreset, provider, model)

        handle = await ctx.agents.create({
          sessionId: SessionId(options.sessionId),
          agentOptions: inheritedOptions,
          meta: {
            cwd: inheritedCwd,
            ...(resolvedPreset !== undefined ? { agentPreset: resolvedPreset } : {}),
          },
          ...(setup !== undefined ? { setup } : {}),
        })
      }
      const sessionCwd = handle.agent.session.header.cwd
      const workspaceRegistry = ctx.get('workspaceRegistry') as unknown as WorkspaceRegistryLike | undefined
      if (workspaceRegistry !== undefined && sessionCwd !== undefined) {
        try {
          const workspace = await workspaceRegistry.resolveByPath(sessionCwd)
          if (workspace !== undefined) await workspace.attachSession(SessionId(options.sessionId))
        } catch (error: unknown) {
          logger.warn(`workspace session attachment failed: ${describeError(error)}`)
        }
      }
      if (typeof options.title === 'string' && options.title.trim() !== '' && sessionTitleSvc !== undefined) {
        try {
          sessionTitleSvc.rename(handle.agent.session, options.title)
        } catch (error: unknown) {
          logger.warn(`session title rename failed: ${describeError(error)}`)
        }
      }
      return handle
    },
    resume: async options => {
      const resumeSessionId = SessionId(options.resumeSessionId)
      const sourceConfig = await readSessionConfig(resumeSessionId)
      const fallback = agentOptions(config)
      const provider = sourceConfig.provider ?? fallback.provider
      const model = sourceConfig.model ?? fallback.model
      const maxTokens = sourceConfig.maxTokens
      const restoredOptions = {
        ...(provider !== undefined ? { provider } : {}),
        ...(model !== undefined ? { model } : {}),
        ...(maxTokens !== undefined ? { maxTokens } : {}),
      }
      const { setup } = await presetSetup(resumeSessionId, sourceConfig.agentPreset, provider, model)
      return ctx.agents.resume({
        resumeSessionId,
        agentOptions: restoredOptions,
        ...(setup !== undefined ? { setup } : {}),
      })
    },
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
    // kind discriminator: 'question' | 'approval'
    kind?: 'question' | 'approval'
    // Resolver receives raw Flink answers: { id, value, custom? }[]; per-kind mapping done by resolver closure.
    resolve: (answersRaw: { id: string; value: string | string[]; custom?: string }[]) => void
    reject: (err: unknown) => void
  }>()

  const adapter = new InboundAdapter({
    transport,
    resolver,
    receipts,
    resultStore,
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
    emitOutbound: envelope => sender.send(envelope),
    commandResultTtlSeconds,
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

  // Route interactive questions for sessions whose sessionTitle starts with
  // "bot-" to Redis via the outbound stream. The native agent-scoped
  // 'user-questions/request' waterfall (dsh ≥ 0.1.2) replaced the old
  // single-provider registerRouter seam: an untagged listener is admitted for
  // every agent, and next() (which takes no argument) delegates to the Web
  // answerer.
  try {
    if (!sessionTitleSvc) {
      logger.error('required service sessionTitle not available')
      throw new Error('missing required services')
    }

    const disposer = ctx.on(
      'user-questions/request',
      async (request: AskUserQuestionRequest, next: () => Promise<AskUserQuestionAnswer>) => {
        // If no agent provided, or no session on the agent, fall through.
        const agent = request.agent as unknown as Agent | undefined
        if (agent == null) return next()
        const session = agent.session

        const title = sessionTitleSvc.get(session)?.title
        if (!shouldRouteToRedis(title)) return next()

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

        // Build envelope payload using the pure mapper.
        const correlationId = randomUUID()
        const questions = mapQuestionsToOutbound(request.questions)
        const envelope: OutboundEnvelope = {
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
        const pendingPromise = new Promise<AskUserQuestionAnswer>((resolve, reject) => {
          const onAbort = () => {
            if (!settled && pendingQuestions.delete(correlationId)) {
              settled = true
              reject(new Error('ask aborted'))
            }
          }
          // Insert pending before sending so the inbound adapter can resolve it
          // if an answer races in immediately after send returns. The abort
          // listener is attached to remove pending if the request is aborted.
          // The pending resolve expects raw Flink answers and maps them to the
          // AskUserQuestionAnswer shape using the existing mapper.
          const resolver = (answersRaw: { id: string; value: string | string[]; custom?: string }[]) => {
            try {
              const mapped = mapFlinkAnswersToAskAnswer(answersRaw)
              resolve(mapped)
            } catch (e) {
              reject(e)
            }
          }
          pendingQuestions.set(correlationId, { conversationId, sessionId, kind: 'question', resolve: resolver, reject })
          request.signal?.addEventListener('abort', onAbort, { once: true })
        })

        try {
          await sender.send(envelope)
        } catch (err) {
          // Remove pending and propagate error.
          if (pendingQuestions.delete(correlationId)) {
            settled = true
          }
          throw err
        }

        try {
          const answer = await pendingPromise
          return answer
        } finally {
          // Ensure no leaked pending in any case.
          pendingQuestions.delete(correlationId)
        }
      },
    )

    // Dispose the listener on plugin shutdown. This cordis fork has no public
    // 'dispose' event (its Events interface declares only internal/* and dsh
    // augmentations) — cleanup is effect-based on the owning fiber, so use
    // ctx.effect (the same idiom as the old disposer wiring and the main
    // dispose effect below).
    ctx.effect(() => disposer, 'harness-redis-connector.user-questions-dispose')
  } catch (err) {
    logger.warn(`user-questions integration not registered: ${String(err)}`)
  }

  // Register an approval handler that wins the waterfall for bot-* sessions.
  try {
    if (ctx.approval === undefined || !sessionTitleSvc) {
      logger.warn('approval service or sessionTitle not available; skipping approval routing')
    } else {
      ctx.on('approval/request', async (req: ApprovalRequest, next: () => Promise<ApprovalOutcome>) => {
        // If no agent or session, fall through to Web.
        const agent = req.agent ?? undefined
        if (!agent) return next()
        const session = agent.session

        const titleSnapshot = sessionTitleSvc.get(session)
        const title = titleSnapshot?.title
        if (!shouldRouteToRedis(title)) return next()

        // Handle abort early.
        if (req.signal?.aborted) return 'cancelled'

        const sessionId = String(session.id)
        const conversationId = await bindings.conversationIdFor(sessionId)
        if (conversationId === undefined) {
          logger.warn(`approval: no conversation bound to session ${sessionId}`)
          return 'unavailable'
        }

        const correlationId = randomUUID()
        const text = `Approve \`${req.toolName}\`?` + (req.reason ? ` — ${req.reason}` : '')
        const envelope: OutboundEnvelope = {
          version: 1,
          conversation_id: conversationId,
          session_id: sessionId,
          correlation_id: correlationId,
          type: 'question_request',
          content: { questions: [ { id: 'approval', text, options: ['Approve', 'Deny'] } ] },
          metadata: state.metadataFor(conversationId),
        }

        let settled = false
        const pendingPromise = new Promise<ApprovalOutcome>((resolve, reject) => {
          const onAbort = () => {
            if (!settled && pendingQuestions.delete(correlationId)) {
              settled = true
              resolve('cancelled')
            }
          }
          const resolver = (answersRaw: { id: string; value: string | string[]; custom?: string }[]) => {
            // Map raw Flink answers to ApprovalOutcome.
            try {
              const approvalAns = answersRaw.find(a => a.id === 'approval')
              const val = approvalAns === undefined ? undefined : (Array.isArray(approvalAns.value) ? approvalAns.value[0] : approvalAns.value)
              if (val === 'Approve') resolve('allowed-once')
              else if (val === 'Deny') resolve('rejected')
              else resolve('unavailable')
            } catch (e) {
              reject(e)
            }
          }
          pendingQuestions.set(correlationId, { conversationId, sessionId, kind: 'approval', resolve: resolver, reject })
          req.signal?.addEventListener('abort', onAbort, { once: true })
        })

        try {
          await sender.send(envelope)
        } catch (err) {
          if (pendingQuestions.delete(correlationId)) settled = true
          return 'unavailable'
        }

        try {
          const outcome = await pendingPromise
          return outcome
        } finally {
          pendingQuestions.delete(correlationId)
        }
      }, { prepend: true })
    }
  } catch (err) {
    logger.warn(`approval routing not registered: ${String(err)}`)
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
