import { randomUUID } from 'node:crypto'
import type { ConversationBindingStore } from '../binding-store/types.js'

/**
 * Session resolution: maps a `conversation_id` (plus an optional explicit
 * `session_id`) to a live agent, creating or resuming one when necessary, and
 * keeps the conversation↔session binding current. It never drives the agent and
 * never disposes the prior session on rebind.
 *
 * The ports below are minimal structural views of the harness `AgentRegistry`,
 * `Agent`, `AgentHandle`, and `SessionPersistence` services so this module (and
 * its tests) need no harness runtime import; `plugin-entry.ts` adapts the real
 * services to them.
 *
 * @module harness-redis-connector/session-resolver
 */

/** Minimal live-agent view needed by resolution. */
export interface AgentLike {
  readonly id: string
}

/** Minimal owned handle: the agent plus its disposer. */
export interface AgentHandleLike {
  readonly agent: AgentLike
  dispose(): Promise<void>
}

/** Create-options port; the resolver supplies a session id and optional title. */
export interface CreateAgentOptionsLike {
  readonly sessionId: string
  readonly title?: string
  /** Existing session whose runtime configuration a renewal should inherit. */
  readonly sourceSessionId?: string
}

/** Resume-options port; the resolver supplies only the resumed session id. */
export interface ResumeAgentOptionsLike {
  readonly resumeSessionId: string
}

/** The subset of `AgentRegistry` the resolver drives. */
export interface AgentRegistryLike {
  get(id: string): AgentLike | undefined
  create(options: CreateAgentOptionsLike): Promise<AgentHandleLike>
  resume(options: ResumeAgentOptionsLike): Promise<AgentHandleLike>
}

/**
 * Persistence-existence check port (backed by `sessionPersistence.list`).
 * The real harness `list()` returns snapshots containing `header`; only its
 * `id` and `cwd` are used here. A header whose `cwd` is undefined is treated as NOT
 * persisted, so the session is re-created fresh (with `header.cwd`) instead of
 * resumed.
 */
export interface SessionPersistenceLike {
  list(options?: unknown): Promise<readonly { readonly header: { readonly id: string; readonly cwd?: string } }[]>
}

/** A resolution result: the live agent plus ownership of its handle. */
export interface ResolvedAgent {
  readonly agent: AgentLike
  /** `true` when the connector created/resumed the agent and owns its disposal. */
  readonly owned: boolean
  /** The owned handle, present only when {@link owned} is true. */
  readonly handle?: AgentHandleLike
}

/** The resolve surface the inbound adapter consumes. */
export interface SessionResolverLike {
  resolve(conversationId: string, requestedSessionId: string | null, fresh: boolean): Promise<ResolvedAgent>
  renew(conversationId: string, title?: string): Promise<RenewedSession>
}

/** A freshly-created session returned by a system-command renewal. */
export interface RenewedSession {
  readonly sessionId: string
  readonly handle: AgentHandleLike
}

interface CreatedOwnedSession extends ResolvedAgent {
  readonly sessionId: string
  readonly handle: AgentHandleLike
}

export interface SessionResolverOptions {
  readonly agents: AgentRegistryLike
  readonly bindings: ConversationBindingStore
  readonly persistence?: SessionPersistenceLike | undefined
  /** Fresh session-id source, overridable in tests. Defaults to a UUID. */
  readonly freshSessionId?: () => string
}

/** Raised when an explicit `session_id` conflicts with the existing binding. */
export class BindingConflictError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'BindingConflictError'
  }
}

/** Resolve the conversation binding to a live, resumed, or freshly-created agent. */
export class SessionResolver implements SessionResolverLike {
  private readonly agents: AgentRegistryLike
  private readonly bindings: ConversationBindingStore
  private readonly persistence: SessionPersistenceLike | undefined
  private readonly freshSessionId: () => string

  constructor(options: SessionResolverOptions) {
    this.agents = options.agents
    this.bindings = options.bindings
    this.persistence = options.persistence
    this.freshSessionId = options.freshSessionId ?? (() => randomUUID())
  }

  /**
   * Resolve the live agent for an inbound message.
   * @param conversationId - the envelope `conversation_id`.
   * @param requestedSessionId - the envelope `session_id`, or `null` when absent.
   * @param fresh - `true` for `new_session`: always create a fresh session and rebind.
   * @returns the live agent and its ownership.
   * @throws {@link BindingConflictError} when a non-null `requestedSessionId`
   *   disagrees with the existing binding (never silently overwrites it).
   */
  async resolve(conversationId: string, requestedSessionId: string | null, fresh: boolean): Promise<ResolvedAgent> {
    if (fresh) return this.createOwned(conversationId)

    const boundSessionId = await this.bindings.sessionIdFor(conversationId)

    let effective: string | null
    if (requestedSessionId !== null && requestedSessionId !== '') {
      if (boundSessionId !== undefined && boundSessionId !== requestedSessionId) {
        throw new BindingConflictError(
          `session_id "${requestedSessionId}" conflicts with bound session "${boundSessionId}" for conversation "${conversationId}"`,
        )
      }
      effective = requestedSessionId
    } else {
      effective = boundSessionId ?? null
    }

    if (effective !== null) {
      const live = this.agents.get(effective)
      if (live !== undefined) {
        if (boundSessionId !== effective) await this.bindings.bind(conversationId, effective)
        return { agent: live, owned: false }
      }
      if (await this.isPersisted(effective)) {
        const handle = await this.agents.resume({ resumeSessionId: effective })
        await this.bindings.bind(conversationId, effective)
        return { agent: handle.agent, owned: true, handle }
      }
      // Bound/explicit session is neither live nor persisted: fall through to a fresh session.
    }
    return this.createOwned(conversationId)
  }

  /** Create and bind a fresh session without driving an agent turn. */
  async renew(conversationId: string, title?: string): Promise<RenewedSession> {
    const sourceSessionId = await this.bindings.sessionIdFor(conversationId)
    const { sessionId, handle } = await this.createOwned(conversationId, title, sourceSessionId)
    return { sessionId, handle }
  }

  private async createOwned(conversationId: string, title?: string, sourceSessionId?: string): Promise<CreatedOwnedSession> {
    const sessionId = this.freshSessionId()
    const handle = await this.agents.create({
      sessionId,
      ...(title !== undefined ? { title } : {}),
      ...(sourceSessionId !== undefined ? { sourceSessionId } : {}),
    })
    await this.bindings.bind(conversationId, sessionId)
    return { agent: handle.agent, owned: true, sessionId, handle }
  }

  private async isPersisted(id: string): Promise<boolean> {
    if (this.persistence === undefined) return false
    const snapshots = await this.persistence.list()
    return snapshots.some(snapshot => snapshot.header.id === id && snapshot.header.cwd !== undefined)
  }
}
