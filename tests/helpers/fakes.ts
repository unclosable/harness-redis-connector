import type {
  AutoClaimResult,
  PendingSummary,
  ReadGroupOptions,
  RedisTransport,
  StreamEntry,
} from '../../src/redis-transport/types.js'
import type {
  AgentHandleLike,
  AgentLike,
  AgentRegistryLike,
  CreateAgentOptionsLike,
  ResumeAgentOptionsLike,
  SessionPersistenceLike,
} from '../../src/session-resolver/index.js'

/** In-memory {@link RedisTransport} fake that records XADD/XACK calls. */
export class FakeRedisTransport implements RedisTransport {
  readonly added: Array<{ stream: string; fields: Record<string, string> }> = []
  readonly acked: Array<{ stream: string; group: string; ids: string[] }> = []
  queue: StreamEntry[] = []
  groups = new Set<string>()
  /** Optional hook invoked inside `xack`, before recording. */
  onAck?: (stream: string, group: string, ids: string[]) => void
  /** Optional hook invoked inside `xadd`, before recording. */
  onAdd?: (stream: string, fields: Record<string, string>) => void

  async xgroupCreate(stream: string, group: string, _id: string): Promise<boolean> {
    if (this.groups.has(group)) return false
    this.groups.add(group)
    return true
  }

  async xreadGroup(_group: string, _consumer: string, _stream: string, options: ReadGroupOptions): Promise<StreamEntry[]> {
    return this.queue.splice(0, options.count)
  }

  async xack(stream: string, group: string, ids: string[]): Promise<void> {
    this.onAck?.(stream, group, ids)
    this.acked.push({ stream, group, ids })
  }

  async xadd(stream: string, fields: Record<string, string>): Promise<string> {
    this.onAdd?.(stream, fields)
    const id = `1-${this.added.length + 1}`
    this.added.push({ stream, fields })
    return id
  }

  async xpending(): Promise<PendingSummary> {
    return { count: 0, minId: null, maxId: null }
  }

  async xautoclaim(): Promise<AutoClaimResult> {
    return { nextStartId: '0-0', entries: [] }
  }

  async close(): Promise<void> {}
}

/** A live-agent fake recording followup/steer deliveries. */
export class FakeAgent implements AgentLike {
  readonly id: string
  readonly followups: unknown[] = []
  readonly steers: unknown[] = []
  disposed = false

  constructor(id: string) {
    this.id = id
  }

  followup(message: unknown): void {
    this.followups.push(message)
  }

  steer(message: unknown): void {
    this.steers.push(message)
  }
}

/** An in-memory {@link AgentRegistryLike} fake. */
export class FakeAgentRegistry implements AgentRegistryLike {
  readonly live = new Map<string, FakeAgent>()
  readonly created: CreateAgentOptionsLike[] = []
  readonly resumed: ResumeAgentOptionsLike[] = []

  get(id: string): FakeAgent | undefined {
    return this.live.get(id)
  }

  async create(options: CreateAgentOptionsLike): Promise<AgentHandleLike> {
    this.created.push(options)
    const agent = new FakeAgent(options.sessionId)
    this.live.set(options.sessionId, agent)
    return {
      agent,
      dispose: async () => {
        this.live.delete(options.sessionId)
        agent.disposed = true
      },
    }
  }

  async resume(options: ResumeAgentOptionsLike): Promise<AgentHandleLike> {
    this.resumed.push(options)
    const agent = new FakeAgent(options.resumeSessionId)
    this.live.set(options.resumeSessionId, agent)
    return {
      agent,
      dispose: async () => {
        this.live.delete(options.resumeSessionId)
        agent.disposed = true
      },
    }
  }
}

/** An in-memory {@link SessionPersistenceLike} fake. */
export class FakeSessionPersistence implements SessionPersistenceLike {
  constructor(public readonly snapshots: Array<{ header: { id: string; cwd?: string } }> = []) {}

  async list(): Promise<Array<{ header: { id: string; cwd?: string } }>> {
    return this.snapshots
  }
}
