/**
 * Minimal Redis Streams surface used by the connector. Kept behind an interface
 * so tests can substitute an in-memory fake and so `ioredis` stays an
 * implementation detail of the `redis-transport` module.
 *
 * @module harness-redis-connector/redis-transport
 */

/** One Redis Stream entry: its id plus a field/value map. */
export interface StreamEntry {
  id: string
  fields: Record<string, string>
}

/** Pending-entries summary returned by `XPENDING`. */
export interface PendingSummary {
  count: number
  minId: string | null
  maxId: string | null
}

/** Result of one `XAUTOCLAIM` pass. */
export interface AutoClaimResult {
  nextStartId: string
  entries: StreamEntry[]
}

/** Options for a blocking `XREADGROUP` read. */
export interface ReadGroupOptions {
  /** Maximum entries to read in one call. */
  count: number
  /** Block time in milliseconds before the read returns empty. */
  blockMs: number
  /** Stream entry id to read from; `>` for new entries. */
  id: string
}

/** The Streams operations the connector drives. */
export interface RedisTransport {
  /**
   * Ensure a consumer group exists on `stream`.
   * @returns `true` when the group was created, `false` when it already existed.
   */
  xgroupCreate(stream: string, group: string, id: string): Promise<boolean>

  /** Read a batch of entries via a consumer group. Resolves `[]` on block timeout. */
  xreadGroup(group: string, consumer: string, stream: string, options: ReadGroupOptions): Promise<StreamEntry[]>

  /** Acknowledge entries as processed. */
  xack(stream: string, group: string, ids: string[]): Promise<void>

  /** Append one entry to a stream. Resolves the new entry id. */
  xadd(stream: string, fields: Record<string, string>): Promise<string>

  /** Summarize the pending-entries list for restart recovery. */
  xpending(stream: string, group: string): Promise<PendingSummary>

  /** Reclaim idle pending entries to `consumer` (restart recovery). */
  xautoclaim(
    stream: string,
    group: string,
    consumer: string,
    minIdleMs: number,
    startId: string,
    count: number,
  ): Promise<AutoClaimResult>

  /** Close the underlying connection. */
  close(): Promise<void>
}
