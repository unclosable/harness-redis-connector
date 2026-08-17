import { Redis } from 'ioredis'
import type { AutoClaimResult, PendingSummary, ReadGroupOptions, RedisTransport, StreamEntry } from './types.js'

/** Whether a `ReplyError` reports an already-existing consumer group. */
function isBusyGroup(error: unknown): boolean {
  return error instanceof Error && /BUSYGROUP/.test(error.message)
}

type RawFields = Array<string | Buffer>
type RawEntry = [id: string, fields: RawFields]
type RawStream = [name: string, entries: RawEntry[]]

/** Fold an alternating `[field, value, ...]` array into a record. */
function fieldsToRecord(fields: RawFields): Record<string, string> {
  const record: Record<string, string> = {}
  for (let i = 0; i < fields.length; i += 2) {
    const key = fields[i]
    const value = fields[i + 1]
    if (key !== undefined && value !== undefined) record[String(key)] = String(value)
  }
  return record
}

/** Flatten a field/value record into the alternating `[field, value, ...]` args `XADD` expects. */
function recordToArgs(fields: Record<string, string>): string[] {
  const args: string[] = []
  for (const [key, value] of Object.entries(fields)) args.push(key, value)
  return args
}

/**
 * `ioredis`-backed {@link RedisTransport}. One instance wraps one `Redis` client;
 * callers own the client's connection options and lifecycle via {@link close}.
 */
export class IoredisRedisTransport implements RedisTransport {
  constructor(private readonly client: Redis) {}

  async xgroupCreate(stream: string, group: string, id: string): Promise<boolean> {
    try {
      await this.client.xgroup('CREATE', stream, group, id, 'MKSTREAM')
      return true
    } catch (error: unknown) {
      if (isBusyGroup(error)) return false
      throw error
    }
  }

  async xreadGroup(group: string, consumer: string, stream: string, options: ReadGroupOptions): Promise<StreamEntry[]> {
    const result = await this.client.xreadgroup(
      'GROUP', group, consumer,
      'COUNT', options.count,
      'BLOCK', options.blockMs,
      'STREAMS', stream, options.id,
    ) as unknown as RawStream[] | null
    if (result === null) return []
    const raw = result[0]
    if (raw === undefined) return []
    return raw[1].map(([id, fields]) => ({ id, fields: fieldsToRecord(fields) }))
  }

  async xack(stream: string, group: string, ids: string[]): Promise<void> {
    if (ids.length === 0) return
    await this.client.xack(stream, group, ...ids)
  }

  async xadd(stream: string, fields: Record<string, string>): Promise<string> {
    const id = await this.client.xadd(stream, '*', ...recordToArgs(fields))
    return id ?? ''
  }

  async xpending(stream: string, group: string): Promise<PendingSummary> {
    const result = await this.client.xpending(stream, group) as unknown as [number, string | null, string | null]
    return { count: result[0], minId: result[1], maxId: result[2] }
  }

  async xautoclaim(
    stream: string,
    group: string,
    consumer: string,
    minIdleMs: number,
    startId: string,
    count: number,
  ): Promise<AutoClaimResult> {
    const result = await this.client.xautoclaim(
      stream, group, consumer, minIdleMs, startId, 'COUNT', count,
    ) as unknown as [nextStartId: string, entries: RawEntry[]]
    return { nextStartId: result[0], entries: result[1].map(([id, fields]) => ({ id, fields: fieldsToRecord(fields) })) }
  }

  async close(): Promise<void> {
    await this.client.quit()
  }
}

/**
 * Build an `ioredis` client from an optional URL.
 * @param url - Redis connection URL; omitted to use ioredis' localhost default.
 * @returns the connected client.
 */
export function createRedisClient(url?: string): Redis {
  return url !== undefined ? new Redis(url) : new Redis()
}
