import type { Redis } from 'ioredis'
import type { CommandResultStore } from './types.js'

/** Redis-backed system-command result store, one expiring key per message id. */
export class RedisCommandResultStore implements CommandResultStore {
  constructor(
    private readonly client: Redis,
    private readonly keyPrefix: string,
  ) {}

  private key(messageId: string): string {
    return `${this.keyPrefix}:${messageId}`
  }

  async get(messageId: string): Promise<string | undefined> {
    return (await this.client.get(this.key(messageId))) ?? undefined
  }

  async set(messageId: string, serializedResultJson: string, ttlSeconds: number): Promise<void> {
    await this.client.set(this.key(messageId), serializedResultJson, 'EX', ttlSeconds)
  }
}
