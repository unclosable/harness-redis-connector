import type { Redis } from 'ioredis'
import type { InboundReceiptStore } from './types.js'

/**
 * Redis {@link InboundReceiptStore}: one key per message id, written with
 * `SET key 1 EX ttl NX`, which resolves to `OK` only on the first claim.
 */
export class RedisReceiptStore implements InboundReceiptStore {
  constructor(
    private readonly client: Redis,
    private readonly keyPrefix: string,
    private readonly ttlSeconds: number,
  ) {}

  private key(messageId: string): string {
    return `${this.keyPrefix}:${messageId}`
  }

  async claim(messageId: string): Promise<boolean> {
    const result = await this.client.set(this.key(messageId), '1', 'EX', this.ttlSeconds, 'NX')
    return result === 'OK'
  }
}
