import type { InboundReceiptStore } from './types.js'

export interface InMemoryReceiptOptions {
  /** Receipt validity window in milliseconds. */
  ttlMs: number
  /** Clock override for tests. Defaults to `Date.now`. */
  now?: () => number
}

/**
 * In-memory {@link InboundReceiptStore}. The canonical, unit-tested
 * implementation of the TTL'd idempotency window; the Redis backend mirrors it
 * with `SET key 1 EX ttl NX`.
 */
export class InMemoryReceiptStore implements InboundReceiptStore {
  private readonly receipts = new Map<string, number>()

  constructor(private readonly options: InMemoryReceiptOptions) {}

  private now(): number {
    return this.options.now?.() ?? Date.now()
  }

  async claim(messageId: string): Promise<boolean> {
    const t = this.now()
    const expiresAt = this.receipts.get(messageId)
    if (expiresAt !== undefined && expiresAt > t) return false
    this.receipts.set(messageId, t + this.options.ttlMs)
    return true
  }
}
