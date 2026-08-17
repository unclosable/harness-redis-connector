import { describe, expect, it } from 'vitest'
import { InMemoryReceiptStore } from '../src/receipt-store/index.js'

describe('receipt idempotency', () => {
  it('claims a new id once and rejects duplicates within the TTL', async () => {
    const store = new InMemoryReceiptStore({ ttlMs: 1000 })
    expect(await store.claim('m1')).toBe(true)
    expect(await store.claim('m1')).toBe(false)
    expect(await store.claim('m2')).toBe(true)
  })

  it('expires a receipt after the TTL', async () => {
    let now = 0
    const store = new InMemoryReceiptStore({ ttlMs: 100, now: () => now })
    expect(await store.claim('m1')).toBe(true)
    now = 50
    expect(await store.claim('m1')).toBe(false)
    now = 150
    expect(await store.claim('m1')).toBe(true)
  })
})
