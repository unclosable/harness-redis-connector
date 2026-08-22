import type { CommandResultStore } from './types.js'

/** In-memory command-result store for unit tests. */
export class InMemoryCommandResultStore implements CommandResultStore {
  private readonly entries = new Map<string, { value: string; expiresAt: number }>()

  async get(messageId: string): Promise<string | undefined> {
    const entry = this.entries.get(messageId)
    if (entry === undefined) return undefined
    if (entry.expiresAt <= Date.now()) {
      this.entries.delete(messageId)
      return undefined
    }
    return entry.value
  }

  async set(messageId: string, serializedResultJson: string, ttlSeconds: number): Promise<void> {
    this.entries.set(messageId, { value: serializedResultJson, expiresAt: Date.now() + ttlSeconds * 1000 })
  }
}
