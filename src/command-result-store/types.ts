/**
 * Durable serialized system-command results keyed by inbound `message_id`.
 * Replaying the exact stored wire envelope makes command redelivery safe.
 */
export interface CommandResultStore {
  /** Return the serialized result envelope previously stored for this request. */
  get(messageId: string): Promise<string | undefined>

  /** Store a serialized result envelope with the supplied expiration. */
  set(messageId: string, serializedResultJson: string, ttlSeconds: number): Promise<void>
}
