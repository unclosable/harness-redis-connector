/**
 * Idempotency receipts keyed by the external inbound `message_id`. A receipt is
 * valid for a TTL window; a repeated `message_id` within that window is a
 * duplicate and is dropped.
 *
 * @module harness-redis-connector/receipt-store
 */

export interface InboundReceiptStore {
  /**
   * Atomically claim a message id.
   * @param messageId - the external idempotency key (`message_id`).
   * @returns `true` when this is the first claim (and records the receipt),
   *   `false` when the id is already claimed within its TTL.
   */
  claim(messageId: string): Promise<boolean>
}
