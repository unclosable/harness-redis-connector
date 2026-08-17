import { serializeOutboundEnvelope, type OutboundEnvelope } from '../protocol/index.js'
import type { RedisTransport } from '../redis-transport/types.js'

/** Serializes and appends outbound envelopes to the outbound stream. */
export class OutboundSender {
  constructor(
    private readonly transport: RedisTransport,
    private readonly stream: string,
  ) {}

  /**
   * Append one envelope to the outbound stream under the canonical `json` field.
   * @param envelope - the envelope to serialize and XADD.
   */
  async send(envelope: OutboundEnvelope): Promise<void> {
    const payload = serializeOutboundEnvelope(envelope)
    await this.transport.xadd(this.stream, { json: payload })
  }
}
