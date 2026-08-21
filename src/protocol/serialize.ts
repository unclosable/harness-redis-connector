import type { OutboundEnvelope } from './types.js'

/**
 * Serialize one outbound envelope to its single-line JSON wire form.
 * @param envelope - the validated outbound envelope.
 * @returns the JSON string to XADD as the entry payload.
 */
export function serializeOutboundEnvelope(envelope: OutboundEnvelope): string {
  const { contentType, ...wire } = envelope
  return JSON.stringify(contentType === undefined ? wire : { ...wire, content_type: contentType })
}
