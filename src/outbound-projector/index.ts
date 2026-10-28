export { buildAssistantEnvelope, buildErrorEnvelope, buildSystemCommandResultEnvelope, type AssistantEnvelopeArgs, type ErrorEnvelopeArgs, type SystemCommandResultEnvelopeArgs } from './envelopes.js'
export { OutboundSender } from './sender.js'
export { OutboundProjector, assistantText, type OutboundProjectorOptions } from './projector.js'
export { extractLocalImageReferences, removeLocalImageReferences, type ExtractedLocalImages, type LocalImageReference } from './local-images.js'
