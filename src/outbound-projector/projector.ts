import type { ContentBlock } from '@deepseek-ai/dsh-llm'
import type { ImageAttachmentRef } from '@deepseek-ai/dsh-attachment'
import type { SessionEvent } from '@deepseek-ai/dsh-session'
import { Buffer } from 'node:buffer'
import { basename, isAbsolute, resolve as resolvePath, sep } from 'node:path'
import { realpath } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import type { ConversationBindingStore } from '../binding-store/types.js'
import type { OutboundContentImage } from '../protocol/types.js'
import type { ConversationState } from '../state/conversation-state.js'
import { describeError, type LoggerLike } from '../util.js'
import { buildAssistantEnvelope, buildErrorEnvelope } from './envelopes.js'
import { extractLocalImageReferences, removeLocalImageReferences, type LocalImageReference } from './local-images.js'
import type { OutboundSender } from './sender.js'

/**
 * Extract visible text from assistant content blocks: `text` blocks only,
 * ignoring `reasoning` and every other block type.
 * @param blocks - the assistant message content.
 * @returns the joined visible text.
 */
export function assistantText(blocks: readonly ContentBlock[]): string {
  return blocks
    .filter((block): block is Extract<ContentBlock, { type: 'text' }> => block.type === 'text')
    .map(block => block.text)
    .join('')
}

export interface OutboundProjectorOptions {
  readonly bindings: ConversationBindingStore
  readonly state: ConversationState
  readonly sender: OutboundSender
  readonly logger: LoggerLike
  readonly attachments?: {
    available(sessionId: string): boolean
    readImage(sessionId: string, ref: ImageAttachmentRef): Promise<{ data: Uint8Array }>
  }
  readonly localFiles?: {
    available(sessionId: string): boolean
    agentForSession(sessionId: string): unknown | undefined
    ensureWorkingDirectory(agent: unknown): Promise<string | undefined>
    resolve(sessionId: string, path: string, options: { cwd?: string }): Promise<unknown>
    processPath(sessionId: string, target: unknown): string
    stat(sessionId: string, target: unknown): Promise<{ type: string; size?: number } | undefined>
    readBytes(sessionId: string, target: unknown, maxBytes: number): Promise<Uint8Array>
    realpath?(path: string): Promise<string>
    temporaryDirectory?(): string
  }
  /** Permit local images outside the session cwd and OS temporary directory. */
  readonly outboundImagesOutsideCwd?: boolean
}

const MAX_IMAGE_BYTES = 10_485_760

/**
 * Projects Harness session events onto the outbound stream. Only sessions
 * bound via `session_id → conversation_id` are projected; `assistant/message`
 * becomes an `assistant_message` envelope, and `turn/end` with an error reason
 * becomes an `error` envelope.
 */
export class OutboundProjector {
  private readonly bindings: ConversationBindingStore
  private readonly state: ConversationState
  private readonly sender: OutboundSender
  private readonly logger: LoggerLike
  private readonly attachments: OutboundProjectorOptions['attachments']
  private readonly localFiles: OutboundProjectorOptions['localFiles']
  private readonly outboundImagesOutsideCwd: boolean
  // At most two unfinished turns are retained per session; turn/end deletes its
  // entry, preventing a long-lived connector from accumulating dedupe state.
  private readonly delivered = new Map<string, Map<number, Set<string>>>()
  private readonly skipped = new Map<string, Map<number, Set<string>>>()

  constructor(options: OutboundProjectorOptions) {
    this.bindings = options.bindings
    this.state = options.state
    this.sender = options.sender
    this.logger = options.logger
    this.attachments = options.attachments
    this.localFiles = options.localFiles
    this.outboundImagesOutsideCwd = options.outboundImagesOutsideCwd ?? false
  }

  /**
   * Project one durable `session/event`.
   * @param sessionId - the session that produced the event.
   * @param event - the appended session event.
   */
  async project(sessionId: string, event: SessionEvent): Promise<void> {
    const conversationId = await this.bindings.conversationIdFor(sessionId)
    if (conversationId === undefined) return
    const presented = event as unknown as { type?: unknown; data?: { turn?: unknown; files?: unknown } }
    if (presented.type === 'deliverables/presented') {
      const eventData = presented.data
      const turn = typeof eventData?.turn === 'number' ? eventData.turn : undefined
      const files = Array.isArray(eventData?.files) ? eventData.files.filter((file): file is { path: string } => (
        typeof file === 'object' && file !== null && typeof (file as { path?: unknown }).path === 'string'
      )) : []
      if (turn === undefined) return
      const { images } = await this.localImages(sessionId, turn, files.map((file, index) => ({
        markdown: '', destination: file.path, start: index, end: index,
      })))
      if (images.length === 0) return
      await this.safeSend(buildAssistantEnvelope({
        conversationId,
        sessionId,
        correlationId: this.state.correlationFor(conversationId),
        text: '',
        images,
        metadata: this.state.metadataFor(conversationId),
      }))
      return
    }
    switch (event.type) {
      case 'assistant/message': {
        const sourceText = assistantText(event.data.message.content)
        const turn = event.data.turn
        const markdown = extractLocalImageReferences(sourceText)
        const { images: localImages, forwarded } = await this.localImages(sessionId, turn, markdown.references)
        const text = forwarded.length === 0 ? sourceText : removeLocalImageReferences(sourceText, forwarded)
        const images = [...localImages, ...await this.assistantImages(sessionId, turn, event.data.message.content)]
        if (text.length === 0 && images.length === 0) return
        await this.safeSend(buildAssistantEnvelope({
          conversationId,
          sessionId,
          correlationId: this.state.correlationFor(conversationId),
          text,
          ...images.length > 0 ? { images } : {},
          metadata: this.state.metadataFor(conversationId),
        }))
        return
      }
      case 'turn/end': {
        if (event.data.reason.kind === 'error') {
          await this.safeSend(buildErrorEnvelope({
            conversationId,
            sessionId,
            correlationId: this.state.correlationFor(conversationId),
            text: event.data.reason.error.message,
            metadata: this.state.metadataFor(conversationId),
          }))
        }
        this.delivered.get(sessionId)?.delete(event.data.turn)
        if (this.delivered.get(sessionId)?.size === 0) this.delivered.delete(sessionId)
        this.skipped.get(sessionId)?.delete(event.data.turn)
        if (this.skipped.get(sessionId)?.size === 0) this.skipped.delete(sessionId)
        return
      }
      default:
        // Merge-extensible session event vocabulary: non-projected types fall through.
        return
    }
  }

  /**
   * Project a live `agent/error` notification for a bound session.
   * @param sessionId - the agent's session id.
   * @param error - the failure, verbatim.
   */
  async handleAgentError(sessionId: string, error: unknown): Promise<void> {
    const conversationId = await this.bindings.conversationIdFor(sessionId)
    if (conversationId === undefined) return
    await this.safeSend(buildErrorEnvelope({
      conversationId,
      sessionId,
      correlationId: this.state.correlationFor(conversationId),
      text: describeError(error),
      metadata: this.state.metadataFor(conversationId),
    }))
  }

  private async safeSend(envelope: Parameters<OutboundSender['send']>[0]): Promise<void> {
    try {
      await this.sender.send(envelope)
    } catch (error: unknown) {
      this.logger.warn(`outbound projection failed: ${describeError(error)}`)
    }
  }

  private async assistantImages(sessionId: string, turn: number, blocks: readonly ContentBlock[]): Promise<OutboundContentImage[]> {
    const images = blocks.filter((block): block is Extract<ContentBlock, { type: 'image'; attachment: ImageAttachmentRef }> => block.type === 'image')
    const attachments = this.attachments
    if (attachments === undefined || !attachments.available(sessionId)) {
      for (const block of images) this.warnImageSkip(sessionId, turn, 'no-attachments-service', attachmentDestination(block.attachment))
      return []
    }
    const result: OutboundContentImage[] = []
    for (const block of images) {
      const destination = attachmentDestination(block.attachment)
      const key = `attachment:${block.attachment.attachmentId}`
      if (this.deliveredFor(sessionId, turn).has(key)) continue
      try {
        const stored = await attachments.readImage(sessionId, block.attachment)
        if (stored.data.byteLength > MAX_IMAGE_BYTES) {
          this.warnImageSkip(sessionId, turn, 'too-large', destination)
          continue
        }
        const mediaType = sniffImage(stored.data)
        if (mediaType === undefined) {
          this.warnImageSkip(sessionId, turn, 'unsupported-bytes', destination)
          continue
        }
        this.deliveredFor(sessionId, turn).add(key)
        result.push({
          media_type: mediaType,
          data: Buffer.from(stored.data).toString('base64'),
          ...block.attachment.name !== undefined ? { name: block.attachment.name } : {},
        })
      } catch {
        this.warnImageSkip(sessionId, turn, 'read-failed', destination)
      }
    }
    return result
  }

  private async localImages(sessionId: string, turn: number, references: readonly LocalImageReference[]): Promise<{ images: OutboundContentImage[]; forwarded: LocalImageReference[] }> {
    if (this.localFiles === undefined || !this.localFiles.available(sessionId)) {
      for (const reference of references) this.warnImageSkip(sessionId, turn, 'no-fs-service', reference.destination)
      return { images: [], forwarded: [] }
    }
    const result: OutboundContentImage[] = []
    const forwarded: LocalImageReference[] = []
    let cwd: string | undefined
    let canonicalCwd: string | undefined
    let canonicalTmp: string | undefined
    for (const reference of references) {
      if ((!this.outboundImagesOutsideCwd || !isAbsolute(reference.destination)) && cwd === undefined) {
        const agent = this.localFiles.agentForSession(sessionId)
        if (agent === undefined) {
          this.warnImageSkip(sessionId, turn, 'no-agent', reference.destination)
          continue
        }
        try {
          cwd = await this.localFiles.ensureWorkingDirectory(agent)
          if (cwd === undefined) this.warnImageSkip(sessionId, turn, 'no-cwd', reference.destination)
        } catch {
          this.warnImageSkip(sessionId, turn, 'no-cwd', reference.destination)
          continue
        }
        if (cwd === undefined) continue
      }
      let target: unknown
      let path: string
      try {
        target = await this.localFiles.resolve(sessionId, reference.destination, isAbsolute(reference.destination) ? {} : { cwd })
        path = this.localFiles.processPath(sessionId, target)
      } catch {
        this.warnImageSkip(sessionId, turn, 'resolve-failed', reference.destination)
        continue
      }
      try {
        const normalizedPath = resolvePath(path)
        const canonicalPath = await this.canonicalPath(normalizedPath)
        // DSH's local fs only normalizes paths; it does not contain them. Compare
        // realpaths so both the candidate and roots resolve symlinks, and require
        // a path-separator boundary to reject /work versus /workspace-x escapes.
        if (!this.outboundImagesOutsideCwd) {
          if (canonicalPath === undefined) {
            this.warnImageSkip(sessionId, turn, 'canonicalize-failed', reference.destination)
            continue
          }
          canonicalCwd ??= cwd === undefined ? undefined : await this.canonicalPath(cwd)
          canonicalTmp ??= await this.canonicalPath(this.localFiles.temporaryDirectory?.() ?? tmpdir())
          if ((canonicalCwd === undefined || !isWithin(canonicalPath, canonicalCwd)) &&
            (canonicalTmp === undefined || !isWithin(canonicalPath, canonicalTmp))) {
            this.warnImageSkip(sessionId, turn, 'outside-scope', reference.destination)
            continue
          }
        }
        const identity = canonicalPath ?? normalizedPath
        if (this.deliveredFor(sessionId, turn).has(`path:${identity}`)) {
          // The image was already delivered by markdown or present in this turn;
          // suppress a duplicate markdown rendering as well.
          forwarded.push(reference)
          continue
        }
        const info = await this.localFiles.stat(sessionId, target)
        if (info === undefined || info.type !== 'file') {
          this.warnImageSkip(sessionId, turn, 'not-a-file', reference.destination)
          continue
        }
        if (info.size !== undefined && info.size > MAX_IMAGE_BYTES) {
          this.warnImageSkip(sessionId, turn, 'too-large', reference.destination)
          continue
        }
        const data = await this.localFiles.readBytes(sessionId, target, MAX_IMAGE_BYTES)
        const mediaType = sniffImage(data)
        if (mediaType === undefined) {
          this.warnImageSkip(sessionId, turn, 'unsupported-bytes', reference.destination)
          continue
        }
        this.deliveredFor(sessionId, turn).add(`path:${identity}`)
        result.push({ media_type: mediaType, name: basename(path), data: Buffer.from(data).toString('base64') })
        forwarded.push(reference)
      } catch {
        this.warnImageSkip(sessionId, turn, 'read-failed', reference.destination)
      }
    }
    return { images: result, forwarded }
  }

  private deliveredFor(sessionId: string, turn: number): Set<string> {
    let turns = this.delivered.get(sessionId)
    if (turns === undefined) {
      turns = new Map<number, Set<string>>()
      this.delivered.set(sessionId, turns)
    }
    let delivered = turns.get(turn)
    if (delivered === undefined) {
      delivered = new Set<string>()
      turns.set(turn, delivered)
      while (turns.size > 2) turns.delete(turns.keys().next().value!)
    }
    return delivered
  }

  private warnImageSkip(sessionId: string, turn: number, reason: string, destination: string): void {
    let turns = this.skipped.get(sessionId)
    if (turns === undefined) {
      turns = new Map()
      this.skipped.set(sessionId, turns)
    }
    let reasons = turns.get(turn)
    if (reasons === undefined) {
      reasons = new Set()
      turns.set(turn, reasons)
      while (turns.size > 2) turns.delete(turns.keys().next().value!)
    }
    if (reasons.has(reason)) return
    reasons.add(reason)
    this.logger.warn(`image-forward skipped reason=${reason} destination=${destination}`)
  }

  private async canonicalPath(path: string): Promise<string | undefined> {
    try {
      return await (this.localFiles?.realpath?.(path) ?? realpath(path))
    } catch {
      return undefined
    }
  }
}

function attachmentDestination(attachment: ImageAttachmentRef): string {
  return attachment.name ?? String(attachment.attachmentId)
}

function isWithin(path: string, root: string): boolean {
  return path === root || path.startsWith(root.endsWith(sep) ? root : `${root}${sep}`)
}

function sniffImage(data: Uint8Array): OutboundContentImage['media_type'] | undefined {
  if (data.length >= 8 && data[0] === 0x89 && data[1] === 0x50 && data[2] === 0x4e && data[3] === 0x47 && data[4] === 0x0d && data[5] === 0x0a && data[6] === 0x1a && data[7] === 0x0a) return 'image/png'
  if (data.length >= 3 && data[0] === 0xff && data[1] === 0xd8 && data[2] === 0xff) return 'image/jpeg'
  if (data.length >= 12 && String.fromCharCode(...data.slice(0, 4)) === 'RIFF' && String.fromCharCode(...data.slice(8, 12)) === 'WEBP') return 'image/webp'
  if (data.length >= 6 && (String.fromCharCode(...data.slice(0, 6)) === 'GIF87a' || String.fromCharCode(...data.slice(0, 6)) === 'GIF89a')) return 'image/gif'
  return undefined
}
