import { describe, expect, it, vi } from 'vitest'
import type { SessionEvent } from '@deepseek-ai/dsh-session'
import type { ImageAttachmentRef } from '@deepseek-ai/dsh-attachment'
import { InMemoryConversationBindingStore } from '../src/binding-store/index.js'
import { extractLocalImageReferences, OutboundProjector, OutboundSender } from '../src/outbound-projector/index.js'
import { InMemoryConversationState } from '../src/state/conversation-state.js'
import { FakeRedisTransport } from './helpers/fakes.js'

function assistantEvent(blocks: unknown[]): SessionEvent {
  return {
    type: 'assistant/message',
    seq: 3,
    time: 0,
    data: {
      turn: 1,
      step: 1,
      message: {
        id: 'a1',
        role: 'assistant',
        source: { kind: 'model', provider: 'p', model: 'm' },
        content: blocks,
      },
    },
  } as unknown as SessionEvent
}

function turnEndErrorEvent(message: string): SessionEvent {
  return {
    type: 'turn/end',
    seq: 4,
    time: 0,
    data: { turn: 1, reason: { kind: 'error', error: { message, code: 'E' } } },
  } as unknown as SessionEvent
}

function presentedEvent(turn: number, paths: string[]): SessionEvent {
  return { type: 'deliverables/presented', seq: 4, time: 0, data: { turn, callId: 'call-1', files: paths.map(path => ({ path })) } } as unknown as SessionEvent
}

const png = Uint8Array.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])

function setup(options: {
  attachments?: { readImage(ref: ImageAttachmentRef): Promise<{ data: Uint8Array }> }
  localFiles?: ConstructorParameters<typeof OutboundProjector>[0]['localFiles']
  outboundImagesOutsideCwd?: boolean
  warn?: ReturnType<typeof vi.fn>
} = {}) {
  const transport = new FakeRedisTransport()
  const bindings = new InMemoryConversationBindingStore()
  const state = new InMemoryConversationState()
  const sender = new OutboundSender(transport, 'harness:outbound', 'json')
  const projector = new OutboundProjector({
    bindings,
    state,
    sender,
    attachments: options.attachments === undefined ? undefined : { available: () => true, ...options.attachments },
    localFiles: options.localFiles,
    outboundImagesOutsideCwd: options.outboundImagesOutsideCwd,
    logger: { warn: options.warn ?? (() => {}), error: () => {}, info: () => {} },
  })
  return { transport, bindings, state, projector }
}

function lastPayload(transport: FakeRedisTransport): Record<string, unknown> {
  const entry = transport.added[transport.added.length - 1]
  return JSON.parse(entry!.fields['json']!) as Record<string, unknown>
}

describe('outbound projector', () => {
  it('projects assistant/message to an outbound envelope with correlation and text', async () => {
    const { transport, bindings, state, projector } = setup()
    await bindings.bind('telegram:chat:1', 'session-1')
    state.recordInbound('telegram:chat:1', 'telegram-update-1001', { chatId: '1' })

    await projector.project('session-1', assistantEvent([{ type: 'text', text: 'hello' }]))

    expect(transport.added).toHaveLength(1)
    expect(lastPayload(transport)).toMatchObject({
      type: 'assistant_message',
      conversation_id: 'telegram:chat:1',
      session_id: 'session-1',
      correlation_id: 'telegram-update-1001',
    })
    expect(lastPayload(transport).content_type).toBe('text/markdown')
    expect(lastPayload(transport).content).toBe('hello')
    expect(lastPayload(transport).metadata).toEqual({ chatId: '1' })
  })

  it('extracts text from text blocks only, ignoring reasoning blocks', async () => {
    const { transport, bindings, projector } = setup()
    await bindings.bind('telegram:chat:1', 'session-1')

    await projector.project('session-1', assistantEvent([
      { type: 'text', text: 'A' },
      { type: 'reasoning', text: 'hidden' },
      { type: 'text', text: 'B' },
    ]))

    expect(lastPayload(transport).content_type).toBe('text/markdown')
    expect(lastPayload(transport).content).toBe('AB')
  })

  it('adds assistant image blocks to outbound envelopes', async () => {
    const { transport, bindings, projector } = setup({ attachments: {
      readImage: async (_ref: ImageAttachmentRef) => ({ data: png }),
    } })
    await bindings.bind('telegram:chat:1', 'session-1')
    const imageRef: ImageAttachmentRef = {
      attachmentId: 'att-1' as ImageAttachmentRef['attachmentId'],
      mediaType: 'image/png',
      bytes: png.length,
      width: 2,
      height: 2,
      name: 'chart.png',
    }
    await projector.project('session-1', assistantEvent([
      { type: 'text', text: 'hello' },
      { type: 'image', attachment: imageRef },
    ]))

    const payload = lastPayload(transport)
    expect(payload.content).toEqual({
      text: 'hello',
      images: [{ media_type: 'image/png', name: 'chart.png', data: 'iVBORw0KGgo=' }],
    })
  })

  it('degrades to text-only when image reads fail', async () => {
    const { transport, bindings, projector } = setup({ attachments: {
      readImage: async () => { throw new Error('read failed') },
    } })
    await bindings.bind('telegram:chat:1', 'session-1')
    const imageRef: ImageAttachmentRef = {
      attachmentId: 'att-2' as ImageAttachmentRef['attachmentId'],
      mediaType: 'image/png',
      bytes: 4,
      width: 2,
      height: 2,
    }
    await projector.project('session-1', assistantEvent([
      { type: 'text', text: 'hello' },
      { type: 'image', attachment: imageRef },
    ]))

    expect(lastPayload(transport).content).toBe('hello')
  })

  it('ignores sessions that are not bound', async () => {
    const { transport, projector } = setup()
    await projector.project('session-unbound', assistantEvent([{ type: 'text', text: 'hi' }]))
    expect(transport.added).toHaveLength(0)
  })

  it('projects turn/end with an error reason to an error envelope', async () => {
    const { transport, bindings, projector } = setup()
    await bindings.bind('telegram:chat:1', 'session-1')

    await projector.project('session-1', turnEndErrorEvent('boom'))

    const payload = lastPayload(transport)
    expect(payload.type).toBe('error')
    expect(payload.content).toEqual({ text: 'boom' })
  })

  it('projects agent/error to an error envelope', async () => {
    const { transport, bindings, projector } = setup()
    await bindings.bind('telegram:chat:1', 'session-1')

    await projector.handleAgentError('session-1', new Error('agent failed'))

    const payload = lastPayload(transport)
    expect(payload.type).toBe('error')
    expect(payload.content).toEqual({ text: 'agent failed' })
  })

  it('forwards local markdown images before cleaned text', async () => {
    const files = localFiles({ '/work/charts/rev.png': png })
    const { transport, bindings, projector } = setup({ localFiles: files })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', assistantEvent([{ type: 'text', text: 'Here\n\n![chart](<charts/rev.png>)\n\nDone' }]))
    expect(lastPayload(transport).content).toEqual({ text: 'Here\n\nDone', images: [{ media_type: 'image/png', name: 'rev.png', data: 'iVBORw0KGgo=' }] })
    expect(files.resolve).toHaveBeenCalledWith('session-1', 'charts/rev.png', { cwd: '/work' })
  })

  it('uses absolute markdown destinations without resolving a session cwd', async () => {
    const files = localFiles({ '/tmp/chart.png': png })
    const { transport, bindings, projector } = setup({ localFiles: files })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![chart](/tmp/chart.png)' }]))
    expect(lastPayload(transport).content).toMatchObject({ text: '', images: [{ name: 'chart.png' }] })
    expect(files.resolve).toHaveBeenCalledWith('session-1', '/tmp/chart.png', {})
  })

  it('keeps markdown text when a local image cannot be forwarded', async () => {
    const warn = vi.fn()
    const { transport, bindings, projector } = setup({ localFiles: localFiles({}), warn })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![chart](missing.png)' }]))
    expect(lastPayload(transport).content).toBe('![chart](missing.png)')
    expect(warn).toHaveBeenCalledWith('image-forward skipped reason=not-a-file destination=missing.png')
  })

  it('keeps relative markdown when no session working directory can be resolved', async () => {
    const files = localFiles({ '/work/chart.png': png })
    const noAgentFiles = { ...files, agentForSession: () => undefined }
    const warn = vi.fn()
    const { transport, bindings, projector } = setup({ localFiles: noAgentFiles, warn })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![chart](chart.png)' }]))
    expect(lastPayload(transport).content).toBe('![chart](chart.png)')
    expect(warn).toHaveBeenCalledWith('image-forward skipped reason=no-agent destination=chart.png')
  })

  it('keeps markdown text for non-images and oversized local files', async () => {
    const files = localFiles({ '/work/nope.bin': Uint8Array.of(1, 2), '/work/huge.png': png }, { '/work/huge.png': 10_485_761 })
    const warn = vi.fn()
    const { transport, bindings, projector } = setup({ localFiles: files, warn })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![n](nope.bin) ![h](huge.png)' }]))
    expect(lastPayload(transport).content).toBe('![n](nope.bin) ![h](huge.png)')
    expect(warn).toHaveBeenCalledWith('image-forward skipped reason=unsupported-bytes destination=nope.bin')
    expect(warn).toHaveBeenCalledWith('image-forward skipped reason=too-large destination=huge.png')
  })

  it('forwards presented local images as an images-only envelope and dedupes markdown', async () => {
    const { transport, bindings, projector } = setup({ localFiles: localFiles({ '/work/chart.png': png }) })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![chart](chart.png)' }]))
    await projector.project('session-1', presentedEvent(1, ['/work/chart.png']))
    expect(transport.added).toHaveLength(1)
    expect(lastPayload(transport).content).toEqual({ text: '', images: [{ media_type: 'image/png', name: 'chart.png', data: 'iVBORw0KGgo=' }] })
  })

  it('removes markdown when the same image was already delivered by present', async () => {
    const { transport, bindings, projector } = setup({ localFiles: localFiles({ '/work/chart.png': png }) })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', presentedEvent(1, ['/work/chart.png']))
    await projector.project('session-1', assistantEvent([{ type: 'text', text: 'See ![chart](chart.png)' }]))
    expect(transport.added).toHaveLength(2)
    expect(lastPayload(transport).content).toBe('See')
  })

  it('forwards a presented image without assistant text', async () => {
    const { transport, bindings, projector } = setup({ localFiles: localFiles({ '/work/chart.png': png }) })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', presentedEvent(3, ['/work/chart.png']))
    expect(lastPayload(transport).content).toEqual({ text: '', images: [{ media_type: 'image/png', name: 'chart.png', data: 'iVBORw0KGgo=' }] })
  })

  it('does not forward or alter markdown image references in fenced or inline code', async () => {
    const files = localFiles({ '/work/a.png': png })
    const { transport, bindings, projector } = setup({ localFiles: files })
    await bindings.bind('telegram:chat:1', 'session-1')
    const text = '```md\n![x](a.png)\n```\n`![y](a.png)`\n![z](a.png)'
    await projector.project('session-1', assistantEvent([{ type: 'text', text }]))
    expect(lastPayload(transport).content).toEqual({ text: '```md\n![x](a.png)\n```\n`![y](a.png)`\n', images: [{ media_type: 'image/png', name: 'a.png', data: 'iVBORw0KGgo=' }] })
  })

  it('keeps an unterminated fence byte-for-byte and leaves no-image text unchanged', async () => {
    const { transport, bindings, projector } = setup({ localFiles: localFiles({ '/work/a.png': png }) })
    await bindings.bind('telegram:chat:1', 'session-1')
    const text = 'before\n~~~\n![x](a.png)\n'
    await projector.project('session-1', assistantEvent([{ type: 'text', text }]))
    expect(lastPayload(transport).content).toBe(text)
  })

  it('enforces canonical cwd/temp containment before every local read', async () => {
    const files = localFiles({ '/work/in.png': png, '/outside.png': png, '/work/link.png': png, '/tmp/tmp.png': png, '/workspace-x/no.png': png }, {}, {
      '/work/link.png': '/outside.png',
    })
    const warn = vi.fn()
    const { transport, bindings, projector } = setup({ localFiles: files, warn })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![in](in.png) ![out](../outside.png) ![link](link.png) ![tmp](/tmp/tmp.png) ![prefix](/workspace-x/no.png)' }]))
    expect(lastPayload(transport).content).toEqual({ text: '![out](../outside.png) ![link](link.png) ![prefix](/workspace-x/no.png)', images: [
      { media_type: 'image/png', name: 'in.png', data: 'iVBORw0KGgo=' },
      { media_type: 'image/png', name: 'tmp.png', data: 'iVBORw0KGgo=' },
    ] })
    expect(files.readBytes).toHaveBeenCalledTimes(2)
    expect(warn).toHaveBeenCalledWith('image-forward skipped reason=outside-scope destination=../outside.png')
  })

  it('forwards when the filesystem service becomes available after projector construction', async () => {
    let service: ReturnType<typeof localFiles> | undefined
    const lazyFiles = {
      available: () => service !== undefined,
      agentForSession: (sessionId: string) => service?.agentForSession(sessionId),
      ensureWorkingDirectory: (agent: unknown) => service?.ensureWorkingDirectory(agent),
      resolve: (_sessionId: string, path: string, options: { cwd?: string }) => service?.resolve('session-1', path, options) ?? Promise.reject(new Error('unavailable')),
      processPath: (_sessionId: string, target: unknown) => {
        if (service === undefined) throw new Error('unavailable')
        return service.processPath('session-1', target)
      },
      stat: (_sessionId: string, target: unknown) => service?.stat('session-1', target) ?? Promise.reject(new Error('unavailable')),
      readBytes: (_sessionId: string, target: unknown, maxBytes: number) => service?.readBytes('session-1', target, maxBytes) ?? Promise.reject(new Error('unavailable')),
      realpath: (path: string) => service?.realpath!(path) ?? Promise.reject(new Error('unavailable')),
      temporaryDirectory: () => '/tmp',
    }
    const { transport, bindings, projector } = setup({ localFiles: lazyFiles })
    service = localFiles({ '/work/chart.png': png })
    await bindings.bind('telegram:chat:1', 'session-1')

    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![chart](chart.png)' }]))

    expect(lastPayload(transport).content).toMatchObject({ text: '', images: [{ name: 'chart.png' }] })
  })

  it('warns no-fs-service once per reason for a session turn', async () => {
    const warn = vi.fn()
    const unavailable = { ...localFiles({ '/work/chart.png': png }), available: () => false }
    const { bindings, projector } = setup({ localFiles: unavailable, warn })
    await bindings.bind('telegram:chat:1', 'session-1')

    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![a](chart.png) ![b](other.png)' }]))

    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith('image-forward skipped reason=no-fs-service destination=chart.png')
  })

  it('allows an outside image only when outboundImagesOutsideCwd is enabled', async () => {
    const files = localFiles({ '/outside.png': png })
    const { transport, bindings, projector } = setup({ localFiles: files, outboundImagesOutsideCwd: true })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![out](/outside.png)' }]))
    expect(lastPayload(transport).content).toMatchObject({ text: '', images: [{ name: 'outside.png' }] })
  })

  it('dedupes normalized/canonical paths and clears turn state at turn end', async () => {
    const files = localFiles({ '/work/a.png': png })
    const { transport, bindings, projector } = setup({ localFiles: files })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![a](./a.png) ![b](a.png)' }]))
    expect(lastPayload(transport).content).toEqual({ text: '', images: [{ media_type: 'image/png', name: 'a.png', data: 'iVBORw0KGgo=' }] })
    await projector.project('session-1', turnEndErrorEvent('done'))
    await projector.project('session-1', assistantEvent([{ type: 'text', text: '![a](a.png)' }]))
    expect(transport.added).toHaveLength(3)
  })

  it('sniffs and caps assistant image blocks from returned bytes', async () => {
    const warn = vi.fn()
    const { transport, bindings, projector } = setup({ attachments: { readImage: async () => ({ data: Uint8Array.of(1, 2, 3) }) }, warn })
    await bindings.bind('telegram:chat:1', 'session-1')
    await projector.project('session-1', assistantEvent([{ type: 'text', text: 'text' }, { type: 'image', attachment: { attachmentId: 'x', mediaType: 'image/png' } }]))
    expect(lastPayload(transport).content).toBe('text')
    expect(warn).toHaveBeenCalledWith('image-forward skipped reason=unsupported-bytes destination=x')
  })
})

function localFiles(files: Record<string, Uint8Array>, sizes: Record<string, number> = {}, canonical: Record<string, string> = {}) {
  const resolve = vi.fn(async (_sessionId: string, path: string, options?: { cwd?: string }) => ({ path: path.startsWith('/') ? path : new URL(path, `file://${options!.cwd}/`).pathname }))
  const readBytes = vi.fn(async (_sessionId: string, target: unknown, _maxBytes: number) => files[(target as { path: string }).path]!)
  return {
    available: () => true,
    agentForSession: () => ({}),
    ensureWorkingDirectory: async () => '/work',
    resolve,
    processPath: (_sessionId: string, target: unknown) => (target as { path: string }).path,
    stat: async (_sessionId: string, target: unknown) => {
      const path = (target as { path: string }).path
      return files[path] === undefined ? undefined : { type: 'file', size: sizes[path] ?? files[path].length }
    },
    readBytes,
    realpath: async (path: string) => canonical[path] ?? path,
    temporaryDirectory: () => '/tmp',
  }
}

describe('local markdown images', () => {
  it('extracts angle, plain, title, encoded, and file destinations in order', () => {
    const result = extractLocalImageReferences('![a](<charts/a b.png>) ![b](plain.png "title") ![c](dir%2Fd.png) ![d](file:///tmp/e.png)')
    expect(result.references.map(reference => reference.destination)).toEqual(['charts/a b.png', 'plain.png', 'dir/d.png', '/tmp/e.png'])
    expect(result.text).toBe('')
  })

  it('keeps http, data, and dsh-app image references and collapses removed blank lines', () => {
    const result = extractLocalImageReferences('A\n\n![x](local.png)\n\n\nB ![h](https://x/y.png) ![d](data:image/png;base64,x) ![a](dsh-app://x)')
    expect(result.references).toHaveLength(1)
    expect(result.text).toBe('A\n\nB ![h](https://x/y.png) ![d](data:image/png;base64,x) ![a](dsh-app://x)')
  })

  it('preserves surrounding whitespace while cleaning inline and standalone references', () => {
    expect(extractLocalImageReferences('  See ![x](a.png) now  ').text).toBe('  See now  ')
    expect(extractLocalImageReferences('A\n\n![x](a.png)\n\n\nB').text).toBe('A\n\nB')
    expect(extractLocalImageReferences('![a](a.png) ![b](b.png)').text).toBe('')
  })

  it('does not let a longer backtick run close an inline code span', () => {
    const text = '`![x](a.png)`` then ![y](b.png)'
    expect(extractLocalImageReferences(text).references.map(reference => reference.destination)).toEqual(['a.png', 'b.png'])
  })
})
