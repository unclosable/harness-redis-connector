import type { Context } from '@deepseek-ai/cordis'
import { describe, expect, it } from 'vitest'
import { AttachmentError } from '@deepseek-ai/dsh-attachment'
import { agentOptions, createInboundUserMessageBuilder } from '../src/plugin-entry.js'

describe('agentOptions', () => {
  it('reads the current default model selection through live getters', () => {
    let selection = { provider: 'first-provider', model: 'first-model', reasoningEffort: undefined as 'high' | undefined }
    const ctx = {
      get: (name: string) => name === 'agentDefaultModel'
        ? { currentSelection: () => selection }
        : undefined,
    } as unknown as Context
    const options = agentOptions({}, ctx)

    expect(Object.getOwnPropertyDescriptor(options, 'provider')?.get).toBeTypeOf('function')
    expect(options).toMatchObject(selection)

    selection = { provider: 'next-provider', model: 'next-model', reasoningEffort: 'high' }
    expect(options).toMatchObject(selection)
  })

  it('keeps configured routes and tolerates an absent default-model service', () => {
    const noDefaults = { get: () => undefined } as unknown as Context

    expect(agentOptions({ provider: 'configured-provider', model: 'configured-model' }, noDefaults)).toMatchObject({
      provider: 'configured-provider', model: 'configured-model', reasoningEffort: undefined,
    })
    expect(agentOptions({}, noDefaults)).toMatchObject({
      provider: undefined, model: undefined, reasoningEffort: undefined,
    })
  })

  describe('createInboundUserMessageBuilder', () => {
    const agent = {
      model: { current: () => ({ provider: 'deepseek-official', model: 'deepseek-flash' }) },
    } as any

    it('admits text+images in a single admission call and builds a user message', async () => {
      const admitCalls: unknown[][] = []
      const sent: unknown[] = []
      const attachments = {
        admitPromptContent: async (parts: unknown[]) => {
          admitCalls.push(parts)
          return [
            { type: 'text', text: '' },
            {
              type: 'image',
              attachment: {
                attachmentId: 'att-1',
                mediaType: 'image/png',
                bytes: 4,
                width: 2,
                height: 2,
              },
            },
          ]
        },
      }
      const ctx = {
        logger: () => ({ warn: () => {}, error: () => {}, info: () => {} }),
        get: (name: string) => {
          if (name === 'attachments') return attachments
          if (name === 'llm') {
            return { resolveModelInfo: async () => ({ inputModalities: ['text', 'image'] }) }
          }
          return undefined
        },
      } as unknown as Context
      const builder = createInboundUserMessageBuilder({
        ctx,
        config: {},
        sender: { send: async envelope => { sent.push(envelope) } } as any,
      })

      const message = await builder(agent, {
        text: '',
        images: [{ media_type: 'image/png', data: 'iVBORw0KGgo=', name: 'sample.png' }],
      }, {
        conversationId: 'telegram:chat:1',
        messageId: 'm1',
        metadata: {},
      })

      expect(admitCalls).toHaveLength(1)
      expect(admitCalls[0]).toEqual([
        { type: 'text', text: '' },
        { type: 'image', mediaType: 'image/png', data: 'iVBORw0KGgo=', name: 'sample.png' },
      ])
      expect((message as any).content).toHaveLength(2)
      expect(sent).toHaveLength(0)
    })

    it('emits an error and returns undefined when model input modalities exclude image', async () => {
      const sent: any[] = []
      const ctx = {
        logger: () => ({ warn: () => {}, error: () => {}, info: () => {} }),
        get: (name: string) => {
          if (name === 'llm') return { resolveModelInfo: async () => ({ inputModalities: ['text'] }) }
          return undefined
        },
      } as unknown as Context
      const builder = createInboundUserMessageBuilder({
        ctx,
        config: {},
        sender: { send: async envelope => { sent.push(envelope) } } as any,
      })

      const message = await builder(agent, {
        text: '',
        images: [{ media_type: 'image/png', data: 'iVBORw0KGgo=' }],
      }, {
        conversationId: 'telegram:chat:1',
        messageId: 'm1',
        metadata: {},
      })

      expect(message).toBeUndefined()
      expect(sent).toHaveLength(1)
      expect(sent[0].type).toBe('error')
    })

    it('emits attachment error codes on image admission failures', async () => {
      const sent: any[] = []
      const ctx = {
        logger: () => ({ warn: () => {}, error: () => {}, info: () => {} }),
        get: (name: string) => {
          if (name === 'attachments') {
            return {
              admitPromptContent: async () => { throw new AttachmentError('bad image', 'UNSUPPORTED_IMAGE_TYPE') },
            }
          }
          if (name === 'llm') return { resolveModelInfo: async () => ({ inputModalities: ['text', 'image'] }) }
          return undefined
        },
      } as unknown as Context
      const builder = createInboundUserMessageBuilder({
        ctx,
        config: {},
        sender: { send: async envelope => { sent.push(envelope) } } as any,
      })

      const message = await builder(agent, {
        text: '',
        images: [{ media_type: 'image/png', data: 'iVBORw0KGgo=' }],
      }, {
        conversationId: 'telegram:chat:1',
        messageId: 'm1',
        metadata: {},
      })

      expect(message).toBeUndefined()
      expect(sent).toHaveLength(1)
      expect(sent[0].content.text).toContain('UNSUPPORTED_IMAGE_TYPE')
    })
  })
})
