import { describe, expect, it } from 'vitest'
import { InMemoryConversationBindingStore } from '../src/binding-store/index.js'
import { BindingConflictError, SessionResolver } from '../src/session-resolver/index.js'
import { FakeAgent, FakeAgentRegistry, FakeSessionPersistence } from './helpers/fakes.js'

function setup(overrides: { persistence?: FakeSessionPersistence } = {}) {
  const agents = new FakeAgentRegistry()
  const bindings = new InMemoryConversationBindingStore()
  let next = 0
  const resolver = new SessionResolver({
    agents,
    bindings,
    persistence: overrides.persistence,
    freshSessionId: () => `session-${++next}`,
  })
  return { agents, bindings, resolver }
}

describe('session resolver', () => {
  it('reuses a live bound agent without creating or resuming', async () => {
    const { agents, bindings, resolver } = setup()
    await bindings.bind('telegram:chat:1', 'session-live')
    agents.live.set('session-live', new FakeAgent('session-live'))

    const resolved = await resolver.resolve('telegram:chat:1', null, false)

    expect(resolved.owned).toBe(false)
    expect(resolved.agent.id).toBe('session-live')
    expect(agents.created).toHaveLength(0)
    expect(agents.resumed).toHaveLength(0)
  })

  it('resumes a persisted but not-live session (owned)', async () => {
    const { agents, bindings, resolver } = setup({
      persistence: new FakeSessionPersistence([{ id: 'session-old', cwd: '/work/session-old' }]),
    })
    await bindings.bind('telegram:chat:1', 'session-old')

    const resolved = await resolver.resolve('telegram:chat:1', null, false)

    expect(resolved.owned).toBe(true)
    expect(resolved.handle).toBeDefined()
    expect(resolved.agent.id).toBe('session-old')
    expect(agents.resumed).toHaveLength(1)
    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-old')
  })

  it('does not resume a persisted session lacking a cwd header — creates a fresh session and rebinds', async () => {
    const { agents, bindings, resolver } = setup({
      persistence: new FakeSessionPersistence([{ id: 'session-old' }]),
    })
    await bindings.bind('telegram:chat:1', 'session-old')

    const resolved = await resolver.resolve('telegram:chat:1', null, false)

    expect(resolved.owned).toBe(true)
    expect(resolved.agent.id).toBe('session-1')
    expect(agents.resumed).toHaveLength(0)
    expect(agents.created).toHaveLength(1)
    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-1')
  })

  it('creates a fresh session when neither live nor persisted (owned)', async () => {
    const { agents, bindings, resolver } = setup({ persistence: new FakeSessionPersistence([]) })

    const resolved = await resolver.resolve('telegram:chat:1', null, false)

    expect(resolved.owned).toBe(true)
    expect(resolved.agent.id).toBe('session-1')
    expect(agents.created).toHaveLength(1)
    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-1')
  })

  it('rejects an explicit mismatched session_id instead of overwriting the binding', async () => {
    const { bindings, resolver } = setup()
    await bindings.bind('telegram:chat:1', 'session-a')

    await expect(resolver.resolve('telegram:chat:1', 'session-b', false)).rejects.toBeInstanceOf(BindingConflictError)
    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-a')
  })

  it('new_session creates a fresh session and rebinds without disposing the old one', async () => {
    const { agents, bindings, resolver } = setup()
    await bindings.bind('telegram:chat:1', 'session-old')
    agents.live.set('session-old', new FakeAgent('session-old'))

    const resolved = await resolver.resolve('telegram:chat:1', null, true)

    expect(resolved.owned).toBe(true)
    expect(resolved.agent.id).toBe('session-1')
    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-1')
    expect(agents.live.has('session-old')).toBe(true)
  })

  it('renew inherits the existing bound session id while creating and rebinding a fresh session', async () => {
    const { agents, bindings, resolver } = setup()
    await bindings.bind('telegram:chat:1', 'session-old')

    const renewed = await resolver.renew('telegram:chat:1')

    expect(renewed.sessionId).toBe('session-1')
    expect(renewed.handle.agent.id).toBe('session-1')
    expect(agents.created).toHaveLength(1)
    expect(agents.created[0]?.sourceSessionId).toBe('session-old')
    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-1')
  })

  it('renew without an existing binding passes no source session id', async () => {
    const { agents, bindings, resolver } = setup()

    await resolver.renew('telegram:chat:1', 'bot-operator')

    expect(agents.created[0]?.title).toBe('bot-operator')
    expect(agents.created[0]?.sourceSessionId).toBeUndefined()
    expect(await bindings.sessionIdFor('telegram:chat:1')).toBe('session-1')
  })
})
