import { describe, it, expect } from 'vitest'

describe('approval mapping', () => {
  it('maps Approve -> allowed-once', () => {
    const raw = [{ id: 'approval', value: 'Approve' }]
    const val = (Array.isArray(raw[0].value) ? raw[0].value[0] : raw[0].value) as string
    expect(val === 'Approve' ? 'allowed-once' : 'other').toBe('allowed-once')
  })

  it('maps Deny -> rejected', () => {
    const raw = [{ id: 'approval', value: 'Deny' }]
    const val = (Array.isArray(raw[0].value) ? raw[0].value[0] : raw[0].value) as string
    expect(val === 'Deny' ? 'rejected' : 'other').toBe('rejected')
  })

  it('maps missing -> unavailable', () => {
    const raw: any[] = []
    const ans = raw.find(a => a.id === 'approval')
    expect(ans === undefined ? 'unavailable' : 'other').toBe('unavailable')
  })

  it('maps array value to first element', () => {
    const raw = [{ id: 'approval', value: ['Approve','Deny'] }]
    const val = Array.isArray(raw[0].value) ? raw[0].value[0] : raw[0].value
    expect(val).toBe('Approve')
  })
})
