/**
 * Shared, dependency-free helpers used across the connector modules.
 * @module harness-redis-connector/util
 */

/** The logger surface the connector modules need (satisfied by `ctx.logger`). */
export interface LoggerLike {
  debug?(...args: unknown[]): void
  warn(...args: unknown[]): void
  error(...args: unknown[]): void
  info(...args: unknown[]): void
}

/**
 * Render any thrown value as a single-line diagnostic.
 * @param error - the thrown value.
 * @returns its message when it has one, otherwise a JSON/string rendering.
 */
export function describeError(error: unknown): string {
  if (error instanceof Error) return error.message
  if (typeof error === 'string') return error
  if (typeof error === 'object' && error !== null) {
    const message = (error as { message?: unknown }).message
    if (typeof message === 'string') return message
  }
  try {
    return JSON.stringify(error)
  } catch {
    return String(error)
  }
}

/**
 * Resolve after `ms`, aborting early on `signal`.
 * @param ms - milliseconds to sleep.
 * @param signal - optional abort signal.
 */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise(resolve => {
    const timer = setTimeout(() => { cleanup(); resolve() }, ms)
    const cleanup = (): void => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', onAbort)
    }
    const onAbort = (): void => { cleanup(); resolve() }
    signal?.addEventListener('abort', onAbort, { once: true })
  })
}
