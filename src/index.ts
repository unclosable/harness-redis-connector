/**
 * Package entry: re-exports the function-plugin surface (`name` / `inject` /
 * `Config` / `apply`) for the harness Loader.
 *
 * @module harness-redis-connector
 */

export { name, inject, Config, apply } from './plugin-entry.js'
export type { Config as HarnessRedisConnectorConfig } from './plugin-entry.js'
