# DSH Core Integration — agent-scoped user-questions waterfall

Summary

This connector consumes the native, agent-scoped `user-questions/request`
Cordis waterfall shipped by `@deepseek-ai/dsh-user-questions` (dsh ≥ 0.1.2).
There is no local harness patch and no `registerRouter` seam anymore: upstream
removed the single-provider UserQuestion model and replaced it with a scoped
answerer waterfall. The connector registers one untagged listener that routes
interactive questions for `bot-*` sessions to Redis while `next()` leaves the
Web answerer as the default for everything else.

1) The contract — exact API

The upstream service asks through a scoped Cordis waterfall
(`packages/interaction/user-questions/src/index.ts`):

```ts
this.ctx.waterfall(
  scopeTarget(agent, agent),
  'user-questions/request',
  { ...request, agent },
  noAnswerer,
)
```

The event is declared on the Cordis `Events` interface (same package, `types.ts`):

```ts
'user-questions/request'(
  this: Scoped<Agent>,
  request: AskUserQuestionRequestEvent,
  next: () => Promise<AskUserQuestionAnswer>,
): Promise<AskUserQuestionAnswer>
```

- The waterfall is agent-scoped via `scopeTarget(agent, agent)` from
  `@deepseek-ai/dsh-scope`. Admission rule: **an untagged listener — one
  registered with plain `ctx.on('user-questions/request', ...)` on an
  unscoped context — is admitted for every agent** (the scope filter returns
  `true` when the listener's context carries no scope tag). Tagged listeners
  receive only events dispatched to their scope key or one of its ancestors.
- `next` takes **no argument**: `next: () => Promise<AskUserQuestionAnswer>`.
  A listener that wants to fall through calls `next()`; the value returned is
  the next listener's answer, or the built-in `noAnswerer` rejection
  (`UserQuestionError`, code `NO_PROVIDER`) when no listener claims the
  request.
- Returning an answer from a listener claims the request and vetoes the rest
  of the chain (Cordis waterfall semantics: not calling `next()` stops it).

2) Why it changed

Upstream 0.1.1 supported exactly one global UserQuestion provider with no
routing seam (a second registration was rejected with `DUPLICATE_PROVIDER`),
so the connector previously relied on a small, local, additive
`registerRouter` patch on `UserQuestionService`. That seam is gone: since
0.1.2 the ask path is the agent-scoped waterfall above, which natively
supports multiple listeners. The connector therefore needs no harness patch —
an untagged listener is admitted for every agent and decides per interaction
whether to answer via Redis or delegate with `next()`.

3) How the connector consumes it

- The connector's plugin `inject` includes these keys:

  ['agents', 'userQuestions', 'sessionTitle', 'approval']

  (`userQuestions` remains a real service and is kept for dependency wiring;
  the listener itself is a plain event registration and needs no service
  handle).

- During initialization the connector registers one untagged listener:

```ts
const disposer = ctx.on(
  'user-questions/request',
  async (request: AskUserQuestionRequest, next: () => Promise<AskUserQuestionAnswer>) => {
    const agent = request.agent as unknown as Agent | undefined
    if (agent == null) return next()
    const session = agent.session
    const title = sessionTitleSvc.get(session)?.title
    if (!shouldRouteToRedis(title)) return next()
    const sessionId = String(session.id)
    if (request.signal?.aborted) throw new Error('ask aborted')
    // route to Redis: conversationId lookup, outbound question_request
    // envelope, pending entry, sender.send, await the inbound answer
    // and return the AskUserQuestionAnswer
  },
)
ctx.effect(() => disposer, 'harness-redis-connector.user-questions-dispose')
```

- Routing rule: if the session title starts with `bot-` (exact prefix match)
  route to Redis; otherwise call `next()` exactly once so the Web answerer
  runs.
- The listener is untagged, so it is admitted for every agent (see §1).
- The connector also registers approval routing on the `approval/request`
  waterfall (approvals are implemented as a special question id `approval` —
  see PROTOCOL.md).

4) Breaking-change detection (what to grep for after a DSH upgrade)

The integration is now native — there is no compile-time seam to re-apply. The
only failure signatures are runtime logs:

- Runtime (silent/missed registration)
  - The connector's startup try/catch logs: "user-questions integration not registered"

- Runtime (approvals routing)
  - The connector logs: "approval routing not registered" or "skipping approval routing"

- If the upstream event signature changes (for example `next` gains an
  argument again, the event name moves, or the request type changes), the
  listener body will no longer typecheck — that is the compile-time signal.

Re-apply note: nothing to re-apply in the Harness checkout for this
integration. After a Harness upgrade rebuild the host library with:

  pnpm run build:lib   # runs tsdown; refreshes lib/index.js for the host packages

DO NOT rely on `tsc -b` alone — that refreshes only `lib/types/` and leaves
`lib/index.js` stale. After the rebuild restart `dsh web`.


# Operational checklist (quick)

- If connector interactive questions stop reaching Telegram but Web still
  receives in-browser prompts: check that the connector's listener registered
  successfully (see logs above) and that the session title still starts with
  `bot-`.

- If the listener fails to register, the startup try/catch logs
  "user-questions integration not registered" and questions fall back to Web.

- If approvals fail to escalate to Telegram: check for the "approval routing not registered" log.


(End of DSH integration notes)
