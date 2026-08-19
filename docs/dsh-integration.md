# DSH Core Integration — registerRouter seam

Summary

This connector depends on a small, local, additive seam applied to the DeepSeek
Harness (uncommitted patch in the local harness checkout). The seam exposes a
router registration API for the UserQuestionService so the connector can route
interactive questions for `bot-*` sessions to Redis while leaving the Web
provider as the default for non-bot sessions.

1) What the seam is — exact API

The local patch adds the following (type-only) API to:
packages/interaction/user-questions/src/index.ts

```ts
// packages/interaction/user-questions/src/index.ts (local patch)
interface UserQuestionRouter {
  ask(request: AskUserQuestionRequest, next: (req: AskUserQuestionRequest) => Promise<AskUserQuestionAnswer>): Promise<AskUserQuestionAnswer>
}
registerRouter(router: UserQuestionRouter): () => void
```

Behavioral note: next() forwards the request to the next router and ultimately
forwards to the single Web provider. The seam is additive: it does not replace
or remove the existing single-provider model, it merely allows registering a
router chain before the Web provider.

2) Why it exists

Upstream UserQuestionService supports exactly one global provider with no
routing seam. A second provider registration is rejected (`DUPLICATE_PROVIDER`)
and there is no getter to inspect or delegate to the Web provider. The connector
needs to route only a subset of sessions (those whose session title starts with
`bot-`) to Redis/Telegram while leaving all other interactive questions to the
existing Web provider. The registerRouter seam is the minimal additive fix that
lets the connector decide per-interaction whether to handle the question or
call next() and let Web handle it.

3) How the connector consumes it

- The connector's plugin `inject` includes these keys:

  ['agents', 'userQuestions', 'sessionTitle', 'approval']

- During initialization the connector registers one router:

```ts
ctx.userQuestions.registerRouter({
  async ask(request, next) {
    const title = await ctx.sessionTitle.get(request.agent.session)?.title
    if (title?.startsWith('bot-')) {
      // route to Redis (emit question_request), await inbound question_answer
      // resolve and return AskUserQuestionAnswer
    }
    return next(request)
  }
})
```

- Routing rule: if session title starts with `bot-` (exact prefix match) route
  to Redis; otherwise call next(request) exactly once so the Web provider runs.

- The connector also registers approval routing using the same seam (approvals
  are implemented as a special question id `approval` — see PROTOCOL.md).

4) Breaking-change detection (what to grep for after a DSH upgrade)

If a future Harness upgrade removes or renames the seam, these are the exact
failure signatures to look for:

- Build-time TypeScript error
  - "Property 'registerRouter' does not exist on type 'UserQuestionService'"

- Runtime (silent/missed registration)
  - The connector's startup try/catch logs: "user-questions integration not registered"

- Runtime (approvals routing)
  - The connector logs: "approval routing not registered" or "skipping approval routing"

Re-apply note: re-add the seam to the Harness checkout, then rebuild the
Harness's host library with:

  pnpm run build:lib:host  # runs tsdown; necessary to refresh lib/index.js

DO NOT rely on `tsc -b` alone — that refreshes only `lib/types/` and leaves
`lib/index.js` stale. After re-apply + build restart `dsh web`.


# Operational checklist (quick)

- If connector interactive questions stop reaching Telegram but Web still
  receives in-browser prompts: check that the seam is present and that the
  connector's router registered successfully (see logs above).

- If the build fails with the TS Property error: reapply the local patch to the
  Harness `packages/interaction/user-questions` and run `pnpm run build:lib:host`.

- If approvals fail to escalate to Telegram: check for the "approval routing not registered" log and follow the same re-apply + tsdown rebuild steps.


(End of DSH integration notes)
