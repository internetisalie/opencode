import { expect, test } from "bun:test"
import { PluginHttp } from "@opencode/core/plugin/http"
import { Effect, Exit, Scope } from "effect"

test("HTTP registrations are disposed with the plugin scope", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const first = yield* PluginHttp.Service
        const scope = yield* Scope.make()
        const second = yield* Scope.make()
        const handler = { fetch: () => new Response("first") }
        yield* first.register("example", handler).pipe(Effect.provideService(Scope.Scope, scope))
        yield* first.register("example", handler).pipe(Effect.provideService(Scope.Scope, second))
        expect(yield* first.get("example")).toBe(handler)

        yield* Scope.close(scope, Exit.void)
        expect(yield* first.get("example")).toBe(handler)
        yield* Scope.close(second, Exit.void)
        expect(yield* first.get("example")).toBeUndefined()
      }).pipe(Effect.provide(PluginHttp.layer)),
    ),
  )
})
