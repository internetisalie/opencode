export * as PluginHttp from "./http.js"

import type { Handler } from "@opencode/plugin/effect/http"
import type { Registration } from "@opencode/plugin/effect/registration"
import { Context, Effect, Layer, Scope } from "effect"
import { makeLocationNode } from "@opencode/util/effect/app-node"

export interface Interface {
  readonly register: (id: string, handler: Handler) => Effect.Effect<Registration, never, Scope.Scope>
  readonly get: (id: string) => Effect.Effect<Handler | undefined>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/PluginHttp") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const handlers = new Map<string, { readonly handler: Handler }[]>()
    return Service.of({
      register: (id, handler) =>
        Effect.gen(function* () {
          const scope = yield* Scope.Scope
          const entry = { handler }
          handlers.set(id, [...(handlers.get(id) ?? []), entry])
          const dispose = Effect.sync(() => {
            const next = handlers.get(id)?.filter((current) => current !== entry) ?? []
            if (next.length) handlers.set(id, next)
            else handlers.delete(id)
          })
          yield* Scope.addFinalizer(scope, dispose)
          return { dispose }
        }),
      get: (id) => Effect.sync(() => handlers.get(id)?.at(-1)?.handler),
    })
  }),
)

export const node = makeLocationNode({ service: Service, layer, deps: [] })
