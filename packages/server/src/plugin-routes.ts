import { Plugin } from "@opencode/core/plugin"
import { PluginHttp } from "@opencode/core/plugin/http"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { Context, Effect, Option } from "effect"
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { IncomingMessage } from "node:http"
import { Readable } from "node:stream"
import { ServerAuth } from "./auth"
import { requestRef } from "./location"
import { authorizedRequest, unauthorizedResponse } from "./middleware/authorization"

export function pluginRoutes(
  locations: Context.Service.Shape<typeof LocationServiceMap.Service>,
  password: Option.Option<string>,
) {
  const auth = ServerAuth.Config.of({ password, username: "opencode" })
  return HttpRouter.use((router) =>
    router.add("*", "/api/plugins/:pluginID/*", () =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        // A plugin can execute arbitrary code during Location activation. Authenticate first.
        if (ServerAuth.required(auth) && !(yield* authorizedRequest(request, auth)))
          return unauthorizedResponse(request)

        const route = yield* HttpRouter.RouteContext
        const id = route.params.pluginID
        if (!id) return HttpServerResponse.empty({ status: 404 })
        return yield* Effect.gen(function* () {
          const plugins = yield* Plugin.Service
          yield* plugins.awaitActivation
          const http = yield* PluginHttp.Service
          const handler = yield* http.get(id)
          if (!handler) return HttpServerResponse.empty({ status: 404 })

          const url = new URL(request.url, `http://${request.headers.host ?? "localhost"}`)
          url.pathname = url.pathname.replace(/^\/api\/plugins\/[^/]+/, "") || "/"
          const source = request.source
          const body =
            request.method === "GET" || request.method === "HEAD"
              ? undefined
              : source instanceof Request
                ? (source.body ?? undefined)
                : source instanceof Readable
                  ? (Readable.toWeb(source) as unknown as ReadableStream<Uint8Array>)
                  : undefined
          const controller = source instanceof IncomingMessage ? new AbortController() : undefined
          const abort = () => controller?.abort()
          if (source instanceof IncomingMessage) {
            if (source.aborted) abort()
            source.on("aborted", abort)
            yield* Effect.addFinalizer(() => Effect.sync(() => source.off("aborted", abort)))
          }
          const input = new Request(url, {
            method: request.method,
            headers: request.headers,
            body,
            signal: source instanceof Request ? source.signal : controller?.signal,
            duplex: body ? "half" : undefined,
          } as RequestInit & { duplex?: "half" })
          const response = yield* Effect.promise(async () => {
            const result = await handler.fetch(input)
            if (!(result instanceof Response)) throw new TypeError(`Plugin ${id} returned a non-Response`)
            return result
          })
          return HttpServerResponse.fromWeb(response)
        }).pipe(Effect.provide(locations.get(requestRef(request))), Effect.scoped)
      }),
    ),
  )
}
