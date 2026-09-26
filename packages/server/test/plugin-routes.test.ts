import { expect } from "bun:test"
import { SdkPlugins } from "@opencode/core/plugin/sdk"
import { PluginPromise } from "@opencode/core/plugin/promise"
import { define } from "@opencode/plugin/effect/plugin"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Effect, Layer } from "effect"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const options = {
  app: { version: "test-version" },
  database: { path: ":memory:" },
  config: { project: false },
  models: { fetch: false },
  fs: { filewatcher: false },
} as const

it.live("authenticates before activating location plugins and forwards HTTP requests", () =>
  Effect.gen(function* () {
    const location = yield* tmpdirScoped()
    const activations: string[] = []
    const signals: AbortSignal[] = []
    const disposePromise = new Map<string, () => Promise<void>>()
    const plugin = define({
      id: "test-http",
      effect: Effect.fn(function* (ctx) {
        activations.push(ctx.location.directory)
        yield* ctx.http.register({
          fetch: async (request) => {
            const url = new URL(request.url)
            if (url.pathname === "/signal") signals.push(request.signal)
            if (url.pathname === "/stream")
              return new Response(
                new ReadableStream({
                  start(controller) {
                    controller.enqueue(new TextEncoder().encode("first"))
                  },
                }),
                { headers: { "content-type": "text/plain" } },
              )
            const body = request.method === "POST" ? await request.text() : ""
            return Response.json(
              {
                directory: ctx.location.directory,
                method: request.method,
                path: url.pathname,
                query: Array.from(url.searchParams.entries()),
                body,
              },
              { status: 201, headers: { "x-plugin-response": "preserved" } },
            )
          },
        })
      }),
    })
    const store = SdkPlugins.Service.of({
      register: () => Effect.void,
      all: () => [
        { ...plugin, revision: "test", source: { type: "sdk" as const } },
        {
          ...define({
            id: "@scope/plugin",
            effect: (ctx) => ctx.http.register({ fetch: () => new Response("scoped") }).pipe(Effect.asVoid),
          }),
          revision: "test",
          source: { type: "sdk" as const },
        },
        {
          ...PluginPromise.fromPromise({
            id: "promise-http",
            async setup(ctx) {
              const registration = await ctx.http.register({ fetch: () => new Response("promise") })
              disposePromise.set(ctx.location.directory, registration.dispose)
            },
          }),
          revision: "test",
          source: { type: "sdk" as const },
        },
      ],
    })
    const overrides: LayerNode.Replacements = [SdkPlugins.node.replace(Layer.succeed(SdkPlugins.Service, store))]
    const handler = yield* ServerFetch.make(
      { ...options, config: { ...options.config, directory: location.path }, password: "secret" },
      { overrides },
    )
    const route = "http://opencode.local/api/plugins/test-http/memories?limit=2&limit=3"
    const denied = yield* Effect.promise(() =>
      handler(new Request(route, { headers: { "x-opencode-directory": location.path } })),
    )
    expect(denied.status).toBe(401)
    expect(activations).toEqual([])

    const response = yield* Effect.promise(() =>
      handler(
        new Request(route, {
          method: "POST",
          headers: {
            authorization: `Basic ${btoa("opencode:secret")}`,
            "x-opencode-directory": location.path,
            "content-type": "text/plain",
          },
          body: "hello",
        }),
      ),
    )
    expect(response.status).toBe(201)
    expect(response.headers.get("x-plugin-response")).toBe("preserved")
    expect(yield* Effect.promise(() => response.json())).toEqual({
      directory: location.path,
      method: "POST",
      path: "/memories",
      query: [
        ["limit", "2"],
        ["limit", "3"],
      ],
      body: "hello",
    })
    expect(activations).toEqual([location.path])

    const other = yield* tmpdirScoped()
    const isolated = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/plugins/test-http", {
          headers: {
            authorization: `Basic ${btoa("opencode:secret")}`,
            "x-opencode-directory": other.path,
          },
        }),
      ),
    )
    expect(isolated.status).toBe(201)
    expect(yield* Effect.promise(() => isolated.json())).toMatchObject({ directory: other.path, path: "/" })
    expect(activations).toEqual([location.path, other.path])

    const scoped = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/plugins/%40scope%2Fplugin/nested", {
          headers: {
            authorization: `Basic ${btoa("opencode:secret")}`,
            "x-opencode-directory": location.path,
          },
        }),
      ),
    )
    expect(scoped.status).toBe(200)
    expect(yield* Effect.promise(() => scoped.text())).toBe("scoped")

    const promiseRoute = "http://opencode.local/api/plugins/promise-http/memories"
    const promiseHeaders = {
      authorization: `Basic ${btoa("opencode:secret")}`,
      "x-opencode-directory": location.path,
    }
    const promised = yield* Effect.promise(() => handler(new Request(promiseRoute, { headers: promiseHeaders })))
    expect(promised.status).toBe(200)
    expect(yield* Effect.promise(() => promised.text())).toBe("promise")
    expect(disposePromise.has(location.path)).toBe(true)
    yield* Effect.promise(() => disposePromise.get(location.path)!())
    const disposed = yield* Effect.promise(() => handler(new Request(promiseRoute, { headers: promiseHeaders })))
    expect(disposed.status).toBe(404)
    const otherPromise = yield* Effect.promise(() =>
      handler(new Request(promiseRoute, { headers: { ...promiseHeaders, "x-opencode-directory": other.path } })),
    )
    expect(yield* Effect.promise(() => otherPromise.text())).toBe("promise")

    const controller = new AbortController()
    const observed = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/plugins/test-http/signal", {
          signal: controller.signal,
          headers: {
            authorization: `Basic ${btoa("opencode:secret")}`,
            "x-opencode-directory": location.path,
          },
        }),
      ),
    )
    expect(observed.status).toBe(201)
    expect(signals).toHaveLength(1)
    controller.abort()
    expect(signals[0].aborted).toBe(true)

    const stream = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/plugins/test-http/stream", {
          headers: {
            authorization: `Basic ${btoa("opencode:secret")}`,
            "x-opencode-directory": location.path,
          },
        }),
      ),
    ).pipe(Effect.timeout("2 seconds"))
    expect(stream.status).toBe(200)
    const reader = stream.body?.getReader()
    expect(reader).toBeDefined()
    const first = yield* Effect.promise(() => reader!.read()).pipe(Effect.timeout("2 seconds"))
    expect(new TextDecoder().decode(first.value)).toBe("first")
    yield* Effect.promise(() => reader!.cancel())
  }),
)
