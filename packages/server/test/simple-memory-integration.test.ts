import { expect, test } from "bun:test"
import { pathToFileURL } from "node:url"
import { Effect, Layer } from "effect"
import { SdkPlugins } from "@opencode/core/plugin/sdk"
import { PluginPromise } from "@opencode/core/plugin/promise"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const source = process.env.SIMPLE_MEMORY_PLUGIN_PATH

if (!source) {
  test.skip("Simple Memory host integration (set SIMPLE_MEMORY_PLUGIN_PATH)", () => {})
} else {
  it.live("activates Simple Memory and serves its scoped HTTP route", () =>
    Effect.gen(function* () {
      const location = yield* tmpdirScoped()
      const global = yield* tmpdirScoped()
      const module = yield* Effect.promise(() => import(pathToFileURL(source).href))
      const plugin = module.createV2MemoryPlugin(() => global.path)
      const store = SdkPlugins.Service.of({
        register: () => Effect.void,
        all: () => [{ ...PluginPromise.fromPromise(plugin), revision: "test", source: { type: "sdk" as const } }],
      })
      const overrides: LayerNode.Replacements = [SdkPlugins.node.replace(Layer.succeed(SdkPlugins.Service, store))]
      const handler = yield* ServerFetch.make(
        {
          app: { version: "test-version" },
          database: { path: ":memory:" },
          config: { directory: location.path, project: false },
          models: { fetch: false },
          fs: { filewatcher: false },
          password: "secret",
        },
        { overrides },
      )
      const route = "http://opencode.local/api/plugins/opencode-simple-memory/memories?scope=project"
      const headers = {
        authorization: `Basic ${btoa("opencode:secret")}`,
        "x-opencode-directory": location.path,
      }
      const denied = yield* Effect.promise(() => handler(new Request(route)))
      expect(denied.status).toBe(401)
      const response = yield* Effect.promise(() => handler(new Request(route, { headers })))
      const body = yield* Effect.promise(() => response.json())
      expect(response.status, JSON.stringify(body)).toBe(200)
      expect(body).toEqual({ memories: [] })
    }),
  )
}
