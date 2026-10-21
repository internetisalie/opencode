import { describe, expect } from "bun:test"
import { Effect } from "effect"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Npm } from "@opencode-ai/core/npm"
import path from "path"
import { pathToFileURL } from "url"
import { Account } from "../../src/account/account"
import { Auth } from "../../src/auth"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { Plugin } from "../../src/plugin/index"
import { Server } from "../../src/server/server"
import { Global } from "@opencode-ai/core/global"

import { TestInstance, tmpdirScoped } from "../fixture/fixture"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { markPluginDependenciesReady } from "../fixture/plugin"
import { AccountTest } from "../fake/account"
import { AuthTest } from "../fake/auth"
import { NpmTest } from "../fake/npm"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Plugin.node, CrossSpawnSpawner.node]), [
    [Auth.node, AuthTest.empty],
    [Account.node, AccountTest.empty],
    [Npm.node, NpmTest.noop],
    [RuntimeFlags.node, RuntimeFlags.layer({ disableDefaultPlugins: true })],
  ]),
)
const systemHook = "experimental.chat.system.transform"

it.live("R2-L1 served plugin SDK fails fast on its own dispose hook re-entry", () => Effect.gen(function* () {
  yield* Effect.promise(() => markPluginDependenciesReady(Global.Path.config))
  const directory = yield* tmpdirScoped({ git: true })
  const file = path.join(directory, "served-plugin.ts")
  const warm = path.join(directory, "served-warm.json")
  const outcome = path.join(directory, "served-dispose.json")
  yield* Effect.promise(() => Bun.write(file, [
    "export default async ({ client, directory, serverUrl }) => {",
    "  let warmed = false",
    "  return {",
    "    event: async ({ event }) => {",
    "      if (event.type !== 'session.created' || warmed) return",
    "      warmed = true",
    "      await client.session.list({ directory }, { throwOnError: true })",
    `      await Bun.write(${JSON.stringify(warm)}, JSON.stringify({ url: serverUrl.toString() }))`,
    "    },",
    "    dispose: async () => {",
    "      const start = performance.now()",
    "      let result = 'served'",
    "      try { await client.session.list({ directory }, { throwOnError: true }) }",
    "      catch (error) { result = String(error) }",
    `      await Bun.write(${JSON.stringify(outcome)}, JSON.stringify({ result, elapsed: performance.now() - start }))`,
    "    },",
    "  }",
    "}",
  ].join("\n")))
  yield* Effect.promise(() => Bun.write(path.join(directory, "opencode.json"), JSON.stringify({
    plugin: [pathToFileURL(file).href], formatter: false, lsp: false,
  })))
  const previous = { ttl: process.env.OPENCODE_INSTANCE_IDLE_TTL_MS, plugins: process.env.OPENCODE_DISABLE_DEFAULT_PLUGINS }
  process.env.OPENCODE_INSTANCE_IDLE_TTL_MS = "2000"
  process.env.OPENCODE_DISABLE_DEFAULT_PLUGINS = "true"
  yield* Effect.addFinalizer(() => Effect.sync(() => {
    if (previous.ttl === undefined) delete process.env.OPENCODE_INSTANCE_IDLE_TTL_MS
    else process.env.OPENCODE_INSTANCE_IDLE_TTL_MS = previous.ttl
    if (previous.plugins === undefined) delete process.env.OPENCODE_DISABLE_DEFAULT_PLUGINS
    else process.env.OPENCODE_DISABLE_DEFAULT_PLUGINS = previous.plugins
  }))
  const listener = yield* Effect.acquireRelease(
    Effect.promise(() => Server.listen({ hostname: "127.0.0.1", port: 0 })),
    (listener) => Effect.promise(() => listener.stop(true)),
  )
  expect(Server.url?.toString()).toBe(listener.url.toString())
  const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
  const created = yield* Effect.promise(() => fetch(new URL("/session", listener.url), {
    method: "POST", headers, body: JSON.stringify({ title: "served plugin re-entry" }),
  }))
  expect(created.status).toBe(200)
  yield* Effect.promise(() => created.json())
  yield* pollWithTimeout(Effect.promise(() => Bun.file(warm).exists()).pipe(Effect.map((exists) => exists ? true : undefined)), "plugin's normal HTTP SDK call did not finish")
  expect(yield* Effect.promise(() => Bun.file(warm).json())).toEqual({ url: listener.url.toString() })
  const start = performance.now()
  const disposed = yield* Effect.promise(() => fetch(new URL("/instance/dispose", listener.url), { method: "POST", headers }))
  expect(disposed.status).toBe(200)
  yield* Effect.promise(() => disposed.json())
  yield* pollWithTimeout(Effect.promise(() => Bun.file(outcome).exists()).pipe(Effect.map((exists) => exists ? true : undefined)), "plugin dispose hook did not complete", "8 seconds")
  const result = yield* Effect.promise(() => Bun.file(outcome).json() as Promise<{ result: string; elapsed: number }>)
  expect(result.result).toContain("reentrant")
  expect(result.elapsed).toBeLessThan(1000)
  expect(performance.now() - start).toBeLessThan(1500)
}))

function withProject<A, E, R>(source: string, self: Effect.Effect<A, E, R>) {
  return Effect.gen(function* () {
    const test = yield* TestInstance
    const file = path.join(test.directory, "plugin.ts")
    yield* Effect.all(
      [
        Effect.promise(() => Bun.write(file, source)),
        Effect.promise(() =>
          Bun.write(
            path.join(test.directory, "opencode.json"),
            JSON.stringify(
              {
                $schema: "https://opencode.ai/config.json",
                plugin: [pathToFileURL(file).href],
              },
              null,
              2,
            ),
          ),
        ),
      ],
      { discard: true, concurrency: 2 },
    )
    return yield* self
  })
}

const triggerSystemTransform = Effect.fn("PluginTriggerTest.triggerSystemTransform")(function* () {
  const plugin = yield* Plugin.Service
  const out = { system: [] as string[] }
  yield* plugin.trigger(
    systemHook,
    {
      model: {
        providerID: ProviderV2.ID.anthropic,
        modelID: ModelV2.ID.make("claude-sonnet-4-6"),
      },
    },
    out,
  )
  return out.system
})

describe("plugin.trigger", () => {
  it.instance("runs synchronous hooks without crashing", () =>
    withProject(
      [
        "export default async () => ({",
        `  ${JSON.stringify(systemHook)}: (_input, output) => {`,
        '    output.system.unshift("sync")',
        "  },",
        "})",
        "",
      ].join("\n"),
      Effect.gen(function* () {
        expect(yield* triggerSystemTransform()).toEqual(["sync"])
      }),
    ),
  )

  it.instance("awaits asynchronous hooks", () =>
    withProject(
      [
        "export default async () => ({",
        `  ${JSON.stringify(systemHook)}: async (_input, output) => {`,
        "    await Bun.sleep(1)",
        '    output.system.unshift("async")',
        "  },",
        "})",
        "",
      ].join("\n"),
      Effect.gen(function* () {
        expect(yield* triggerSystemTransform()).toEqual(["async"])
      }),
    ),
  )
})
