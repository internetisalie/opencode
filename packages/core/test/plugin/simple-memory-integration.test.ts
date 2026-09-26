import { expect, test } from "bun:test"
import { pathToFileURL } from "node:url"
import { Duration, Effect, Layer, LayerMap } from "effect"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { Watcher } from "@opencode/core/filesystem/watcher"
import { Instance } from "@opencode/core/instance"
import { Location } from "@opencode/core/location"
import { LocationServiceMap } from "@opencode/core/location-services"
import { Plugin } from "@opencode/core/plugin"
import { PluginPromise } from "@opencode/core/plugin/promise"
import { SdkPlugins } from "@opencode/core/plugin/sdk"
import { Session } from "@opencode/core/session"
import { Tool } from "@opencode/core/tool"
import { AbsolutePath } from "@opencode/core/schema"
import { tempGlobalLayer } from "../fixture/global"
import { offlineModels } from "../fixture/models"
import { tmpdirScoped } from "../fixture/tmpdir"
import { testEffect } from "../lib/effect"
import { executeTool, toolIdentity } from "../lib/tool"

const source = process.env.SIMPLE_MEMORY_PLUGIN_PATH

if (!source) {
  test.skip("Simple Memory tool integration (set SIMPLE_MEMORY_PLUGIN_PATH)", () => {})
} else {
  const instances = Layer.effect(
    LocationServiceMap.Service,
    Effect.gen(function* () {
      const watcher = yield* Watcher.Test
      const map = yield* LayerMap.make(
        (ref: Location.Ref) => Instance.layer(ref, { discovery: false, replacements: bindings }),
        {
          idleTimeToLive: Duration.infinity,
        },
      )
      const bindings: LayerNode.Replacements = [
        Global.node.replace(tempGlobalLayer),
        offlineModels,
        Watcher.node.replace(Layer.succeed(Watcher.Service, watcher)),
        LocationServiceMap.node.replace(Layer.succeed(LocationServiceMap.Service, map)),
        Instance.node.replace(
          Layer.succeed(Instance.Service, { provide: (session) => Effect.provide(map.get(session.location)) }),
        ),
      ]
      return map
    }),
  ).pipe(Layer.provide(Watcher.testLayer))
  const it = testEffect(
    AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, SdkPlugins.node, LocationServiceMap.node]), [
      Global.node.replace(tempGlobalLayer),
      offlineModels,
      LocationServiceMap.node.replace(instances),
    ]).pipe(Layer.provideMerge(Watcher.testLayer)),
  )
  it.live("registers the Simple Memory tool catalog on a disposable v2 Location", () =>
    Effect.gen(function* () {
      const directory = yield* tmpdirScoped()
      const global = yield* tmpdirScoped()
      const module = yield* Effect.promise(() => import(pathToFileURL(source).href))
      const sdk = yield* SdkPlugins.Service
      yield* sdk.register(PluginPromise.fromPromise(module.createV2MemoryPlugin(() => global.path)))
      const locations = yield* LocationServiceMap.Service
      yield* Effect.gen(function* () {
        const plugins = yield* Plugin.Service
        yield* plugins.awaitActivation
        expect((yield* plugins.list()).find((item) => item.id === "opencode-simple-memory")?.state.status).toBe(
          "active",
        )
        const names = (yield* (yield* Tool.Service).list()).map((item) => item.name)
        expect(names).toContain("memory_write")
        expect(names).toContain("memory_read")
        expect(names).toContain("memory_recall")
        expect(names.filter((name) => name.startsWith("memory_"))).toHaveLength(11)
        const tools = yield* Tool.Service
        const sessionID = Session.ID.make("ses_simple_memory_test")
        const written = yield* executeTool(tools, {
          sessionID,
          ...toolIdentity,
          call: {
            type: "tool-call",
            id: "call-memory-write",
            name: "memory_write",
            input: { scope: "project", title: "Integration", type: "context", content: "v2 plugin works" },
          },
        })
        expect(written.status).toBe("completed")
        const text = written.content?.find((item) => item.type === "text")
        expect(text?.text).toContain("v2 plugin works")
      }).pipe(
        Effect.scoped,
        Effect.provide(locations.get(Location.Ref.make({ directory: AbsolutePath.make(directory.path) }))),
      )
    }),
  )
}
