import { expect, test } from "bun:test"
import { pathToFileURL } from "node:url"
import { Effect } from "effect"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { PluginPromise } from "@opencode/core/plugin/promise"
import { Tool } from "@opencode/core/tool"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"

const source = process.env.PTY_PLUGIN_PATH

if (!source) {
  test.skip("PTY plugin host integration (set PTY_PLUGIN_PATH)", () => {})
} else {
  const it = testEffect(PluginTestLayer)
  it.live("registers the PTY package's five native tools through the v2 host", () =>
    Effect.gen(function* () {
      const module = yield* Effect.promise(() => import(pathToFileURL(source).href))
      const plugins = yield* Plugin.Service
      const tools = yield* Tool.Service
      yield* PluginPromise.fromPromise(module.default).effect(yield* PluginHost.make(plugins))
      const registered = (yield* tools.list()).filter((tool) => tool.name.startsWith("pty_"))
      expect(registered.map((tool) => tool.name).sort()).toEqual([
        "pty_kill",
        "pty_list",
        "pty_read",
        "pty_spawn",
        "pty_write",
      ])
    }),
  )
}
