import { describe, expect, spyOn } from "bun:test"
import path from "path"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ConfigProvider, Deferred, Effect, Fiber, Layer } from "effect"
import { TestClock } from "effect/testing"
import { LSPClient } from "@/lsp/client"
import { InstanceRef } from "@/effect/instance-ref"
import { InstanceStore } from "@/project/instance-store"
import { registerDisposer } from "@/effect/instance-registry"
import { Instruction } from "@/session/instruction"
import { ReadTool } from "@/tool/read"
import { Truncate } from "@/tool/truncate"
import { Agent } from "@/agent/agent"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { SessionID, MessageID } from "@/session/schema"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { LSP } from "@/lsp/lsp"
import * as LSPServer from "@/lsp/server"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { TestInstance, testInstanceStoreLayer, tmpdirScoped } from "../fixture/fixture"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"

const lspLayer = (flags: Parameters<typeof RuntimeFlags.layer>[0] = {}) =>
  LayerNode.compile(LayerNode.group([LSP.node, Config.node, RuntimeFlags.node, EventV2Bridge.node]), [
    [RuntimeFlags.node, RuntimeFlags.layer(flags)],
  ])

const it = testEffect(Layer.mergeAll(lspLayer(), LayerNode.compile(CrossSpawnSpawner.node)))
const experimentalTyIt = testEffect(
  Layer.mergeAll(lspLayer({ experimentalLspTy: true }), LayerNode.compile(CrossSpawnSpawner.node)),
)
const fakeServerPath = path.join(__dirname, "../fixture/lsp/fake-lsp-server.js")
const idleIt = testEffect(Layer.mergeAll(
  lspLayer(), testInstanceStoreLayer,
  LayerNode.compile(LayerNode.group([CrossSpawnSpawner.node, Instruction.node, FSUtil.node, Truncate.node, Agent.node, Ripgrep.node])),
).pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENCODE_INSTANCE_IDLE_TTL_MS: "200" })))))

const gapIt = testEffect(Layer.mergeAll(
  lspLayer(), testInstanceStoreLayer,
  LayerNode.compile(LayerNode.group([CrossSpawnSpawner.node, Instruction.node, FSUtil.node, Truncate.node, Agent.node, Ripgrep.node])),
).pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENCODE_INSTANCE_IDLE_TTL_MS: "2000" })))))

gapIt.effect("R1-L2 ReadTool warm reserves the gap after the read returns", () => Effect.gen(function* () {
  const quiet = yield* tmpdirScoped({ git: true })
  const directory = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  yield* store.load({ directory: quiet })
  const ctx = yield* store.load({ directory })
  const lsp = yield* LSP.Service
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const spy = spyOn(lsp, "touchFile").mockImplementation(() => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))))
  yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()))
  yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
  const disposed: string[] = []
  const off = registerDisposer(async (dir) => { disposed.push(dir) })
  yield* Effect.addFinalizer(() => Effect.sync(off))
  const filePath = path.join(directory, "gap.txt")
  yield* Effect.promise(() => Bun.write(filePath, "gap\n"))
  const result = yield* Effect.gen(function* () {
    const definition = yield* ReadTool
    const tool = yield* definition.init()
    return yield* tool.execute({ filePath }, { sessionID: SessionID.make("ses_test"), messageID: MessageID.make("msg_test"), callID: "gap", agent: "build", abort: new AbortController().signal, messages: [], metadata: () => Effect.void, ask: () => Effect.void })
  }).pipe(Effect.provideService(InstanceRef, ctx))
  expect(result.output).toContain("gap")
  yield* Deferred.await(entered)
  yield* TestClock.adjust(8000)
  expect(disposed).not.toContain(directory)
  expect(disposed).toContain(quiet)
  yield* Deferred.succeed(release, undefined)
}))

for (const fail of [false, true]) idleIt.live(`read warm and active LSP startup/file touch pin the directory through ${fail ? "error" : "completion"}`, () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true, config: { lsp: { fake: { command: [process.execPath, fakeServerPath], extensions: [".repro"] } } } })
  const quiet = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const ctx = yield* store.load({ directory })
  yield* store.load({ directory: quiet })
  const disposed: string[] = []
  const off = registerDisposer(async (dir) => { disposed.push(dir) })
  yield* Effect.addFinalizer(() => Effect.sync(off))
  const starting = yield* Deferred.make<void>()
  const initialize = yield* Deferred.make<void>()
  const opening = yield* Deferred.make<void>()
  const opened = yield* Deferred.make<void>()
  const original = LSPClient.create
  const pending: Promise<LSPClient.Info>[] = []
  const clients: LSPClient.Info[] = []
  const spy = spyOn(LSPClient, "create").mockImplementation((input) => {
    Deferred.doneUnsafe(starting, Effect.void)
    const task = Effect.runPromise(Deferred.await(initialize)).then(() => original(input)).then((client) => {
      clients.push(client)
      const open = client.notify.open
      client.notify.open = async (input) => {
        Deferred.doneUnsafe(opening, Effect.void)
        await Effect.runPromise(Deferred.await(opened))
        if (fail) throw new Error("file touch failed")
        await client.connection.sendNotification("test/publish-diagnostics", { uri: "file:///unsolicited", diagnostics: [] })
        return open(input)
      }
      return client
    })
    pending.push(task)
    return task
  })
  yield* Effect.addFinalizer(() => Effect.sync(() => { spy.mockRestore(); Deferred.doneUnsafe(initialize, Effect.void); Deferred.doneUnsafe(opened, Effect.void) }).pipe(
    Effect.andThen(Effect.promise(() => Promise.allSettled(pending))), Effect.asVoid,
  ))
  const filePath = path.join(directory, "sample.repro")
  yield* Effect.promise(() => Bun.write(filePath, "hello\n"))
  const result = yield* Effect.gen(function* () {
    const definition = yield* ReadTool
    const tool = yield* definition.init()
    return yield* tool.execute({ filePath }, { sessionID: SessionID.make("ses_test"), messageID: MessageID.make("msg_test"), callID: "read", agent: "build", abort: new AbortController().signal, messages: [], metadata: () => Effect.void, ask: () => Effect.void })
  }).pipe(Effect.provideService(InstanceRef, ctx))
  expect(result.output).toContain("hello")
  yield* awaitWithTimeout(Deferred.await(starting), "warm did not start the fixture")
  yield* pollWithTimeout(Effect.sync(() => disposed.includes(quiet) ? true : undefined), "quiet directory did not expire")
  yield* Effect.sleep(1200)
  expect(disposed).not.toContain(directory)
  expect(yield* store.load({ directory })).toBe(ctx)
  yield* Deferred.succeed(initialize, undefined)
  yield* awaitWithTimeout(Deferred.await(opening), "fixture did not initialize")
  yield* Effect.sleep(2200)
  expect(disposed).not.toContain(directory)
  yield* Deferred.succeed(opened, undefined)
  const client = clients[0]
  const rpcReady = yield* Deferred.make<void>()
  const rpcRelease = yield* Deferred.make<void>()
  const send = client.connection.sendRequest
  let requests = 0
  client.connection.sendRequest = new Proxy(send, {
    apply(target, self, args) {
      if (args[0] !== "textDocument/hover" && args[0] !== "workspace/symbol") return Reflect.apply(target, self, args)
      if (++requests === 2) Deferred.doneUnsafe(rpcReady, Effect.void)
      return Effect.runPromise(Deferred.await(rpcRelease)).then(() => Reflect.apply(target, self, args))
    },
  })
  yield* Effect.addFinalizer(() => Deferred.succeed(rpcRelease, undefined).pipe(Effect.asVoid))
  const lsp = yield* LSP.Service
  const hover = yield* lsp.hover({ file: filePath, line: 0, character: 0 }).pipe(Effect.provideService(InstanceRef, ctx), Effect.forkScoped)
  const symbols = yield* lsp.workspaceSymbol("hello").pipe(Effect.provideService(InstanceRef, ctx), Effect.forkScoped)
  yield* awaitWithTimeout(Deferred.await(rpcReady), "LSP RPCs did not start")
  yield* Effect.sleep(2200)
  expect(disposed).not.toContain(directory)
  yield* Fiber.interrupt(hover)
  yield* Effect.sleep(1200)
  expect(disposed).not.toContain(directory)
  yield* Deferred.succeed(rpcRelease, undefined)
  yield* Fiber.join(symbols)
  yield* pollWithTimeout(Effect.sync(() => disposed.includes(directory) ? true : undefined), "LSP operation did not release its lease")
  expect(yield* store.load({ directory })).not.toBe(ctx)
}))
const disabledDownloadIt = testEffect(
  Layer.mergeAll(lspLayer({ disableLspDownload: true }), LayerNode.compile(CrossSpawnSpawner.node)),
)

describe("lsp.spawn", () => {
  it.instance(
    "does not spawn builtin LSP for files outside instance",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const dir = (yield* TestInstance).directory
          const spy = spyOn(LSPServer.Typescript, "spawn").mockResolvedValue(undefined)

          try {
            yield* lsp.touchFile(path.join(dir, "..", "outside.ts"))
            yield* lsp.hover({
              file: path.join(dir, "..", "hover.ts"),
              line: 0,
              character: 0,
            })
            expect(spy).toHaveBeenCalledTimes(0)
          } finally {
            spy.mockRestore()
          }
        }),
      ),
    { config: { lsp: true } },
  )

  it.instance("does not spawn builtin LSP for files inside instance when LSP is unset", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const dir = (yield* TestInstance).directory
        const spy = spyOn(LSPServer.Typescript, "spawn").mockResolvedValue(undefined)

        try {
          yield* lsp.hover({
            file: path.join(dir, "src", "inside.ts"),
            line: 0,
            character: 0,
          })
          expect(spy).toHaveBeenCalledTimes(0)
        } finally {
          spy.mockRestore()
        }
      }),
    ),
  )

  it.instance(
    "would spawn builtin LSP for files inside instance when lsp is true",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const dir = (yield* TestInstance).directory
          const spy = spyOn(LSPServer.Typescript, "spawn").mockResolvedValue(undefined)

          try {
            yield* lsp.hover({
              file: path.join(dir, "src", "inside.ts"),
              line: 0,
              character: 0,
            })
            expect(spy).toHaveBeenCalledTimes(1)
          } finally {
            spy.mockRestore()
          }
        }),
      ),
    { config: { lsp: true } },
  )

  it.instance(
    "publishes lsp.updated after custom LSP initialization",
    () =>
      Effect.gen(function* () {
        const dir = (yield* TestInstance).directory
        const lsp = yield* LSP.Service
        const updated = yield* Deferred.make<void>()
        const events = yield* EventV2Bridge.Service
        const unsubscribe = yield* events.listen((event) => {
          if (event.type === LSP.Event.Updated.type) Deferred.doneUnsafe(updated, Effect.void)
          return Effect.void
        })
        yield* Effect.addFinalizer(() => unsubscribe)

        const file = path.join(dir, "sample.repro")
        yield* Effect.promise(() => Bun.write(file, "sample\n"))
        yield* lsp.touchFile(file)
        yield* awaitWithTimeout(Deferred.await(updated), "lsp.updated event was not published")
      }),
    {
      config: {
        lsp: {
          fake: {
            command: [process.execPath, fakeServerPath],
            extensions: [".repro"],
          },
        },
      },
    },
  )

  it.instance(
    "would spawn builtin LSP for files inside instance when config object is provided",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const dir = (yield* TestInstance).directory
          const spy = spyOn(LSPServer.Typescript, "spawn").mockResolvedValue(undefined)

          try {
            yield* lsp.hover({
              file: path.join(dir, "src", "inside.ts"),
              line: 0,
              character: 0,
            })
            expect(spy).toHaveBeenCalledTimes(1)
          } finally {
            spy.mockRestore()
          }
        }),
      ),
    {
      config: {
        lsp: {
          eslint: { disabled: true },
        },
      },
    },
  )

  it.instance(
    "uses pyright instead of ty by default",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const dir = (yield* TestInstance).directory
          const ty = spyOn(LSPServer.Ty, "spawn").mockResolvedValue(undefined)
          const pyright = spyOn(LSPServer.Pyright, "spawn").mockResolvedValue(undefined)

          try {
            yield* lsp.hover({
              file: path.join(dir, "src", "inside.py"),
              line: 0,
              character: 0,
            })
            expect(ty).toHaveBeenCalledTimes(0)
            expect(pyright).toHaveBeenCalledTimes(1)
          } finally {
            ty.mockRestore()
            pyright.mockRestore()
          }
        }),
      ),
    { config: { lsp: true } },
  )

  experimentalTyIt.instance(
    "uses ty instead of pyright when experimentalLspTy is enabled",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const dir = (yield* TestInstance).directory
          const ty = spyOn(LSPServer.Ty, "spawn").mockResolvedValue(undefined)
          const pyright = spyOn(LSPServer.Pyright, "spawn").mockResolvedValue(undefined)

          try {
            yield* lsp.hover({
              file: path.join(dir, "src", "inside.py"),
              line: 0,
              character: 0,
            })
            expect(ty).toHaveBeenCalledTimes(1)
            expect(pyright).toHaveBeenCalledTimes(0)
          } finally {
            ty.mockRestore()
            pyright.mockRestore()
          }
        }),
      ),
    { config: { lsp: true } },
  )

  disabledDownloadIt.instance(
    "passes disableLspDownload to builtin LSP spawn",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const dir = (yield* TestInstance).directory
          const pyright = spyOn(LSPServer.Pyright, "spawn").mockResolvedValue(undefined)

          try {
            yield* lsp.hover({
              file: path.join(dir, "src", "inside.py"),
              line: 0,
              character: 0,
            })
            expect(pyright).toHaveBeenCalledTimes(1)
            expect(pyright.mock.calls[0]?.[2]).toMatchObject({ disableLspDownload: true })
          } finally {
            pyright.mockRestore()
          }
        }),
      ),
    { config: { lsp: true } },
  )
})
