import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test"
import path from "path"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ConfigProvider, Deferred, Effect, Fiber, Layer, Logger } from "effect"
import { TestClock } from "effect/testing"
import { InstanceStore } from "@/project/instance-store"
import { InstanceRef } from "@/effect/instance-ref"
import { InstanceLifetime } from "@/effect/instance-lifetime"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LSP } from "@/lsp/lsp"
import * as LSPServer from "@/lsp/server"
import { TestInstance, testInstanceStoreLayer, tmpdirScoped } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(LayerNode.compile(LSP.node))
const promiseLogs: Array<{ level: string; message: unknown }> = []
const promiseLogger = Logger.make((options) => { promiseLogs.push({ level: options.logLevel, message: options.message }) })
const idleIt = testEffect(Layer.mergeAll(
  LayerNode.compile(LayerNode.group([LSP.node, CrossSpawnSpawner.node])), testInstanceStoreLayer,
).pipe(
  Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENCODE_INSTANCE_IDLE_TTL_MS: "2000" }))),
  Layer.provide(Logger.layer([promiseLogger])),
))

idleIt.effect("R1-M2 only abandoned native promises lose their lease after five minutes", () => Effect.gen(function* () {
  promiseLogs.length = 0
  const cancelledDir = yield* tmpdirScoped({ git: true })
  const waitingDir = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const cancelledCtx = yield* store.load({ directory: cancelledDir })
  const waitingCtx = yield* store.load({ directory: waitingDir })
  const start = (ctx: typeof cancelledCtx, ready: Deferred.Deferred<void>) => InstanceLifetime.promise(() => {
    Deferred.doneUnsafe(ready, Effect.void)
    return new Promise<void>(() => {})
  }).pipe(Effect.provideService(InstanceRef, ctx), Effect.forkScoped)
  const a = yield* Deferred.make<void>()
  const b = yield* Deferred.make<void>()
  const cancelled = yield* start(cancelledCtx, a)
  const waiting = yield* start(waitingCtx, b)
  yield* Deferred.await(a)
  yield* Deferred.await(b)
  yield* Fiber.interrupt(cancelled)
  yield* TestClock.adjust(299_999)
  expect(yield* store.load({ directory: cancelledDir })).toBe(cancelledCtx)
  yield* TestClock.adjust(4001)
  expect(yield* store.load({ directory: cancelledDir })).not.toBe(cancelledCtx)
  expect(yield* store.load({ directory: waitingDir })).toBe(waitingCtx)
  expect(promiseLogs).toContainEqual({ level: "Warn", message: [
    "abandoned native operation lease expired", { directory: cancelledDir, operation: "LSP.operation", ageMs: 300_000 },
  ] })
  yield* Fiber.interrupt(waiting)
}).pipe(Effect.provide(Logger.layer([promiseLogger]))))

describe("LSP service lifecycle", () => {
  let spawnSpy: ReturnType<typeof spyOn>

  beforeEach(() => {
    spawnSpy = spyOn(LSPServer.Typescript, "spawn").mockResolvedValue(undefined)
  })

  afterEach(() => {
    spawnSpy.mockRestore()
  })

  idleIt.effect("interrupted LSP wait retains the actual startup promise until settlement", () => Effect.gen(function* () {
    const directory = yield* tmpdirScoped({ git: true, config: { lsp: true } })
    const quiet = yield* tmpdirScoped({ git: true })
    const store = yield* InstanceStore.Service
    const ctx = yield* store.load({ directory })
    const other = yield* store.load({ directory: quiet })
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
    spawnSpy.mockImplementation(async () => {
      Deferred.doneUnsafe(started, Effect.void)
      await Effect.runPromise(Deferred.await(release))
      return undefined
    })
    const lsp = yield* LSP.Service
    const startup = yield* lsp.touchFile(path.join(directory, "test.ts")).pipe(Effect.provideService(InstanceRef, ctx), Effect.forkScoped)
    yield* Deferred.await(started)
    yield* Fiber.interrupt(startup)
    yield* TestClock.adjust(4000)
    expect(yield* store.load({ directory })).toBe(ctx)
    expect(yield* store.load({ directory: quiet })).not.toBe(other)
    yield* Deferred.succeed(release, undefined)
    yield* TestClock.adjust(4000)
    expect(yield* store.load({ directory })).not.toBe(ctx)
  }))

  it.instance("init() completes without error", () => LSP.Service.use((lsp) => lsp.init()))

  it.instance("status() returns empty array initially", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.status()
        expect(Array.isArray(result)).toBe(true)
        expect(result.length).toBe(0)
      }),
    ),
  )

  it.instance("diagnostics() returns empty object initially", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.diagnostics()
        expect(typeof result).toBe("object")
        expect(Object.keys(result).length).toBe(0)
      }),
    ),
  )

  it.instance("hasClients() returns false for .ts files in instance when LSP is unset", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.hasClients(path.join((yield* TestInstance).directory, "test.ts"))
        expect(result).toBe(false)
      }),
    ),
  )

  it.instance(
    "hasClients() returns true for .ts files in instance when lsp is true",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const result = yield* lsp.hasClients(path.join((yield* TestInstance).directory, "test.ts"))
          expect(result).toBe(true)
        }),
      ),
    { config: { lsp: true } },
  )

  it.instance(
    "hasClients() keeps built-in LSPs when config object is provided",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const result = yield* lsp.hasClients(path.join((yield* TestInstance).directory, "test.ts"))
          expect(result).toBe(true)
        }),
      ),
    { config: { lsp: { eslint: { disabled: true } } } },
  )

  it.instance("hasClients() returns false for files outside instance", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.hasClients(path.join((yield* TestInstance).directory, "..", "outside.ts"))
        expect(typeof result).toBe("boolean")
      }),
    ),
  )

  it.instance("workspaceSymbol() returns empty array with no clients", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.workspaceSymbol("test")
        expect(Array.isArray(result)).toBe(true)
        expect(result.length).toBe(0)
      }),
    ),
  )

  it.instance("definition() returns empty array for unknown file", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.definition({
          file: path.join((yield* TestInstance).directory, "nonexistent.ts"),
          line: 0,
          character: 0,
        })
        expect(Array.isArray(result)).toBe(true)
      }),
    ),
  )

  it.instance("references() returns empty array for unknown file", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.references({
          file: path.join((yield* TestInstance).directory, "nonexistent.ts"),
          line: 0,
          character: 0,
        })
        expect(Array.isArray(result)).toBe(true)
      }),
    ),
  )

  it.instance("multiple init() calls are idempotent", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        yield* lsp.init()
        yield* lsp.init()
        yield* lsp.init()
      }),
    ),
  )
})

describe("LSP.Diagnostic", () => {
  test("pretty() formats error diagnostic", () => {
    const result = LSP.Diagnostic.pretty({
      range: { start: { line: 9, character: 4 }, end: { line: 9, character: 10 } },
      message: "Type 'string' is not assignable to type 'number'",
      severity: 1,
    } as any)
    expect(result).toBe("ERROR [10:5] Type 'string' is not assignable to type 'number'")
  })

  test("pretty() formats warning diagnostic", () => {
    const result = LSP.Diagnostic.pretty({
      range: { start: { line: 0, character: 0 }, end: { line: 0, character: 5 } },
      message: "Unused variable",
      severity: 2,
    } as any)
    expect(result).toBe("WARN [1:1] Unused variable")
  })

  test("pretty() defaults to ERROR when no severity", () => {
    const result = LSP.Diagnostic.pretty({
      range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } },
      message: "Something wrong",
    } as any)
    expect(result).toBe("ERROR [1:1] Something wrong")
  })
})
