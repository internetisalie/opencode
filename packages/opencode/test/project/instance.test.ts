import { describe, expect, spyOn } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Cause, ConfigProvider, Deferred, Effect, Exit, Fiber, Layer, Logger, Option, Scope } from "effect"
import { EffectBridge } from "../../src/effect/bridge"
import { GlobalBus } from "../../src/bus/global"
import { provideInstanceContext } from "../../src/server/routes/instance/httpapi/middleware/instance-context"
import { WorkspaceRouteContext } from "../../src/server/routes/instance/httpapi/middleware/workspace-routing"
import { Workspace } from "../../src/control-plane/workspace"
import { HttpServerResponse } from "effect/unstable/http"
import { Git } from "../../src/git"
import { Vcs } from "../../src/project/vcs"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { InstanceLifetime } from "../../src/effect/instance-lifetime"
import { TestClock } from "effect/testing"
import { InstanceRef } from "../../src/effect/instance-ref"
import { registerDisposer } from "../../src/effect/instance-registry"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { tmpdirScoped } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

let bootstrapRun: Effect.Effect<void> = Effect.void
const noopBootstrap = Layer.succeed(
  InstanceBootstrap.Service,
  InstanceBootstrap.Service.of({ run: Effect.suspend(() => bootstrapRun) }),
)

const it = testEffect(
  LayerNode.compile(LayerNode.group([InstanceStore.node, CrossSpawnSpawner.node]), [
    [InstanceStore.bootstrapNode, noopBootstrap],
  ]),
)

const setBootstrap = (run: Effect.Effect<void>) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      bootstrapRun = run
    }),
    () =>
      Effect.sync(() => {
        bootstrapRun = Effect.void
      }),
  )

const registerDisposerScoped = (disposer: (directory: string) => Promise<void>) =>
  Effect.acquireRelease(
    Effect.sync(() => registerDisposer(disposer)),
    (off) => Effect.sync(off),
  )

const idle = (ttl?: number) => testEffect(
  LayerNode.compile(LayerNode.group([InstanceStore.node, CrossSpawnSpawner.node]), [
    [InstanceStore.bootstrapNode, noopBootstrap],
  ]).pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(
    ttl === undefined ? {} : { OPENCODE_INSTANCE_IDLE_TTL_MS: String(ttl) },
  )))),
)

idle(2000).effect("R2-L2 reload with the old request context waits for an in-flight explicit dispose", () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const first = yield* store.load({ directory })
  const bridge = yield* EffectBridge.make().pipe(Effect.provideService(InstanceRef, first))
  const started = yield* Deferred.make<() => void>()
  let calls = 0
  yield* registerDisposerScoped((dir) => dir !== directory || calls++ > 0 ? Promise.resolve() : new Promise<void>((resolve) => {
    Deferred.doneUnsafe(started, Effect.succeed(resolve))
  }))
  const teardown = yield* store.dispose(first).pipe(Effect.forkScoped)
  const release = yield* Deferred.await(started)
  yield* Effect.addFinalizer(() => Effect.sync(release))
  const reload = yield* bridge.run(store.reload({ directory })).pipe(Effect.exit, Effect.forkScoped({ startImmediately: true }))
  yield* Effect.sync(release)
  yield* Fiber.join(teardown)
  const result = yield* Fiber.join(reload)
  expect(Exit.isSuccess(result)).toBe(true)
  if (Exit.isSuccess(result)) {
    expect(result.value).not.toBe(first)
    expect(yield* store.load({ directory })).toBe(result.value)
  }
}))

idle(2000).effect("R2-L2 provide with a completed generation's request context recreates the directory", () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const first = yield* store.load({ directory })
  yield* store.dispose(first)
  const result = yield* store.provide({ directory }, InstanceRef).pipe(Effect.provideService(InstanceRef, first), Effect.exit)
  expect(Exit.isSuccess(result)).toBe(true)
  if (Exit.isSuccess(result)) {
    expect(result.value?.directory).toBe(directory)
    expect(result.value).not.toBe(first)
  }
  // An old request ref is not a teardown marker. The genuine fiber-scoped
  // marker must still reject access, including after a replacement has booted.
  const reentrant = yield* store.provide({ directory }, Effect.void).pipe(
    Effect.provideService(InstanceLifetime.disposing, first), Effect.exit,
  )
  expect(Exit.isFailure(reentrant) ? String(Cause.squash(reentrant.cause)) : "served").toContain("reentrant")
}))

for (const reload of [false, true]) idle(2000).effect(`R2-L3 timed-out ${reload ? "reload" : "dispose"} notifies release and reports late cleanup failure`, () => Effect.gen(function* () {
  const messages: ReadonlyArray<unknown>[] = []
  const finished = yield* Deferred.make<void>()
  const logger = Logger.make((options) => {
    const message = options.message as ReadonlyArray<unknown>
    messages.push(message)
    if (message[0] === "instance disposal finished") Deferred.doneUnsafe(finished, Effect.void)
  })
  yield* Effect.gen(function* () {
    const directory = yield* tmpdirScoped({ git: true })
    const store = yield* InstanceStore.Service
    const first = yield* store.load({ directory })
    let events = 0
    const listener = (event: { directory?: string; payload: { type?: string } }) => {
      if (event.directory === directory && event.payload.type === "server.instance.disposed") events++
    }
    GlobalBus.on("event", listener)
    yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", listener)))
    const started = yield* Deferred.make<(reason: Error) => void>()
    let calls = 0
    yield* registerDisposerScoped((dir) => dir !== directory || calls++ > 0 ? Promise.resolve() : new Promise<void>((_, reject) => {
      Deferred.doneUnsafe(started, Effect.succeed(reject))
    }))
    const operation = yield* (reload ? store.reload({ directory }).pipe(Effect.asVoid) : store.dispose(first)).pipe(Effect.forkScoped)
    const reject = yield* Deferred.await(started)
    const failure = new Error("R2 late disposer failure")
    yield* Effect.addFinalizer(() => Effect.sync(() => reject(failure)))
    yield* TestClock.adjust(6000)
    yield* TestClock.withLive(Fiber.join(operation).pipe(Effect.timeout("2 seconds")))
    const eventsAtDeadline = events
    const fresh = yield* store.load({ directory })
    yield* Effect.sync(() => reject(failure))
    const late = yield* TestClock.withLive(Deferred.await(finished).pipe(Effect.timeoutOption(250)))
    expect({ eventsAtDeadline, lateOutcome: Option.isSome(late) }).toEqual({ eventsAtDeadline: 1, lateOutcome: true })
    expect(messages).toContainEqual(["instance cleanup failed", { directory, cause: failure, late: true }])
    expect(messages).toContainEqual(["instance disposal finished", { directory, cleanupFailures: 1, late: true }])
    expect(events).toBe(1)
    expect(yield* store.load({ directory })).toBe(fresh)
  }).pipe(Effect.provide(Logger.layer([logger])))
}))

const gap = (icons: boolean) => testEffect(LayerNode.compile(LayerNode.group([
  InstanceStore.node, CrossSpawnSpawner.node, Git.node, Vcs.node, FSUtil.node,
]), [
  [InstanceStore.bootstrapNode, noopBootstrap],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalIconDiscovery: icons })],
]).pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENCODE_INSTANCE_IDLE_TTL_MS: "2000" })))))

gap(false).effect("R1-L2 Vcs init reserves the gap after its caller returns", () => Effect.gen(function* () {
  const quiet = yield* tmpdirScoped({ git: true })
  const directory = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  yield* store.load({ directory: quiet })
  const ctx = yield* store.load({ directory })
  const git = yield* Git.Service
  const vcs = yield* Vcs.Service
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const branch = git.branch
  const spy = spyOn(git, "branch").mockImplementation((dir) => dir === directory
    ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.andThen(branch(dir)))
    : branch(dir))
  yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()))
  yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
  const disposed: string[] = []
  yield* registerDisposerScoped(async (dir) => { disposed.push(dir) })
  yield* vcs.init().pipe(Effect.provideService(InstanceRef, ctx))
  yield* Deferred.await(entered)
  yield* TestClock.adjust(8000)
  expect(disposed).not.toContain(directory)
  expect(disposed).toContain(quiet)
  yield* Deferred.succeed(release, undefined)
}))

gap(true).effect("R1-L2 icon discovery reserves the gap after bootstrap returns", () => Effect.gen(function* () {
  const quiet = yield* tmpdirScoped({ git: true })
  const directory = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const fs = yield* FSUtil.Service
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const glob = fs.glob
  const spy = spyOn(fs, "glob").mockImplementation((pattern, options) => String(pattern).includes("favicon")
    ? options?.cwd === directory
      ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.as([] as string[]))
      : Effect.succeed([] as string[])
    : glob(pattern, options))
  yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()))
  yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
  const disposed: string[] = []
  yield* registerDisposerScoped(async (dir) => { disposed.push(dir) })
  yield* store.load({ directory: quiet })
  yield* store.load({ directory })
  yield* Deferred.await(entered)
  yield* TestClock.adjust(8000)
  expect(disposed).not.toContain(directory)
  expect(disposed).toContain(quiet)
  yield* Deferred.succeed(release, undefined)
}))

for (const automatic of [false, true]) idle(2000).effect(`R1-H1 reentrant disposer completes during ${automatic ? "sweep" : "explicit disposal"}`, () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const ctx = yield* store.load({ directory })
  const result = yield* Deferred.make<Exit.Exit<void, unknown>>()
  yield* registerDisposerScoped((dir) => dir !== directory ? Promise.resolve() : Effect.runPromise(
    store.provide({ directory }, Effect.void).pipe(
      Effect.timeoutOrElse({ duration: 100, orElse: () => Effect.fail(new Error("deadlock guard")) }),
      Effect.exit,
      Effect.flatMap((exit) => Deferred.succeed(result, exit)),
      Effect.asVoid,
    ),
  ))
  if (automatic) yield* TestClock.adjust(4000)
  else yield* store.dispose(ctx)
  const exit = yield* Deferred.await(result)
  expect(Exit.isFailure(exit) ? String(Cause.squash(exit.cause)) : "served").toContain("reentrant")
  expect(yield* store.load({ directory })).not.toBe(ctx)
}))

idle(2000).effect("R1-H1 admission leaves a stuck teardown after its bound", () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const first = yield* store.load({ directory })
  const started = yield* Deferred.make<() => void>()
  let calls = 0
  yield* registerDisposerScoped((dir) => dir !== directory || calls++ > 0 ? Promise.resolve() : new Promise<void>((resolve) => {
    Deferred.doneUnsafe(started, Effect.succeed(resolve))
  }))
  const teardown = yield* store.dispose(first).pipe(Effect.forkScoped)
  const release = yield* Deferred.await(started)
  yield* Effect.addFinalizer(() => Effect.sync(release))
  const admitted = yield* Deferred.make<void>()
  const request = yield* store.load({ directory }).pipe(Effect.tap(() => Deferred.succeed(admitted, undefined)), Effect.forkScoped)
  yield* TestClock.adjust(4999)
  expect(yield* Deferred.isDone(admitted)).toBe(false)
  yield* TestClock.adjust(1001)
  const fresh = yield* TestClock.withLive(Fiber.join(request).pipe(Effect.timeout("2 seconds")))
  expect(fresh).not.toBe(first)
  yield* TestClock.withLive(Fiber.join(teardown).pipe(Effect.timeout("2 seconds")))
  yield* Effect.sync(release)
  yield* Fiber.join(teardown)
  expect(yield* store.load({ directory })).toBe(fresh)
}))

const pinLogs: ReadonlyArray<unknown>[] = []
const logged = testEffect(LayerNode.compile(LayerNode.group([InstanceStore.node, CrossSpawnSpawner.node]), [
  [InstanceStore.bootstrapNode, noopBootstrap],
]).pipe(
  Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENCODE_INSTANCE_IDLE_TTL_MS: "2000" }))),
  Layer.provide(Logger.layer([Logger.make((options) => { pinLogs.push(options.message as ReadonlyArray<unknown>) })])),
))

logged.effect("R1-M3 aged pins are structured, throttled, and absent for quiet directories", () => Effect.gen(function* () {
  pinLogs.length = 0
  const directory = yield* tmpdirScoped({ git: true })
  const quiet = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  yield* store.load({ directory: quiet })
  yield* TestClock.adjust(4000)
  yield* store.disposeAll()
  expect(pinLogs.filter((line) => line[0] === "idle instance pins")).toHaveLength(0)
  const ready = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const request = yield* store.provide({ directory }, Deferred.succeed(ready, undefined).pipe(Effect.andThen(Deferred.await(release)))).pipe(Effect.forkScoped)
  yield* Deferred.await(ready)
  const retained = () => pinLogs.filter((line) => line[0] === "idle instance pins")
  yield* TestClock.adjust(3999)
  expect(retained()).toHaveLength(0)
  yield* TestClock.adjust(2001)
  expect(pinLogs).toContainEqual(expect.arrayContaining(["idle instance pins"]))
  expect(retained()[0][1]).toMatchObject({ entries: 1, retained: 1, directories: [{ directory, pins: ["request"], ageMs: 5000 }] })
  yield* TestClock.adjust(290_000)
  expect(retained()).toHaveLength(1)
  yield* Deferred.succeed(release, undefined)
  yield* Fiber.join(request)
  yield* TestClock.adjust(310_000)
  expect(retained()).toHaveLength(1)
}))

for (const ttl of [0, 2000]) idle(ttl).effect(`R1-L1 workspace sync does not capture the request instance TTL=${ttl}`, () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const release = yield* Deferred.make<void>()
  const seen = yield* Deferred.make<unknown>()
  const completed = yield* Deferred.make<void>()
  const scope = yield* Scope.Scope
  yield* Effect.gen(function* () {
    const workspace = yield* Workspace.Service
    yield* provideInstanceContext(Effect.succeed(HttpServerResponse.empty()), store, workspace).pipe(
      Effect.provideService(WorkspaceRouteContext, WorkspaceRouteContext.of({ directory })),
    )
    expect(yield* Deferred.await(seen)).toBeUndefined()
    yield* store.disposeDirectory(directory)
    yield* Deferred.succeed(release, undefined)
    yield* Deferred.await(completed)
  }).pipe(Effect.provide(Layer.mock(Workspace.Service, {
    resumeWorkspaceSyncing: () => Effect.gen(function* () {
      yield* Deferred.succeed(seen, yield* InstanceRef)
      yield* Deferred.await(release).pipe(
        Effect.andThen(InstanceLifetime.hold(Deferred.succeed(completed, undefined))),
        Effect.forkIn(scope),
      )
    }),
  })))
}))

for (const ttl of [undefined, 0, 2000]) {
  idle(ttl).effect(`idle expiry configuration ${ttl ?? "unset"}`, () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const disposed: string[] = []
      const gc = spyOn(Bun, "gc")
      yield* Effect.addFinalizer(() => Effect.sync(() => gc.mockRestore()))
      yield* registerDisposerScoped(async (directory) => { disposed.push(directory) })
      const first = yield* store.load({ directory: dir })
      yield* TestClock.adjust(1999)
      expect(disposed).toEqual([])
      yield* TestClock.adjust(120_000)
      expect(disposed).toEqual(ttl ? [dir] : [])
      expect(gc).toHaveBeenCalledTimes(ttl ? 1 : 0)
      const next = yield* store.load({ directory: dir })
      if (ttl) expect(next).not.toBe(first)
      else expect(next).toBe(first)
    }),
  )
}

idle(2000).effect("a pre-fork reservation is released even when its target scope is already closed", () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const ctx = yield* store.load({ directory })
  const scope = yield* Scope.make()
  yield* Scope.close(scope, Exit.void)
  const fiber = yield* InstanceLifetime.fork(Effect.never, scope).pipe(Effect.provideService(InstanceRef, ctx))
  yield* Fiber.await(fiber)
  yield* TestClock.adjust(4000)
  expect(yield* store.load({ directory })).not.toBe(ctx)
}))

idle(2000).effect("bootstrap-owned work remains protected after load completes", () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true })
  const parent = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const parentContext = yield* store.load({ directory: parent })
  const scope = yield* Scope.Scope
  const ready = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const done = yield* Deferred.make<void>()
  yield* setBootstrap(InstanceLifetime.fork(Deferred.succeed(ready, undefined).pipe(
    Effect.andThen(Deferred.await(release)),
    Effect.andThen(Deferred.succeed(done, undefined)),
  ), scope).pipe(Effect.asVoid))
  const first = yield* store.load({ directory }).pipe(Effect.provideService(InstanceRef, parentContext))
  yield* Deferred.await(ready)
  yield* TestClock.adjust(4000)
  expect(yield* store.load({ directory })).toBe(first)
  yield* Deferred.succeed(release, undefined)
  yield* Deferred.await(done)
  yield* TestClock.adjust(1999)
  expect(yield* store.load({ directory })).toBe(first)
  yield* TestClock.adjust(2001)
  expect(yield* store.load({ directory })).not.toBe(first)
}))

idle(2000).effect("one changed sweep collects a multi-directory batch exactly once", () => Effect.gen(function* () {
  const one = yield* tmpdirScoped({ git: true })
  const two = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const gc = spyOn(Bun, "gc")
  yield* Effect.addFinalizer(() => Effect.sync(() => gc.mockRestore()))
  yield* store.load({ directory: one })
  yield* store.load({ directory: two })
  yield* TestClock.adjust(4000)
  expect(gc).toHaveBeenCalledTimes(1)
  yield* TestClock.adjust(10_000)
  expect(gc).toHaveBeenCalledTimes(1)
}))

idle(2000).effect("failed cleanup settles every disposer and is not counted as successful disposal", () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true })
  const store = yield* InstanceStore.Service
  const ctx = yield* store.load({ directory })
  const attempts: string[] = []
  yield* registerDisposerScoped(async () => { attempts.push("failed"); throw new Error("cleanup failed") })
  yield* registerDisposerScoped(async () => { attempts.push("finished") })
  const gc = spyOn(Bun, "gc")
  yield* Effect.addFinalizer(() => Effect.sync(() => gc.mockRestore()))
  yield* TestClock.adjust(4000)
  expect(attempts.sort()).toEqual(["failed", "finished"])
  expect(gc).toHaveBeenCalledTimes(0)
  expect(yield* store.load({ directory })).not.toBe(ctx)
}))

for (const value of ["-1", "1.5", "Infinity", "NaN", "invalid"]) it.live(`rejects invalid idle TTL ${value}`, () => Effect.gen(function* () {
  const result = yield* Layer.build(Layer.fresh(LayerNode.compile(InstanceStore.node, [
    [InstanceStore.bootstrapNode, noopBootstrap],
  ]).pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENCODE_INSTANCE_IDLE_TTL_MS: value })))))).pipe(Effect.exit)
  expect(Exit.isFailure(result)).toBe(true)
}))

idle(2000).effect("request ownership resets idle time on release and isolates directories", () =>
  Effect.gen(function* () {
    const one = yield* tmpdirScoped({ git: true })
    const two = yield* tmpdirScoped({ git: true })
    const store = yield* InstanceStore.Service
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const disposed: string[] = []
    yield* registerDisposerScoped(async (directory) => { disposed.push(directory) })
    const first = yield* store.load({ directory: one })
    yield* store.load({ directory: two })
    const request = yield* store.provide({ directory: one }, Deferred.succeed(started, undefined).pipe(
      Effect.andThen(Deferred.await(release)),
    )).pipe(Effect.forkScoped)
    yield* Deferred.await(started)
    yield* TestClock.adjust(5000)
    expect(disposed).toEqual([two])
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(request)
    yield* TestClock.adjust(1999)
    expect(disposed).toEqual([two])
    yield* TestClock.adjust(2001)
    expect(disposed).toEqual([two, one])
    expect(yield* store.load({ directory: one })).not.toBe(first)
  }),
)

idle(2000).live("admission waits for teardown and deduplicates recreation", () =>
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const store = yield* InstanceStore.Service
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<() => void>()
    yield* registerDisposerScoped(() => new Promise<void>((resolve) => {
      Deferred.doneUnsafe(started, Effect.void)
      Deferred.doneUnsafe(release, Effect.succeed(resolve))
    }))
    const first = yield* store.load({ directory: dir })
    const teardown = yield* store.dispose(first).pipe(Effect.forkScoped)
    yield* Deferred.await(started)
    const next = yield* store.load({ directory: dir }).pipe(Effect.forkScoped)
    const concurrent = yield* store.load({ directory: dir }).pipe(Effect.forkScoped)
    yield* Effect.yieldNow
    yield* Effect.sync(yield* Deferred.await(release))
    yield* Fiber.join(teardown)
    const [a, b] = yield* Effect.all([Fiber.join(next), Fiber.join(concurrent)])
    expect(a).not.toBe(first)
    expect(b).toBe(a)
  }),
)

describe("InstanceStore", () => {
  it.live("loads instance context", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const ctx = yield* store.load({ directory: dir })

      expect(ctx.directory).toBe(dir)
      expect(ctx.worktree).toBe(dir)
    }),
  )

  it.live("runs bootstrap with InstanceRef provided", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      let initializedDirectory: string | undefined

      yield* setBootstrap(
        Effect.gen(function* () {
          initializedDirectory = (yield* InstanceRef)?.directory
        }),
      )
      yield* store.load({ directory: dir })

      expect(initializedDirectory).toBe(dir)
    }),
  )

  it.live("caches loaded instance context by directory", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      let initialized = 0

      yield* setBootstrap(
        Effect.sync(() => {
          initialized++
        }),
      )
      const first = yield* store.load({ directory: dir })
      const second = yield* store.load({ directory: dir })

      expect(second).toBe(first)
      expect(initialized).toBe(1)
    }),
  )

  it.live("dedupes concurrent loads while init is in flight", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let initialized = 0

      yield* setBootstrap(
        Effect.gen(function* () {
          initialized++
          yield* Deferred.succeed(started, undefined)
          yield* Deferred.await(release)
        }),
      )
      const first = yield* store.load({ directory: dir }).pipe(Effect.forkScoped)

      yield* Deferred.await(started)

      yield* setBootstrap(
        Effect.sync(() => {
          initialized++
        }),
      )
      const second = yield* store.load({ directory: dir }).pipe(Effect.forkScoped)

      expect(initialized).toBe(1)
      yield* Deferred.succeed(release, undefined)

      const [firstCtx, secondCtx] = yield* Effect.all([Fiber.join(first), Fiber.join(second)])
      expect(secondCtx).toBe(firstCtx)
      expect(initialized).toBe(1)
    }),
  )

  it.live("removes failed loads from the cache", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      let attempts = 0

      yield* setBootstrap(
        Effect.sync(() => {
          attempts++
          throw new Error("init failed")
        }),
      )
      const failed = yield* store.load({ directory: dir }).pipe(
        Effect.as(false),
        Effect.catchCause(() => Effect.succeed(true)),
      )

      expect(failed).toBe(true)

      yield* setBootstrap(
        Effect.sync(() => {
          attempts++
        }),
      )
      const ctx = yield* store.load({ directory: dir })

      expect(ctx.directory).toBe(dir)
      expect(attempts).toBe(2)
    }),
  )

  it.live("reload replaces the cached context", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service

      const first = yield* store.load({ directory: dir })
      const second = yield* store.reload({ directory: dir })
      const cached = yield* store.load({ directory: dir })

      expect(second).not.toBe(first)
      expect(cached).toBe(second)
    }),
  )

  it.live("stale dispose does not delete an in-flight reload", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const reloading = yield* Deferred.make<void>()
      const releaseReload = yield* Deferred.make<void>()
      const disposed: Array<string> = []
      yield* registerDisposerScoped(async (directory) => {
        disposed.push(directory)
      })

      const first = yield* store.load({ directory: dir })
      yield* setBootstrap(
        Effect.gen(function* () {
          yield* Deferred.succeed(reloading, undefined)
          yield* Deferred.await(releaseReload)
        }),
      )
      const reload = yield* store.reload({ directory: dir }).pipe(Effect.forkScoped)

      yield* Deferred.await(reloading)
      const staleDispose = yield* store.dispose(first).pipe(Effect.forkScoped)
      yield* Deferred.succeed(releaseReload, undefined)

      const second = yield* Fiber.join(reload)
      yield* Fiber.join(staleDispose)

      expect(disposed).toEqual([dir])
      expect(yield* store.load({ directory: dir })).toBe(second)
    }),
  )

  it.live("dedupes concurrent disposeAll calls", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const disposing = yield* Deferred.make<void>()
      const releaseDispose = yield* Deferred.make<() => void>()
      const disposed: Array<string> = []
      yield* registerDisposerScoped((directory) => {
        disposed.push(directory)
        Deferred.doneUnsafe(disposing, Effect.void)
        return new Promise<void>((resolve) => {
          Deferred.doneUnsafe(releaseDispose, Effect.succeed(resolve))
        })
      })

      yield* store.load({ directory: dir })
      const first = yield* store.disposeAll().pipe(Effect.forkScoped)
      yield* Deferred.await(disposing)
      const release = yield* Deferred.await(releaseDispose)
      const second = yield* store.disposeAll().pipe(Effect.forkScoped)

      expect(disposed).toEqual([dir])
      yield* Effect.sync(release)
      yield* Effect.all([Fiber.join(first), Fiber.join(second)])
      expect(disposed).toEqual([dir])
    }),
  )

  it.live("re-arms disposeAll after completion", () =>
    Effect.gen(function* () {
      const dir1 = yield* tmpdirScoped({ git: true })
      const dir2 = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const disposed: Array<string> = []
      yield* registerDisposerScoped(async (directory) => {
        disposed.push(directory)
      })

      yield* store.load({ directory: dir1 })
      yield* store.disposeAll()
      expect(disposed).toEqual([dir1])

      yield* store.load({ directory: dir2 })
      yield* store.disposeAll()
      expect(disposed).toEqual([dir1, dir2])
    }),
  )
})
