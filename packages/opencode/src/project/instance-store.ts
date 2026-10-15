import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { makeGlobalNode, Node } from "@opencode-ai/core/effect/app-node"
import { GlobalBus } from "@/bus/global"
import { serviceUse } from "@opencode-ai/core/effect/service-use"
import { WorkspaceContext } from "@/control-plane/workspace-context"
import { InstanceRef } from "@/effect/instance-ref"
import { InstanceLifetime } from "@/effect/instance-lifetime"
import { disposeInstance as runDisposers } from "@/effect/instance-registry"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Clock, Config, Context, Deferred, Duration, Effect, Exit, Layer, Scope } from "effect"
import { type InstanceContext } from "./instance-context"
import { InstanceBootstrap } from "./bootstrap-service"
import * as Project from "./project"

export interface LoadInput {
  directory: string
  worktree?: string
  project?: Project.Info
}

export interface Interface {
  readonly load: (input: LoadInput) => Effect.Effect<InstanceContext>
  readonly reload: (input: LoadInput) => Effect.Effect<InstanceContext>
  readonly dispose: (ctx: InstanceContext) => Effect.Effect<void>
  readonly disposeDirectory: (directory: string) => Effect.Effect<void>
  readonly disposeAll: () => Effect.Effect<void>
  readonly provide: <A, E, R>(input: LoadInput, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/InstanceStore") {}

export const use = serviceUse(Service)

interface Entry {
  readonly deferred: Deferred.Deferred<InstanceContext>
  readonly closed: Deferred.Deferred<void>
  active: number
  lastUsed: number
  revision: number
  disposing: boolean
  context?: InstanceContext
  readonly scope: Scope.Closeable
}

const layer: Layer.Layer<Service, never, Project.Service | InstanceBootstrap.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const project = yield* Project.Service
    const bootstrap = yield* InstanceBootstrap.Service
    const scope = yield* Scope.Scope
    const clock = yield* Clock.Clock
    const ttl = yield* Config.number("OPENCODE_INSTANCE_IDLE_TTL_MS").pipe(Config.withDefault(0), Effect.orDie)
    if (!Number.isFinite(ttl) || !Number.isInteger(ttl) || ttl < 0)
      return yield* Effect.die(new Error("OPENCODE_INSTANCE_IDLE_TTL_MS must be zero or a positive finite integer"))
    const cache = new Map<string, Entry>()

    const makeEntry = (): Entry => ({
      deferred: Deferred.makeUnsafe<InstanceContext>(),
      closed: Deferred.makeUnsafe<void>(),
      active: 0,
      lastUsed: clock.currentTimeMillisUnsafe(),
      revision: 0,
      disposing: false,
      scope: Scope.forkUnsafe(scope),
    })

    const reserve = (entry: Entry) => {
      entry.active++
      entry.revision++
      let released = false
      return Effect.sync(() => {
        if (released) return
        released = true
        entry.active--
        entry.revision++
        entry.lastUsed = clock.currentTimeMillisUnsafe()
      })
    }

    const boot = (input: LoadInput & { directory: string }, entry: Entry) => {
      const owner: InstanceLifetime.Owner = {
        scope: entry.scope,
        retained: new Set(),
        acquire: () => {
          if (entry.disposing || cache.get(input.directory) !== entry)
            throw new Error(`Instance generation is disposing: ${input.directory}`)
          return reserve(entry)
        },
      }
      const run = Effect.gen(function* () {
        const ctx: InstanceContext =
          input.project && input.worktree
            ? {
                directory: input.directory,
                worktree: input.worktree,
                project: input.project,
              }
            : yield* project.fromDirectory(input.directory).pipe(
                Effect.map((result) => ({
                  directory: input.directory,
                  worktree: result.sandbox,
                  project: result.project,
                })),
              )
        if (ttl > 0) InstanceLifetime.bind(ctx, owner)
        yield* bootstrap.run.pipe(Effect.provideService(InstanceRef, ctx))
        return ctx
      }).pipe(
        Effect.provideService(InstanceLifetime.BootstrapOwner, ttl > 0 ? owner : undefined),
      )
      // Before ctx exists, bootstrap work belongs to this entry rather than
      // to an ambient caller that happened to open another directory.
      return (ttl > 0 ? run.pipe(Effect.provideService(InstanceRef, undefined)) : run).pipe(
        Effect.withSpan("InstanceStore.boot"),
      )
    }

    const removeEntry = (directory: string, entry: Entry) =>
      Effect.sync(() => {
        if (cache.get(directory) !== entry) return false
        cache.delete(directory)
        return true
      })

    const completeLoad = (directory: string, input: LoadInput, entry: Entry) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(boot({ ...input, directory }, entry))
        entry.lastUsed = clock.currentTimeMillisUnsafe()
        if (Exit.isFailure(exit)) {
          yield* removeEntry(directory, entry)
          yield* Scope.close(entry.scope, exit)
        }
        if (Exit.isSuccess(exit)) entry.context = exit.value
        yield* Deferred.done(entry.deferred, exit).pipe(Effect.asVoid)
      })

    const emitDisposed = (input: { directory: string; project?: string }) =>
      Effect.sync(() =>
        GlobalBus.emit("event", {
          directory: input.directory,
          project: input.project,
          workspace: WorkspaceContext.workspaceID,
          payload: {
            type: "server.instance.disposed",
            properties: {
              directory: input.directory,
            },
          },
        }),
      )

    const disposeContext = Effect.fn("InstanceStore.disposeContext")(function* (ctx: InstanceContext) {
      yield* Effect.logInfo("disposing instance", { directory: ctx.directory })
      const failures = yield* Effect.promise(() => runDisposers(ctx.directory))
      for (const failure of failures)
        yield* Effect.logError("instance cleanup failed", { directory: ctx.directory, cause: failure.reason })
      yield* Effect.logInfo("instance disposal finished", { directory: ctx.directory, cleanupFailures: failures.length })
      yield* emitDisposed({ directory: ctx.directory, project: ctx.project.id })
      return failures.length === 0
    })

    const closeEntryScope = (directory: string, entry: Entry) => Scope.close(entry.scope, Exit.void).pipe(
      Effect.as(true),
      Effect.catchCause((cause) => Effect.logError("instance location retention cleanup failed", { directory, cause }).pipe(Effect.as(false))),
    )

    const disposeEntry = Effect.fnUntraced(function* (directory: string, entry: Entry, ctx: InstanceContext, eligible?: () => boolean) {
      if (cache.get(directory) !== entry) return false
      if (eligible && !eligible()) return false
      if (ttl > 0) {
        if (entry.disposing) {
          yield* Deferred.await(entry.closed)
          return false
        }
        entry.disposing = true
      }
      return yield* Effect.gen(function* () {
        let closed = false
        const disposed = yield* disposeContext(ctx).pipe(Effect.ensuring(
          closeEntryScope(directory, entry).pipe(Effect.tap((result) => Effect.sync(() => { closed = result }))),
        ))
        return disposed && closed && cache.get(directory) === entry
      }).pipe(Effect.ensuring(Effect.gen(function* () {
        yield* removeEntry(directory, entry)
        yield* Deferred.succeed(entry.closed, undefined)
      })))
    }, Effect.uninterruptible)

    const load = (input: LoadInput): Effect.Effect<InstanceContext> => {
      const directory = FSUtil.resolve(input.directory)
      return Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const existing = cache.get(directory)
          if (existing?.disposing) {
            yield* restore(Deferred.await(existing.closed))
            return yield* restore(load(input))
          }
          if (existing) return yield* restore(Deferred.await(existing.deferred))

          const entry = makeEntry()
          cache.set(directory, entry)
          yield* Effect.gen(function* () {
            yield* Effect.logInfo("creating instance", { directory: directory })
            yield* completeLoad(directory, input, entry)
          }).pipe(Effect.forkIn(scope, { startImmediately: true }))
          return yield* restore(Deferred.await(entry.deferred))
        }),
      ).pipe(Effect.withSpan("InstanceStore.load"))
    }

    const reload = (input: LoadInput): Effect.Effect<InstanceContext> => {
      const directory = FSUtil.resolve(input.directory)
      return Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const previous = cache.get(directory)
          if (previous?.disposing) {
            yield* restore(Deferred.await(previous.closed))
            return yield* restore(reload(input))
          }
          const entry = makeEntry()
          cache.set(directory, entry)
          yield* Effect.gen(function* () {
            yield* Effect.logInfo("reloading instance", { directory: directory })
            if (previous) {
              yield* Deferred.await(previous.deferred).pipe(Effect.ignore)
              const failures = yield* Effect.promise(() => runDisposers(directory))
              for (const failure of failures)
                yield* Effect.logError("instance cleanup failed", { directory, cause: failure.reason })
              yield* closeEntryScope(directory, previous)
              yield* emitDisposed({ directory, project: input.project?.id })
            }
            yield* completeLoad(directory, input, entry)
          }).pipe(Effect.forkIn(scope, { startImmediately: true }))
          return yield* restore(Deferred.await(entry.deferred))
        }),
      ).pipe(Effect.withSpan("InstanceStore.reload"))
    }

    const dispose = Effect.fn("InstanceStore.dispose")(function* (ctx: InstanceContext) {
      const entry = cache.get(ctx.directory)
      if (!entry) return yield* disposeContext(ctx).pipe(Effect.asVoid)

      const exit = yield* Deferred.await(entry.deferred).pipe(Effect.exit)
      if (Exit.isFailure(exit)) return yield* removeEntry(ctx.directory, entry).pipe(Effect.asVoid)
      if (exit.value !== ctx) return
      yield* disposeEntry(ctx.directory, entry, ctx).pipe(Effect.asVoid)
    })

    const disposeDirectory = Effect.fn("InstanceStore.disposeDirectory")(function* (input: string) {
      const directory = FSUtil.resolve(input)
      const entry = cache.get(directory)
      if (!entry) return
      const exit = yield* Deferred.await(entry.deferred).pipe(Effect.exit)
      if (Exit.isFailure(exit)) return yield* removeEntry(directory, entry).pipe(Effect.asVoid)
      yield* disposeEntry(directory, entry, exit.value).pipe(Effect.asVoid)
    })

    const disposeAllOnce = Effect.fnUntraced(function* () {
      yield* Effect.logInfo("disposing all instances")
      yield* Effect.forEach(
        [...cache.entries()],
        (item) =>
          Effect.gen(function* () {
            const exit = yield* Deferred.await(item[1].deferred).pipe(Effect.exit)
            if (Exit.isFailure(exit)) {
              yield* Effect.logWarning("instance dispose failed", { key: item[0], cause: exit.cause })
              yield* removeEntry(item[0], item[1])
              return
            }
            yield* disposeEntry(item[0], item[1], exit.value)
          }),
        { discard: true },
      )
    })

    const cachedDisposeAll = yield* Effect.cachedWithTTL(disposeAllOnce(), Duration.zero)
    const disposeAll = Effect.fn("InstanceStore.disposeAll")(function* () {
      return yield* cachedDisposeAll
    })

    const acquire = (input: LoadInput) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          while (true) {
            const ctx = yield* restore(load(input))
            const entry = cache.get(ctx.directory)
            if (!entry || entry.disposing || entry.context !== ctx) continue
            return { ctx, release: reserve(entry) }
          }
        }),
      )

    const provide = <A, E, R>(input: LoadInput, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      ttl === 0
        ? load(input).pipe(Effect.flatMap((ctx) => effect.pipe(Effect.provideService(InstanceRef, ctx))))
        : Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const lease = yield* restore(acquire(input))
              return yield* restore(effect.pipe(Effect.provideService(InstanceRef, lease.ctx))).pipe(
                Effect.ensuring(lease.release),
              )
            }),
          )

    const sweep = Effect.gen(function* () {
      let disposed = 0
      for (const [directory, entry] of [...cache]) {
        if (entry.disposing || entry.active || clock.currentTimeMillisUnsafe() - entry.lastUsed < ttl) continue
        if (!Deferred.isDoneUnsafe(entry.deferred)) continue
        const exit = yield* Effect.exit(Deferred.await(entry.deferred))
        if (Exit.isFailure(exit)) continue
        const revision = entry.revision
        const busy = yield* InstanceLifetime.busy(exit.value).pipe(Effect.catchCause((cause) =>
          Effect.logWarning("instance activity check failed", { directory, cause }).pipe(Effect.as(true)),
        ))
        // Admission may have happened while the non-creating probes were running.
        if (busy || entry.active || revision !== entry.revision || cache.get(directory) !== entry) continue
        if (entry.disposing || clock.currentTimeMillisUnsafe() - entry.lastUsed < ttl) continue
        yield* Effect.logInfo("idle-disposing instance", { directory, idleMs: clock.currentTimeMillisUnsafe() - entry.lastUsed })
        if (yield* disposeEntry(directory, entry, exit.value, () =>
          !entry.active && entry.revision === revision && clock.currentTimeMillisUnsafe() - entry.lastUsed >= ttl,
        )) disposed++
      }
      if (!disposed) return
      const start = performance.now()
      Bun.gc(true)
      yield* Effect.logInfo("idle instance sweep collected", { disposed, gcMs: performance.now() - start })
    })

    if (ttl > 0) yield* sweep.pipe(
      Effect.catchCause((cause) => Effect.logError("idle instance sweep failed", { cause })),
      Effect.delay(Math.min(30_000, Math.max(1000, ttl / 2))),
      Effect.forever,
      Effect.forkIn(scope),
    )

    yield* Effect.addFinalizer(() => disposeAll().pipe(Effect.ignore))

    return Service.of({
      load,
      reload,
      dispose,
      disposeDirectory,
      disposeAll,
      provide,
    })
  }),
)

export const bootstrapNode = LayerNode.unbound(InstanceBootstrap.Service, Node.tags.values.global)

export const node = makeGlobalNode({
  service: Service,
  layer: layer,
  deps: [Project.node, bootstrapNode],
})

export * as InstanceStore from "./instance-store"
