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
import { Option } from "effect"
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
export const TEARDOWN_WAIT_MS = 5000

interface Entry {
  readonly deferred: Deferred.Deferred<InstanceContext>
  readonly closed: Deferred.Deferred<void>
  active: number
  lastUsed: number
  revision: number
  disposing: boolean
  context?: InstanceContext
  readonly scope: Scope.Closeable
  readonly pins: Map<object, { kind: InstanceLifetime.Kind; since: number }>
  retainedSince?: number
}

interface DisposalReport {
  readonly directory: string
  readonly project?: string
  late: boolean
  notified: boolean
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
      pins: new Map(),
    })

    const reserve = (entry: Entry, kind: InstanceLifetime.Kind = "request") => {
      const token = {}
      entry.pins.set(token, { kind, since: clock.currentTimeMillisUnsafe() })
      entry.active++
      entry.revision++
      let released = false
      return Effect.sync(() => {
        if (released) return
        released = true
        entry.active--
        entry.pins.delete(token)
        entry.revision++
        entry.lastUsed = clock.currentTimeMillisUnsafe()
      })
    }

    const boot = (input: LoadInput & { directory: string }, entry: Entry) => {
      const owner: InstanceLifetime.Owner = {
        directory: input.directory,
        closing: () => entry.disposing || cache.get(input.directory) !== entry,
        scope: entry.scope,
        retained: new Set(),
        acquire: (kind = "bootstrap") => {
          if (entry.disposing || cache.get(input.directory) !== entry)
            throw new Error(`Instance generation is disposing: ${input.directory}`)
          return reserve(entry, kind)
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

    const notifyDisposed = (report: DisposalReport) => Effect.uninterruptible(Effect.suspend(() => {
      if (report.notified) return Effect.void
      report.notified = true
      return emitDisposed(report)
    }))

    const observeDisposers = (report: DisposalReport, ctx?: InstanceContext) => Effect.gen(function* () {
      // This observer belongs to the native cleanup promise, not to the
      // cancellable deadline wait. It only logs; late completion must neither
      // notify subscribers again nor touch a replacement generation's state.
      const context = yield* Effect.context<never>()
      const log = Effect.runPromiseWith(context)
      const run = () => runDisposers(report.directory).then((failures) => log(Effect.gen(function* () {
        const late = report.late ? { late: true } : {}
        for (const failure of failures)
          yield* Effect.logError("instance cleanup failed", { directory: report.directory, cause: failure.reason, ...late })
        yield* Effect.logInfo("instance disposal finished", { directory: report.directory, cleanupFailures: failures.length, ...late })
      })).then(() => failures))
      return yield* ctx ? InstanceLifetime.dispose(ctx, run) : Effect.promise(run)
    })

    const disposeContext = Effect.fn("InstanceStore.disposeContext")(function* (ctx: InstanceContext, report?: DisposalReport) {
      yield* Effect.logInfo("disposing instance", { directory: ctx.directory })
      if (ttl > 0) {
        const outcome = report ?? { directory: ctx.directory, project: ctx.project.id, late: false, notified: false }
        const failures = yield* observeDisposers(outcome, ctx)
        yield* notifyDisposed(outcome)
        return failures.length === 0
      }
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

    const boundedTeardown = <A>(report: DisposalReport, effect: Effect.Effect<A>) => effect.pipe(
      Effect.interruptible,
      Effect.timeoutOption(TEARDOWN_WAIT_MS),
      Effect.tap((result) => Option.isNone(result) ? Effect.gen(function* () {
        report.late = true
        yield* Effect.logWarning("instance teardown deadline exceeded", { directory: report.directory, timeoutMs: TEARDOWN_WAIT_MS })
        yield* notifyDisposed(report)
      }) : Effect.void),
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
        const report: DisposalReport = { directory, project: ctx.project.id, late: false, notified: false }
        let closed = false
        const work = ttl > 0
          ? Effect.gen(function* () {
              // Release generation-owned borrows before RcMap invalidation.
              // Idle sweeps can only reach here once all pins have gone idle.
              closed = yield* closeEntryScope(directory, entry)
              if (cache.get(directory) !== entry) return false
              return yield* disposeContext(ctx, report)
            })
          : disposeContext(ctx).pipe(Effect.ensuring(
              closeEntryScope(directory, entry).pipe(Effect.tap((result) => Effect.sync(() => { closed = result }))),
            ))
        const result = yield* ttl > 0 ? boundedTeardown(report, work) : work.pipe(Effect.map(Option.some))
        const disposed = Option.isSome(result) && result.value
        return disposed && closed && cache.get(directory) === entry
      }).pipe(Effect.ensuring(Effect.gen(function* () {
        yield* removeEntry(directory, entry)
        yield* Deferred.succeed(entry.closed, undefined)
      })))
    }, Effect.uninterruptible)

    const load = (input: LoadInput): Effect.Effect<InstanceContext> => {
      const directory = FSUtil.resolve(input.directory)
      const caller = InstanceLifetime.captureDisposal()
      return Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const existing = cache.get(directory)
          if (ttl > 0) {
            if (((yield* InstanceLifetime.disposing) ?? caller)?.directory === directory)
              return yield* Effect.die(new Error(`reentrant directory teardown: ${directory}`))
          }
          if (existing?.disposing) {
            const result = yield* restore(Deferred.await(existing.closed).pipe(Effect.timeoutOption(TEARDOWN_WAIT_MS)))
            if (Option.isNone(result)) {
              yield* Effect.logWarning("instance admission teardown wait expired", { directory, timeoutMs: TEARDOWN_WAIT_MS })
              yield* removeEntry(directory, existing)
            }
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
            yield* restore(load(input))
            return yield* restore(reload(input))
          }
          const entry = makeEntry()
          cache.set(directory, entry)
          yield* Effect.gen(function* () {
            yield* Effect.logInfo("reloading instance", { directory: directory })
            if (previous) {
              const old = yield* Deferred.await(previous.deferred).pipe(Effect.option)
              const report: DisposalReport = {
                directory, project: input.project?.id,
                late: false, notified: false,
              }
              const work = Effect.gen(function* () {
                if (ttl > 0) {
                  yield* closeEntryScope(directory, previous)
                  yield* observeDisposers(report, Option.isSome(old) ? old.value : undefined)
                  yield* notifyDisposed(report)
                  return
                }
                const failures = yield* Effect.promise(() => runDisposers(directory))
                for (const failure of failures)
                  yield* Effect.logError("instance cleanup failed", { directory, cause: failure.reason })
                yield* closeEntryScope(directory, previous)
                yield* emitDisposed({ directory, project: input.project?.id })
              })
              if (ttl > 0) yield* boundedTeardown(report, work)
              else yield* work
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

    const provide = <A, E, R>(input: LoadInput, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
      if (ttl > 0 && InstanceLifetime.captureDisposal()?.directory === FSUtil.resolve(input.directory))
        return Effect.die(new Error(`reentrant directory teardown: ${input.directory}`))
      return ttl === 0
        ? load(input).pipe(Effect.flatMap((ctx) => effect.pipe(Effect.provideService(InstanceRef, ctx))))
        : Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const lease = yield* restore(acquire(input))
              return yield* restore(effect.pipe(Effect.provideService(InstanceRef, lease.ctx))).pipe(
                Effect.ensuring(lease.release),
              )
            }),
          )
    }

    let lastPinLog = -Infinity
    const sweep = Effect.gen(function* () {
      let disposed = 0
      let retained = 0
      const directories: Array<{ directory: string; pins: InstanceLifetime.Kind[]; ageMs: number }> = []
      for (const [directory, entry] of [...cache]) {
        if (entry.disposing) continue
        if (!Deferred.isDoneUnsafe(entry.deferred)) {
          retained++
          entry.retainedSince ??= entry.lastUsed
          const ageMs = clock.currentTimeMillisUnsafe() - entry.retainedSince
          if (ageMs > 2 * ttl) directories.push({ directory, pins: ["bootstrap"], ageMs })
          continue
        }
        const exit = yield* Effect.exit(Deferred.await(entry.deferred))
        if (Exit.isFailure(exit)) continue
        const revision = entry.revision
        const activity = yield* InstanceLifetime.activity(exit.value)
        if (cache.get(directory) !== entry || entry.disposing) continue
        const pins = [...new Set([...entry.pins.values()].map((pin) => pin.kind).concat(activity))]
        if (entry.active || activity.length) {
          retained++
          entry.retainedSince ??= Math.min(entry.lastUsed, ...[...entry.pins.values()].map((pin) => pin.since))
          const ageMs = clock.currentTimeMillisUnsafe() - entry.retainedSince
          if (ageMs > 2 * ttl) directories.push({ directory, pins, ageMs })
          continue
        }
        entry.retainedSince = undefined
        // Admission may have happened while the non-creating probes were running.
        if (entry.active || revision !== entry.revision || cache.get(directory) !== entry) continue
        if (entry.disposing || clock.currentTimeMillisUnsafe() - entry.lastUsed < ttl) continue
        yield* Effect.logInfo("idle-disposing instance", { directory, idleMs: clock.currentTimeMillisUnsafe() - entry.lastUsed })
        if (yield* disposeEntry(directory, entry, exit.value, () =>
          !entry.active && entry.revision === revision && clock.currentTimeMillisUnsafe() - entry.lastUsed >= ttl,
        )) disposed++
      }
      if (directories.length && clock.currentTimeMillisUnsafe() - lastPinLog >= Math.max(ttl, 300_000)) {
        lastPinLog = clock.currentTimeMillisUnsafe()
        yield* Effect.logInfo("idle instance pins", { entries: cache.size, retained, directories })
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
