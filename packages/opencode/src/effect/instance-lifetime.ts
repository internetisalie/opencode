import { Cause, Context, Effect, Exit, Fiber, Layer, Scope } from "effect"
import { AsyncLocalStorage } from "node:async_hooks"
import { InstanceRef } from "./instance-ref"
import type { InstanceContext } from "@/project/instance-context"

// The store installs an owner before bootstrap. Activity owners depend on this
// lightweight boundary, never on the store's project/bootstrap dependency graph.
export interface Owner {
  readonly directory: string
  readonly closing: () => boolean
  readonly acquire: (kind?: Kind) => Effect.Effect<void>
  readonly scope: Scope.Scope
  readonly retained: Set<object>
}

export type Kind = "request" | "session" | "runner" | "shell" | "job" | "prompt_async" | "pty" | "event" | "lsp" | "warm" | "bootstrap"
export const CANCELLED_PROMISE_LEASE_MS = 5 * 60_000

// Native plugin hooks and in-process SDK handlers can cross Effect runtimes.
// Keep the fiber marker at the Effect boundary and propagate it across those
// native async boundaries as well, without affecting unrelated request fibers.
const disposal = new AsyncLocalStorage<InstanceContext>()
const DisposalRef = Context.Reference<InstanceContext | undefined>("@opencode/InstanceDisposal", { defaultValue: () => undefined })
export const disposing = DisposalRef
// Sample ALS only at a native callback boundary. Reading it from arbitrary
// running fibers is unsafe: Effect's shared scheduler can run unrelated fibers
// in the async context of the task that happened to drain its queue.
export const captureDisposal = () => Fiber.getCurrent() ? undefined : disposal.getStore()
export const isClosing = (ctx: InstanceContext) => owners.get(ctx)?.closing() ?? false
export const guard = <A>(ctx: InstanceContext, run: () => A) => {
  return isClosing(ctx)
    ? Promise.reject(new Error(`reentrant directory teardown: ${ctx.directory}`))
    : run()
}
export const dispose = <A>(ctx: InstanceContext, run: () => Promise<A>) =>
  Effect.promise(() => disposal.run(ctx, run)).pipe(Effect.provideService(DisposalRef, ctx))

const owners = new WeakMap<InstanceContext, Owner>()
const checks = new Set<{ effect: Effect.Effect<boolean | ReadonlyArray<Kind>>; kind: Kind }>()

export const BootstrapOwner = Context.Reference<Owner | undefined>("@opencode/InstanceBootstrapOwner", {
  defaultValue: () => undefined,
})

export function bind(ctx: InstanceContext, owner: Owner) {
  owners.set(ctx, owner)
}

const current = Effect.gen(function* () {
  const ctx = yield* InstanceRef
  return ctx ? owners.get(ctx) : yield* BootstrapOwner
})

export const enabled = Effect.map(current, Boolean)

// Reservation is synchronous with admission; the returned release is idempotent
// and refers to this generation, even after explicit reload/disposal.
export const lease = (kind: Kind) => Effect.gen(function* () {
  const owner = yield* current
  return owner ? owner.acquire(kind) : Effect.void
})
export const acquire = lease("bootstrap")

export const hold = <A, E, R>(effect: Effect.Effect<A, E, R>, kind: Kind = "bootstrap") =>
  Effect.gen(function* () {
    const owner = yield* current
    if (!owner) return yield* effect
    return yield* Effect.acquireUseRelease(Effect.sync(() => owner.acquire(kind)), () => effect, (release) => release)
  })

export const fork = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  scope: Scope.Scope,
  options?: { startImmediately?: boolean; kind?: Kind; owned?: boolean },
) =>
  Effect.uninterruptibleMask(() =>
    Effect.gen(function* () {
      const owner = yield* current
      if (!owner) return yield* effect.pipe(Effect.forkIn(scope, options))
      const release = owner.acquire(options?.kind ?? "bootstrap")
      // Install the child's finalizer before it can be interrupted, even when
      // the target scope is already closed or closes before its first timeslice.
      return yield* Effect.uninterruptibleMask((restore) => restore(effect).pipe(Effect.ensuring(release))).pipe(
        Effect.forkIn(options?.owned ? owner.scope : scope, { startImmediately: true }),
      )
    }),
  )

// Native promises may continue after their awaiting fiber is interrupted. Keep
// their generation reserved until the actual operation settles.
export const promise = <A>(run: () => Promise<A>, operation = "LSP.operation") =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const owner = yield* current
      if (!owner) return yield* restore(Effect.promise(run))
      const release = owner.acquire("lsp")
      let settled = false
      const pending = Promise.resolve().then(run).finally(() => {
        settled = true
        return Effect.runPromise(release)
      })
      return yield* restore(Effect.promise(() => pending)).pipe(Effect.onExit((exit) => {
        if (settled || Exit.isSuccess(exit) || !Cause.hasInterrupts(exit.cause)) return Effect.void
        return Effect.sleep(CANCELLED_PROMISE_LEASE_MS).pipe(
          Effect.andThen(Effect.suspend(() => settled ? Effect.void : release.pipe(Effect.andThen(
            Effect.logWarning("abandoned native operation lease expired", { directory: owner.directory, operation, ageMs: CANCELLED_PROMISE_LEASE_MS }),
          )))),
          Effect.ensuring(release),
          Effect.forkIn(owner.scope, { startImmediately: true }),
          Effect.asVoid,
        )
      }))
    }),
  )

export const all = <A>(runs: ReadonlyArray<() => Promise<A>>, operation = "LSP.run") =>
  Effect.gen(function* () {
    if (!(yield* enabled)) return yield* Effect.promise(() => Promise.all(runs.map((run) => run())))
    return yield* promise(async () => {
      const results = await Promise.allSettled(runs.map((run) => run()))
      return results.map((result) => {
        if (result.status === "rejected") throw result.reason
        return result.value
      })
    }, operation)
  })

export const retain = <A, E, R>(key: object, layer: Layer.Layer<A, E, R>) =>
  Effect.gen(function* () {
    const ctx = yield* InstanceRef
    const owner = ctx && owners.get(ctx)
    if (!owner || owner.retained.has(key)) return
    yield* Layer.buildWithScope(layer, owner.scope)
    owner.retained.add(key)
  })

export const registerCheck = (effect: Effect.Effect<boolean | ReadonlyArray<Kind>>, kind: Kind = "session") =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const check = { effect, kind }
      checks.add(check)
      return check
    }),
    (check) => Effect.sync(() => { checks.delete(check) }),
  )

export const activity = (ctx: InstanceContext) =>
  Effect.forEach(checks, (check) => check.effect.pipe(
    Effect.provideService(InstanceRef, ctx),
    Effect.map((result): ReadonlyArray<Kind> => typeof result === "boolean" ? result ? [check.kind] : [] : result),
    Effect.catchCause((cause) => Effect.logWarning("instance activity check failed", { directory: ctx.directory, kind: check.kind, cause }).pipe(Effect.as([check.kind]))),
  )).pipe(Effect.map((results) => results.flat()))

export const busy = (ctx: InstanceContext) => activity(ctx).pipe(
    Effect.map((results) => results.length > 0),
  )

export * as InstanceLifetime from "./instance-lifetime"
