import { Context, Effect, Layer, Scope } from "effect"
import { InstanceRef } from "./instance-ref"
import type { InstanceContext } from "@/project/instance-context"

// The store installs an owner before bootstrap. Activity owners depend on this
// lightweight boundary, never on the store's project/bootstrap dependency graph.
export interface Owner {
  readonly acquire: () => Effect.Effect<void>
  readonly scope: Scope.Scope
  readonly retained: Set<object>
}

const owners = new WeakMap<InstanceContext, Owner>()
const checks = new Set<Effect.Effect<boolean>>()

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
export const acquire = Effect.gen(function* () {
  const owner = yield* current
  return owner ? owner.acquire() : Effect.void
})

export const hold = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const owner = yield* current
    if (!owner) return yield* effect
    return yield* Effect.acquireUseRelease(Effect.sync(() => owner.acquire()), () => effect, (release) => release)
  })

export const fork = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  scope: Scope.Scope,
  options?: { startImmediately?: boolean },
) =>
  Effect.uninterruptibleMask(() =>
    Effect.gen(function* () {
      const owner = yield* current
      if (!owner) return yield* effect.pipe(Effect.forkIn(scope, options))
      const release = owner.acquire()
      // Install the child's finalizer before it can be interrupted, even when
      // the target scope is already closed or closes before its first timeslice.
      return yield* Effect.uninterruptibleMask((restore) => restore(effect).pipe(Effect.ensuring(release))).pipe(
        Effect.forkIn(scope, { startImmediately: true }),
      )
    }),
  )

// Native promises may continue after their awaiting fiber is interrupted. Keep
// their generation reserved until the actual operation settles.
export const promise = <A>(run: () => Promise<A>) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const owner = yield* current
      if (!owner) return yield* restore(Effect.promise(run))
      const release = owner.acquire()
      const pending = Promise.resolve().then(run).finally(() => Effect.runPromise(release))
      return yield* restore(Effect.promise(() => pending))
    }),
  )

export const all = <A>(runs: ReadonlyArray<() => Promise<A>>) =>
  Effect.gen(function* () {
    if (!(yield* enabled)) return yield* Effect.promise(() => Promise.all(runs.map((run) => run())))
    return yield* promise(async () => {
      const results = await Promise.allSettled(runs.map((run) => run()))
      return results.map((result) => {
        if (result.status === "rejected") throw result.reason
        return result.value
      })
    })
  })

export const retain = <A, E, R>(key: object, layer: Layer.Layer<A, E, R>) =>
  Effect.gen(function* () {
    const ctx = yield* InstanceRef
    const owner = ctx && owners.get(ctx)
    if (!owner || owner.retained.has(key)) return
    yield* Layer.buildWithScope(layer, owner.scope)
    owner.retained.add(key)
  })

export const registerCheck = (check: Effect.Effect<boolean>) =>
  Effect.acquireRelease(
    Effect.sync(() => { checks.add(check) }),
    () => Effect.sync(() => { checks.delete(check) }),
  )

export const busy = (ctx: InstanceContext) =>
  Effect.forEach(checks, (check) => check.pipe(Effect.provideService(InstanceRef, ctx))).pipe(
    Effect.map((results) => results.some(Boolean)),
  )

export * as InstanceLifetime from "./instance-lifetime"
