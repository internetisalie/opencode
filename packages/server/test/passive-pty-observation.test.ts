import { afterAll, expect } from "bun:test"
import { join } from "node:path"
import { PersistentPty } from "@opencode/schema/persistent-pty"
import { Cause, Effect, Exit, Schema } from "effect"
import type { Scope } from "effect"
import { it } from "../../core/test/lib/effect"
import { makePassivePtyFixture, makeReadFault, makeTestConsumer } from "./lib/passive-pty-fixture"
import type { PassivePtyFixture } from "./lib/passive-pty-fixture"

const script = `printf 'READY:%s\\n' "$1"
while [ ! -f "$2/mid" ]; do sleep 0.02; done
printf 'MID:%s\\n' "$1"
while [ ! -f "$2/final" ]; do sleep 0.02; done
printf 'FINAL%s:%s\\n' "$3" "$1"
exit "$3"`
type Target = { readonly caseID: string; readonly info: PersistentPty.Info; readonly code: 0 | 7 }
function live(name: string, body: () => Effect.Effect<void, unknown, Scope.Scope>, timeout: number = 30_000): void {
  it.live(name, () => body().pipe(Effect.timeoutOrElse({
    duration: timeout - 5_000, orElse: () => Effect.fail(new Error(`case-deadline:${name}`)),
  })), timeout)
}

function open(): Effect.Effect<PassivePtyFixture, unknown, Scope.Scope> {
  const root = process.env.OPENCODE_PASSIVE_PTY_TEST_ROOT
  const nativeBinary = process.env.OPENCODE_PTY_BIN
  if (!root || !nativeBinary) return Effect.fail(new Error("blocked: require OPENCODE_PASSIVE_PTY_TEST_ROOT and OPENCODE_PTY_BIN"))
  return makePassivePtyFixture({ root, nativeBinary })
}
function spawn(fixture: PassivePtyFixture, options: { caseID: string; sessionID: string; code: 0 | 7; child?: boolean }): Effect.Effect<Target, Error> {
  const mid = 'printf \'MID:%s\\n\' "$1"'
  const child = '/bin/sh -c \'exit 7\'\nchild=$?\nprintf \'CHILD:%s:%s\\n\' "$child" "$1"'
  const program = options.child ? script.replace(mid, `${mid}\n${child}`) : script
  return fixture.create({ caseID: options.caseID, sessionID: options.sessionID, program: {
    command: "/bin/sh", args: ["-c", program, "passive-pty", options.caseID, join(fixture.root, "gates", options.caseID), String(options.code)], title: options.caseID, env: {},
  } }).pipe(Effect.map((info) => ({ caseID: options.caseID, info: Object.freeze(info), code: options.code })))
}
function pair(fixture: PassivePtyFixture, target: Target): Effect.Effect<PersistentPty.Snapshot | null, Error> {
  return Effect.gen(function* () {
    const listed = (yield* fixture.list(target.info.sessionID)).find((info) => info.id === target.info.id)
    if (!listed) return yield* Effect.fail(new Error(`deleted-or-missing:${target.info.id}`))
    const snapshot = yield* fixture.snapshot(target.info.id)
    expect(listed.sessionID).toBe(target.info.sessionID)
    expect(snapshot.info.id).toBe(target.info.id)
    expect(snapshot.info.sessionID).toBe(target.info.sessionID)
    if (listed.status !== snapshot.info.status || listed.exitCode !== snapshot.info.exitCode) return null
    return snapshot
  })
}
function waitSample(fixture: PassivePtyFixture, target: Target, stage: "READY" | "MID" | "CHILD" | "FINAL"): Effect.Effect<PersistentPty.Snapshot, Error> {
  return Effect.gen(function* () {
    const marker = stage === "FINAL" ? `FINAL${target.code}:${target.caseID}` : stage === "CHILD" ? `CHILD:7:${target.caseID}` : `${stage}:${target.caseID}`
    while (true) {
      const sample = yield* pair(fixture, target)
      if (sample?.text.includes(marker) && sample.info.status === (stage === "FINAL" ? "exited" : "running")) {
        expect(sample.info.exitCode).toBe(stage === "FINAL" ? target.code : undefined)
        report(target.caseID, sample)
        return sample
      }
      yield* Effect.sleep("100 millis")
    }
  }).pipe(Effect.timeoutOrElse({ duration: "10 seconds", orElse: () => Effect.fail(new Error(`incomplete-predicate:${target.caseID}:${stage}`)) }))
}
function report(caseID: string, data: unknown): void {
  process.stdout.write(`${JSON.stringify({ caseID, data })}\n`)
}
function attributed(sample: PersistentPty.Snapshot, target: Target, targets: readonly Target[]): void {
  expect(sample.text).toContain(`:${target.caseID}`)
  for (const other of targets.filter((other) => other !== target)) expect(sample.text).not.toContain(`:${other.caseID}`)
}
function finish(fixture: PassivePtyFixture, target: Target): Effect.Effect<void, Error> {
  return fixture.releaseGate({ caseID: target.caseID, stage: "mid" }).pipe(Effect.andThen(fixture.releaseGate({ caseID: target.caseID, stage: "final" })))
}
function waitGone(pid: number): Effect.Effect<void, Error> {
  return Effect.tryPromise({ try: async (signal) => {
    while (!signal.aborted) {
      try { process.kill(pid, 0) } catch (cause) {
        if (cause instanceof Error && "code" in cause && cause.code === "ESRCH") return
        throw cause
      }
      await Bun.sleep(20)
    }
  }, catch: (cause) => cause instanceof Error ? cause : new Error(String(cause)) }).pipe(
    Effect.timeoutOrElse({ duration: "10 seconds", orElse: () => Effect.fail(new Error(`producer-still-alive:${pid}`)) }),
  )
}
function quiescent(fixture: PassivePtyFixture, target: Target, first: PersistentPty.Snapshot): Effect.Effect<PersistentPty.Snapshot, Error> {
  return Effect.gen(function* () {
    let previous = first
    while (true) {
      yield* Effect.sleep("100 millis")
      const sample = yield* pair(fixture, target)
      if (sample?.info.status === "exited" && sample.info.exitCode === target.code && sample.text.includes(`FINAL${target.code}:${target.caseID}`)) {
        if (sample.info.output.tail === previous.info.output.tail) return sample
        previous = sample
      }
      report("P03-quiescence", { kind: "incomplete-pair-or-changing-tail", sample })
    }
  }).pipe(Effect.timeoutOrElse({ duration: "10 seconds", orElse: () => Effect.fail(new Error("incomplete-quiescence")) }))
}

live("P01 direct HTTP attribution without UI attachment", () => Effect.gen(function* () {
  const fixture = yield* open()
  const unknownGate = yield* fixture.releaseGate({ caseID: "Unset", stage: "mid" }).pipe(Effect.result)
  expect(unknownGate._tag === "Failure" && unknownGate.failure.message).toBe("uncreated-case:Unset")
  const targets = yield* Effect.forEach([
    { caseID: "A1", sessionID: "ses_A", code: 7 as const }, { caseID: "A2", sessionID: "ses_A", code: 0 as const }, { caseID: "B1", sessionID: "ses_B", code: 0 as const },
  ], (options) => spawn(fixture, options))
  for (const target of targets) attributed(yield* waitSample(fixture, target, "READY"), target, targets)
  expect(yield* fixture.readCurrent("ses_A")).toBeNull()
  expect((yield* fixture.list("ses_A")).map((info) => info.id).sort()).toEqual(targets.slice(0, 2).map((target) => target.info.id).sort())
  expect((yield* fixture.list("ses_B")).map((info) => info.id)).toEqual([targets[2].info.id])
  for (const target of targets) {
    yield* fixture.releaseGate({ caseID: target.caseID, stage: "mid" })
    const sample = yield* waitSample(fixture, target, "MID")
    attributed(sample, target, targets)
    yield* fixture.releaseGate({ caseID: target.caseID, stage: "final" })
    attributed(yield* waitSample(fixture, target, "FINAL"), target, targets)
    expect(yield* fixture.readCurrent("ses_A")).toBeNull()
  }
  report("P01-prior", { source: "LDV-200 runtime6 Finding I", configuredSelection: "B→A→B; current(A)=A2", kind: "attributed prior evidence, not new HTTP/plugin proof" })
}), 30_000)

live("P02 late first state query after producer exit", () => Effect.gen(function* () {
  const fixture = yield* open()
  for (const code of [0, 7] as const) {
    const target = yield* spawn(fixture, { caseID: `Late${code}`, sessionID: "ses_Late", code })
    yield* finish(fixture, target)
    yield* waitGone(target.info.pid)
    const outcome = yield* pair(fixture, target).pipe(Effect.result)
    if (outcome._tag === "Failure") {
      report("P02", { kind: "unavailable", lastState: target.info, error: String(outcome.failure) })
      continue
    }
    if (!outcome.success) {
      report("P02", { kind: "incomplete-pair", lastState: target.info })
      continue
    }
    expect(outcome.success.info.status).toBe("exited")
    expect(outcome.success.info.exitCode).toBe(code)
    expect(outcome.success.text).toContain(`FINAL${code}:${target.caseID}`)
    report("P02", { kind: "late-recovered", sample: outcome.success })
  }
}), 30_000)

live("P03 preserves exited7 and final output for ten seconds", () => Effect.gen(function* () {
  const fixture = yield* open()
  const target = yield* spawn(fixture, { caseID: "Retained", sessionID: "ses_Retained", code: 7 })
  yield* finish(fixture, target)
  yield* waitGone(target.info.pid)
  const first = yield* waitSample(fixture, target, "FINAL")
  const observedAt = performance.now()
  const quiet = yield* quiescent(fixture, target, first)
  const times: number[] = [0, performance.now() - observedAt]
  while (performance.now() - observedAt < 10_000) {
    yield* Effect.sleep("100 millis")
    const sample = yield* pair(fixture, target)
    expect(sample).not.toBeNull()
    expect(sample?.info.status).toBe("exited")
    expect(sample?.info.exitCode).toBe(7)
    expect(sample?.text).toContain("FINAL7:Retained")
    expect(sample?.info.output.tail).toBe(quiet.info.output.tail)
    times.push(performance.now() - observedAt)
  }
  const final = yield* pair(fixture, target)
  expect(final?.info).toMatchObject({ status: "exited", exitCode: 7, output: { tail: quiet.info.output.tail } })
  expect(final?.text).toContain("FINAL7:Retained")
  report("P03", { anchor: "first agreeing Core exit observation", sampledQuiescenceMs: times[1], intervalMs: times, first, final })
}), 45_000)

live("P04 response loss, removal and child exit are not PTY completion", () => Effect.gen(function* () {
  const fixture = yield* open()
  const live = yield* spawn(fixture, { caseID: "Live", sessionID: "ses_Loss", code: 0 })
  yield* waitSample(fixture, live, "READY")
  const fault = yield* makeReadFault(fixture.snapshot(live.info.id), "loss")
  const lost = Effect.runPromiseExit(fault.request)
  yield* fault.ready
  const loss = yield* Effect.promise(() => lost)
  yield* fault.disconnected
  expect(Exit.isFailure(loss) && !Cause.pretty(loss.cause).includes("SchemaError")).toBeTrue()
  report("P04-loss", { kind: "unavailable-sample", cause: Exit.isFailure(loss) ? Cause.pretty(loss.cause) : "unexpected success" })
  yield* waitSample(fixture, live, "READY")
  const removed = yield* spawn(fixture, { caseID: "Removed", sessionID: "ses_Removal", code: 0 })
  yield* waitSample(fixture, removed, "READY")
  yield* fixture.remove(removed.info.id)
  expect(yield* fixture.list(removed.info.sessionID)).toEqual([])
  const missing = yield* fixture.snapshot(removed.info.id).pipe(Effect.result)
  expect(missing._tag).toBe("Failure")
  if (missing._tag === "Failure") expect(missing.failure.message).toContain("HTTP404")
  report("P04-removed", { kind: "unavailable-PTY-not-completed", error: missing._tag === "Failure" ? String(missing.failure) : "unexpected success" })
  const child = yield* spawn(fixture, { caseID: "Child", sessionID: "ses_Child", code: 0, child: true })
  yield* fixture.releaseGate({ caseID: child.caseID, stage: "mid" })
  yield* waitSample(fixture, child, "CHILD")
  yield* fixture.releaseGate({ caseID: child.caseID, stage: "final" })
  yield* waitSample(fixture, child, "FINAL")
}), 30_000)

live("P05 public decoder controls preserve codes and decode checkpoint", () => Effect.sync(() => {
  const base = {
    id: "pty_persistent_1", sessionID: "ses_P05", title: "P05", command: "/bin/sh", args: [], cwd: "/",
    status: "exited", pid: 1, foregroundProcess: null, size: { cols: 80, rows: 24 }, output: { head: 0, tail: 0 },
  }
  const decode = Schema.decodeUnknownSync(Schema.toCodecJson(Schema.Struct({ data: Schema.Array(PersistentPty.Info) })))
  expect(decode({ data: [base] }).data[0].exitCode).toBeUndefined()
  expect(decode({ data: [{ ...base, status: "running" }] }).data[0]).toMatchObject({ status: "running" })
  expect(decode({ data: [{ ...base, status: "running" }] }).data[0].exitCode).toBeUndefined()
  expect(decode({ data: [{ ...base, exitCode: 7 }] }).data[0].exitCode).toBe(7)
  expect(() => decode({ data: [{ ...base, exitCode: null }] })).toThrow()
  const sample = Schema.decodeUnknownSync(Schema.toCodecJson(Schema.Struct({ data: PersistentPty.Snapshot })))({
    data: { info: { ...base, exitCode: 7 }, text: "FINAL7:P05", checkpoint: "AA==", cursor: { x: 0, y: 0 } },
  }).data
  expect(sample.info.exitCode).toBe(7)
  expect(sample.text).toBe("FINAL7:P05")
  expect(sample.checkpoint).toBeInstanceOf(Uint8Array)
  expect(Array.from(sample.checkpoint)).toEqual([0])
  report("P05", { kind: "public decoder controls only", nativeFailedAndNull: "indistinguishable omitted code at this boundary; not live cases" })
}), 30_000)

function until(predicate: () => boolean): Effect.Effect<void, Error> {
  return Effect.gen(function* () { while (!predicate()) yield* Effect.sleep("10 millis") }).pipe(
    Effect.timeoutOrElse({ duration: "10 seconds", orElse: () => Effect.fail(new Error("consumer-state-timeout")) }),
  )
}

live("P06 disposes real held requests and scheduled test-consumer reads", () => Effect.gen(function* () {
  const fixture = yield* open()
  const target = yield* spawn(fixture, { caseID: "Disposed", sessionID: "ses_Disposal", code: 0 })
  yield* waitSample(fixture, target, "READY")
  const scheduled = makeTestConsumer(fixture.snapshot(target.info.id))
  yield* Effect.addFinalizer(() => Effect.promise(scheduled.dispose))
  yield* until(() => scheduled.status() === "timer")
  const callbacks = scheduled.callbacks()
  const timerFirings = scheduled.timerFirings()
  expect(callbacks).toBeGreaterThan(0)
  expect(scheduled.dispose()).toBe(scheduled.dispose())
  yield* Effect.promise(scheduled.dispose)
  yield* Effect.sleep("150 millis")
  expect(scheduled.callbacks()).toBe(callbacks)
  expect(scheduled.timerFirings()).toBe(timerFirings)
  expect(scheduled.errors).toEqual([])
  const fault = yield* makeReadFault(fixture.snapshot(target.info.id), "hold")
  const held = makeTestConsumer(fault.request)
  yield* Effect.addFinalizer(() => Effect.promise(held.dispose))
  yield* fault.ready
  expect(held.status()).toBe("request")
  expect(fault.sockets()).toBe(1)
  expect(held.dispose()).toBe(held.dispose())
  yield* Effect.promise(held.dispose)
  yield* fault.disconnected
  expect(fault.sockets()).toBe(0)
  expect(held.errors).toEqual(["cancelled"])
  yield* fixture.releaseGate({ caseID: target.caseID, stage: "mid" })
  yield* waitSample(fixture, target, "MID")
  report("P06", { cancelled: held.errors, timerFiringsTotal: scheduled.timerFirings(), scope: "scheduled timer cancellation and held-request interruption; not successful post-await delivery or future notifier" })
}), 30_000)

live("P07 same-owner recovery does not establish replacement continuity", () => Effect.gen(function* () {
  const fixture = yield* open()
  const target = yield* spawn(fixture, { caseID: "Recovery", sessionID: "ses_Recovery", code: 0 })
  yield* waitSample(fixture, target, "READY")
  const fault = yield* makeReadFault(fixture.snapshot(target.info.id), "loss")
  const lost = Effect.runPromiseExit(fault.request)
  yield* fault.ready
  const loss = yield* Effect.promise(() => lost)
  yield* fault.disconnected
  expect(Exit.isFailure(loss) && !Cause.pretty(loss.cause).includes("SchemaError")).toBeTrue()
  report("P07-loss", { kind: "unavailable-sample", cause: Exit.isFailure(loss) ? Cause.pretty(loss.cause) : "unexpected success" })
  yield* fixture.releaseGate({ caseID: target.caseID, stage: "mid" })
  yield* waitSample(fixture, target, "MID")
  yield* fixture.close()
  yield* fixture.close()
  yield* waitGone(target.info.pid)
  const ended = yield* fixture.snapshot(target.info.id).pipe(Effect.result)
  expect(ended._tag).toBe("Failure")
  report("P07-owner-end", { kind: "unavailable-not-completion", error: ended._tag === "Failure" ? ended.failure.message : "unexpected success" })
  const replacement = yield* open()
  expect(yield* replacement.list(target.info.sessionID)).toEqual([])
  const old = yield* replacement.snapshot(target.info.id).pipe(Effect.result)
  expect(old._tag).toBe("Failure")
  if (old._tag === "Failure") expect(old.failure.message).toContain("HTTP404")
  report("P07", { sameOwner: "current screen recovered, missed bytes unavailable", ownerEnd: "unavailable, not completion", replacement: "old ID absent, continuity unavailable" })
}), 30_000)

afterAll((): void => report("P08", {
  kind: "evidence-limit report, no byte comparator executed",
  byteContinuity: "unavailable", exactNativeFinalOffset: "unavailable", nativeInstanceFence: "unavailable",
  sampledScreen: "list/snapshot/checkpoint/head/tail cannot prove replay plus independent producer bytes",
  LDV199: "attributed prior saved-interval inspection, experiments 5da4c1d25f8d192addb9662279cd8b556ff7435a / 0f51038fffc3c7060160b8e96cf8e7d1f6b28163; not new Core comparison",
  LDV200: "attributed Finding I and H; runtime6 timeout/TERM143, runtime7 H7 HTTP429, runtime8 authorized retry retained",
  pins: {
    plan: "4ad4d4df23dd943fdd2f9c48d441af3caa6b6741", source: "591c6999ebceb86f1b4869685d87bc2e10d9a333",
    native: "b8caf78cbed9610aa5b744264aade552ed57cac70b2ebc27a788a0b1d2e02caa",
    LDV199Input: "v2/item/gitea.internetisalie.net/loom/loom/LDV-199/input/ldv-199-cloud-gaps-verified-input.tar.gz",
    LDV199Hash: "2de029e3cb4a6cf36da784d29771554b3a6e319099f7288c83bac01b20b4fdc2",
    LDV200Evidence: "v2/item/gitea.internetisalie.net/loom/loom/LDV-200/evidence/ldv200-runtime6-case-I-evidence.tar.gz",
    LDV200Hash: "7f92fd39ff02b01ea3e809a1bba2b30db224a9bbe8d0f1ad5dcc5d222b39a052",
  },
  G03: "unsupported; no acceptance tick from unavailable outcomes",
}))
