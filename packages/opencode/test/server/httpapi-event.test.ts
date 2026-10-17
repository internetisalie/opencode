import { afterEach, describe, expect } from "bun:test"
import { get, type IncomingMessage } from "node:http"
import { ConfigProvider, Deferred, Effect, Fiber, Layer, Queue, Schema, Stream } from "effect"
import { HttpServer } from "effect/unstable/http"
import { registerDisposer } from "../../src/effect/instance-registry"
import { EventPaths } from "../../src/server/routes/instance/httpapi/groups/event"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance, tmpdirScoped } from "../fixture/fixture"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"

const EventData = Schema.Struct({
  id: Schema.optional(Schema.String),
  type: Schema.String,
  properties: Schema.Record(Schema.String, Schema.Any),
})

const readEvent = (reader: Queue.Dequeue<Uint8Array>) =>
  Effect.gen(function* () {
    const value = yield* Queue.take(reader).pipe(
      Effect.timeoutOrElse({
        duration: "5 seconds",
        orElse: () => Effect.fail(new Error("timed out waiting for event")),
      }),
    )
    return Schema.decodeUnknownSync(EventData)(JSON.parse(new TextDecoder().decode(value).replace(/^data: /, "")))
  })

const openEventStream = (directory: string, consume = true) =>
  Effect.gen(function* () {
    const server = yield* HttpServer.HttpServer
    const incoming = yield* Effect.acquireRelease(
      Effect.callback<IncomingMessage, Error>((resume) => {
        const request = get(new URL(EventPaths.event, HttpServer.formatAddress(server.address)), {
          headers: { "x-opencode-directory": directory },
        }, (response) => resume(Effect.succeed(response)))
        request.on("error", (error) => resume(Effect.fail(error)))
      }),
      (response) => Effect.sync(() => { response.destroy(); response.socket?.destroy() }),
    )
    const response = { status: incoming.statusCode, headers: incoming.headers }
    const reader = yield* Queue.unbounded<Uint8Array>()
    if (consume) yield* Stream.fromAsyncIterable(incoming, (error) => error).pipe(
      Stream.runForEach((value) => Queue.offer(reader, value)),
      Effect.ignoreCause,
      Effect.forkScoped,
    )
    yield* Effect.addFinalizer(() => Effect.sync(() => incoming.destroy()))
    return { response, reader, fail: () => incoming.destroy(new Error("client stream failed")) }
  })

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

const it = testEffect(httpApiLayer)
const idleIt = testEffect(httpApiLayer.pipe(Layer.provide(ConfigProvider.layer(
  ConfigProvider.fromUnknown({ OPENCODE_INSTANCE_IDLE_TTL_MS: "200" }),
))))

idleIt.live("pins SSE from eager registration through disconnect, then expires", () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true, config: { formatter: false, lsp: false } })
  const quiet = yield* tmpdirScoped({ git: true, config: { formatter: false, lsp: false } })
  const disposed: string[] = []
  const off = registerDisposer(async (dir) => { disposed.push(dir) })
  yield* Effect.addFinalizer(() => Effect.sync(off))
  const opened = yield* Deferred.make<Queue.Dequeue<Uint8Array>>()
  const connection = yield* Effect.scoped(Effect.gen(function* () {
    const { reader } = yield* openEventStream(directory)
    yield* Deferred.succeed(opened, reader)
    yield* Effect.never
  })).pipe(Effect.forkScoped)
  const reader = yield* Deferred.await(opened)
  expect(yield* readEvent(reader)).toMatchObject({ type: "server.connected" })
  yield* requestInDirectory("/session", quiet, { method: "POST" })
  yield* pollWithTimeout(Effect.sync(() => disposed.includes(quiet) ? true : undefined), "quiet instance did not expire")
  yield* Effect.sleep(1200)
  expect(disposed).not.toContain(directory)
  const created = yield* requestInDirectory("/session", directory, { method: "POST" })
  expect(created.status).toBe(200)
  expect(yield* readEvent(reader)).toMatchObject({ type: "session.created" })
  yield* Fiber.interrupt(connection)
  yield* pollWithTimeout(Effect.sync(() => disposed.includes(directory) ? true : undefined), "SSE lease was not released")
  expect((yield* requestInDirectory("/session", directory)).status).toBe(200)
}))

for (const consume of [false, true]) idleIt.live(`releases an SSE lease after ${consume ? "stream failure" : "closing before body consumption"}`, () => Effect.gen(function* () {
  const directory = yield* tmpdirScoped({ git: true, config: { formatter: false, lsp: false } })
  const disposed: string[] = []
  const off = registerDisposer(async (dir) => { disposed.push(dir) })
  yield* Effect.addFinalizer(() => Effect.sync(off))
  const opened = yield* Deferred.make<() => void>()
  const connection = yield* Effect.scoped(Effect.gen(function* () {
    const stream = yield* openEventStream(directory, consume)
    if (consume) expect(yield* readEvent(stream.reader)).toMatchObject({ type: "server.connected" })
    yield* Deferred.succeed(opened, () => { stream.fail() })
    yield* Effect.never
  })).pipe(Effect.forkScoped)
  const fail = yield* Deferred.await(opened)
  if (consume) yield* Effect.sync(fail)
  yield* Fiber.interrupt(connection)
  yield* pollWithTimeout(Effect.sync(() => disposed.includes(directory) ? true : undefined), "SSE exit path leaked its lease")
  expect(disposed.filter((dir) => dir === directory)).toHaveLength(1)
}))

describe("event HttpApi", () => {
  it.instance(
    "serves event stream",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const { response, reader } = yield* openEventStream(directory)

        expect(response.status).toBe(200)
        expect(response.headers["content-type"]).toContain("text/event-stream")
        expect(response.headers["cache-control"]).toBe("no-cache, no-transform")
        expect(response.headers["x-accel-buffering"]).toBe("no")
        expect(response.headers["x-content-type-options"]).toBe("nosniff")
        expect(yield* readEvent(reader)).toMatchObject({ type: "server.connected", properties: {} })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "keeps the event stream open after the initial event",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const { reader } = yield* openEventStream(directory)
        expect(yield* readEvent(reader)).toMatchObject({ type: "server.connected", properties: {} })

        // If no second event arrives within 250ms, the stream is still open.
        const status = yield* Queue.take(reader).pipe(
          Effect.as("event" as const),
          Effect.timeoutOrElse({ duration: "250 millis", orElse: () => Effect.succeed("open" as const) }),
        )
        expect(status).toBe("open")
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "delivers instance events after the initial event",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const { reader } = yield* openEventStream(directory)
        expect(yield* readEvent(reader)).toMatchObject({ type: "server.connected", properties: {} })

        const created = yield* requestInDirectory("/session", directory, { method: "POST" })
        expect(created.status).toBe(200)
        expect(yield* readEvent(reader)).toMatchObject({ type: "session.created" })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )
})
