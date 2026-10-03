import { expect } from "bun:test"
import { Session } from "@opencode/schema/session"
import { Effect, Schema } from "effect"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

it.live("validates native remote events and fans them out on the ordinary OpenCode event feed", () =>
  Effect.gen(function* () {
    const sent: Array<{ path: string; authorization: string | null; cursor: string | null }> = []
    let controller: ReadableStreamDefaultController<Uint8Array> | undefined
    const proxy = yield* Effect.acquireRelease(
      Effect.sync(() =>
        Bun.serve({
          hostname: "127.0.0.1",
          port: 0,
          idleTimeout: 0,
          fetch(request) {
            sent.push({
              path: new URL(request.url).pathname,
              authorization: request.headers.get("authorization"),
              cursor: request.headers.get("last-event-id"),
            })
            return new Response(
              new ReadableStream<Uint8Array>({
                start(next) {
                  controller = next
                  next.enqueue(
                    new TextEncoder().encode('data: {"id":"evt_connected","type":"server.connected","data":{}}\n\n'),
                  )
                },
              }),
              { headers: { "content-type": "text/event-stream" } },
            )
          },
        }),
      ),
      (proxy) => Effect.sync(() => proxy.stop(true)),
    )
    const handler = yield* ServerFetch.make({
      app: { version: "test" },
      database: { path: ":memory:" },
      fs: { filewatcher: false },
      remoteProxy: { url: `http://127.0.0.1:${proxy.port}`, token: "remote-only" },
    })
    const post = (path: string, body: unknown) =>
      Effect.promise(() =>
        handler(
          new Request(`http://stable.local${path}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
          }),
        ),
      )
    const templateResponse = yield* post("/api/session", {})
    const template = Schema.decodeUnknownSync(Schema.Struct({ data: Schema.toEncoded(Session.Info) }))(
      yield* Effect.promise(() => templateResponse.json()),
    )
    const id = Session.ID.create()
    const mirrored = yield* post("/api/experimental/session/mirror", {
      source: "leaf",
      info: { ...template.data, id },
      messages: [],
    })
    expect(mirrored.status).toBe(200)
    const first = yield* Effect.promise(() => handler(new Request("http://stable.local/api/event")))
    const second = yield* Effect.promise(() => handler(new Request("http://stable.local/api/event")))
    const a = first.body!.getReader(),
      b = second.body!.getReader()
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        await a.cancel()
        await b.cancel()
      }),
    )
    yield* Effect.promise(async () => {
      await a.read()
      await b.read()
    })
    const next = (reader: ReadableStreamDefaultReader<Uint8Array>, id: string) => async () => {
      for (;;) {
        const value = await reader.read()
        if (value.done) throw new Error("Feed ended")
        for (const line of new TextDecoder().decode(value.value).split("\n")) {
          if (!line.startsWith("data: ")) continue
          const event = JSON.parse(line.slice(6))
          if (event.id === id) return event
        }
      }
    }
    yield* Effect.gen(function* () {
      while (!controller) yield* Effect.sleep("20 millis")
    })
    const native = {
      id: "evt_native_started",
      type: "session.execution.started",
      created: Date.now(),
      location: template.data.location,
      durable: { aggregateID: id, seq: 0, version: 1 },
      data: { sessionID: id },
    }
    controller!.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(native)}\n\n`))
    expect(yield* Effect.promise(next(a, native.id))).toEqual(native)
    expect(yield* Effect.promise(next(b, native.id))).toEqual(native)
    expect(sent[0]).toEqual({ path: "/remote/leaf/api/event", authorization: "Bearer remote-only", cursor: null })
    controller!.close()
    yield* Effect.gen(function* () {
      while (sent.length < 2) yield* Effect.sleep("20 millis")
    })
    expect(sent[1]?.cursor).toBe(native.id)
    const ended = {
      ...native,
      id: "evt_native_ended",
      type: "session.execution.succeeded",
      durable: { ...native.durable, seq: 1 },
    }
    controller!.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(ended)}\n\n`))
    expect(yield* Effect.promise(next(a, ended.id))).toEqual(ended)
    expect(yield* Effect.promise(next(b, ended.id))).toEqual(ended)
    // A custom journal envelope fails native decoding and never advances replay.
    controller!.enqueue(
      new TextEncoder().encode('data: {"id":"raw-journal","type":"run.completed","session_id":"leaf"}\n\n'),
    )
    yield* Effect.gen(function* () {
      while (sent.length < 3) yield* Effect.sleep("20 millis")
    })
    expect(sent[2]?.cursor).toBe(ended.id)
  }).pipe(Effect.timeout("15 seconds")),
)
