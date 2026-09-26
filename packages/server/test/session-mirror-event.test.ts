import { expect } from "bun:test"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { Effect, Schema } from "effect"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const SessionResponse = Schema.Struct({ data: Schema.toEncoded(Session.Info) })

it.live("announces appended mirror history over SSE but stays quiet for an identical snapshot", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make({
      app: { version: "test" },
      database: { path: ":memory:" },
      fs: { filewatcher: false },
    })
    const post = (route: string, body: unknown) =>
      Effect.promise(async () => {
        const response = await handler(
          new Request(`http://opencode.local${route}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
          }),
        )
        expect(response.status).toBe(200)
        return response.json() as Promise<unknown>
      })
    const template = Schema.decodeUnknownSync(SessionResponse)(yield* post("/api/session", {}))
    const response = yield* Effect.promise(() => handler(new Request("http://opencode.local/api/event")))
    expect(response.status).toBe(200)
    if (!response.body) return yield* Effect.die(new Error("Event response has no stream"))
    const reader = response.body.getReader()
    yield* Effect.addFinalizer(() => Effect.promise(() => reader.cancel()))
    const next = nextEvent(reader)
    expect((yield* Effect.promise(next)).type).toBe("server.connected")

    const id = Session.ID.create()
    const initial = {
      source: "workspace-a",
      info: { ...template.data, id, time: { ...template.data.time, updated: template.data.time.updated + 1000 } },
      messages: [],
    }
    yield* post("/api/experimental/session/mirror", initial)
    const message = {
      id: SessionMessage.ID.create(),
      type: "user" as const,
      text: "Completed on the leaf",
      time: { created: initial.info.time.updated + 1 },
    }
    const snapshot = {
      ...initial,
      info: { ...initial.info, time: { ...initial.info.time, updated: initial.info.time.updated + 1000 } },
      messages: [message],
    }
    yield* post("/api/experimental/session/mirror", snapshot)
    const update = yield* Effect.promise(async () => {
      for (;;) {
        const event = await next()
        if (event.type === "session.mirror.updated") return event
      }
    })
    expect(update.data.sessionID).toBe(id)

    yield* post("/api/experimental/session/mirror", snapshot)
    const sentinel = Schema.decodeUnknownSync(SessionResponse)(yield* post("/api/session", { title: "Sentinel" }))
    yield* Effect.promise(async () => {
      for (;;) {
        const event = await next()
        expect(event.type).not.toBe("session.mirror.updated")
        if (event.type === "session.created" && event.data.sessionID === sentinel.data.id) return
      }
    })
  }).pipe(Effect.scoped),
)

function nextEvent(reader: ReadableStreamDefaultReader<Uint8Array>) {
  const decoder = new TextDecoder()
  let pending = ""
  return async () => {
    for (;;) {
      const boundary = pending.indexOf("\n\n")
      if (boundary !== -1) {
        const frame = pending.slice(0, boundary)
        pending = pending.slice(boundary + 2)
        const data = frame
          .split("\n")
          .find((line) => line.startsWith("data: "))
          ?.slice(6)
        if (!data) continue
        const event: unknown = JSON.parse(data)
        if (
          typeof event === "object" &&
          event !== null &&
          "type" in event &&
          typeof event.type === "string" &&
          "data" in event &&
          typeof event.data === "object" &&
          event.data !== null
        )
          return event as { type: string; data: Record<string, unknown> }
        continue
      }
      const chunk = await reader.read()
      if (chunk.done) throw new Error("Event stream closed")
      pending += decoder.decode(chunk.value, { stream: true })
    }
  }
}
