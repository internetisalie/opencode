import { SessionStore } from "@opencode/core/session/store"
import { OpenCodeEvent } from "@opencode/protocol/groups/event"
import { Session } from "@opencode/schema/session"
import { Context, Effect, Fiber, Schedule, Schema, Stream } from "effect"
import type { ServerOptions } from "./options"

export class RemoteEventConfig extends Context.Service<RemoteEventConfig, ServerOptions["remoteProxy"]>()(
  "@opencode/server/RemoteEventConfig",
) {}

const decode = Schema.decodeUnknownSync(Schema.fromJsonString(OpenCodeEvent))

/** Mirrored leaves already speak the native event API. Forward validated wire
 * events without replaying them through the local session projector: the leaf
 * owns execution and its settled transcript is imported separately. */
export const observeRemoteEvents = Effect.fn("observeRemoteEvents")(function* (
  config: NonNullable<ServerOptions["remoteProxy"]>,
  sessions: SessionStore.Interface,
  publish: (event: OpenCodeEvent) => Effect.Effect<void>,
) {
  const base = new URL(config.url)
  if (
    !config.token ||
    (base.protocol !== "http:" && base.protocol !== "https:") ||
    base.pathname !== "/" ||
    base.search ||
    base.hash ||
    base.username ||
    base.password
  )
    return yield* Effect.die(new Error("Invalid remote event configuration"))
  const sources = new Map<string, Fiber.Fiber<void>>()
  const connect = (source: string, state: { cursor: string }) =>
    Effect.gen(function* () {
      const url = new URL(`/remote/${encodeURIComponent(source)}/api/event`, base)
      const response = yield* Effect.tryPromise({
        try: (signal) =>
          fetch(url, {
            signal,
            redirect: "error",
            headers: {
              Authorization: `Bearer ${config.token}`,
              Accept: "text/event-stream",
              ...(state.cursor ? { "Last-Event-ID": state.cursor } : {}),
            },
          }),
        catch: () => new Error("Remote event connection unavailable"),
      })
      if (!response.ok || !response.body || !response.headers.get("content-type")?.startsWith("text/event-stream")) {
        yield* Effect.promise(() => response.body?.cancel() ?? Promise.resolve())
        return yield* Effect.fail(new Error("Remote event response unavailable"))
      }
      yield* Stream.fromReadableStream({
        evaluate: () => response.body!,
        onError: () => new Error("Remote event stream disconnected"),
      }).pipe(
        Stream.decodeText(),
        Stream.mapAccumEffect(
          () => "",
          (pending, chunk) =>
            Effect.try({
              try: () => parseNativeBlocks(pending + chunk),
              catch: () => new Error("Invalid remote event stream"),
            }),
        ),
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            if (event.type === "server.connected") return
            if (!("sessionID" in event.data) || typeof event.data.sessionID !== "string") return
            const id = Schema.decodeUnknownSync(Session.ID)(event.data.sessionID)
            if ((yield* sessions.mirrorSource(id)) !== source)
              return yield* Effect.fail(new Error("Remote event source mismatch"))
            const info = yield* sessions.get(id)
            if (!info || event.location?.directory !== info.location.directory)
              return yield* Effect.fail(new Error("Remote event location mismatch"))
            yield* publish(event)
            state.cursor = event.id
          }),
        ),
      )
    })
  const discover = Effect.gen(function* () {
    const list = yield* sessions.list({ limit: 10_000 })
    const active = new Set<string>()
    for (const session of list) {
      const source = yield* sessions.mirrorSource(session.id)
      if (source) active.add(source)
    }
    for (const source of active) {
      if (sources.has(source)) continue
      const state = { cursor: "" }
      const fiber = yield* connect(source, state).pipe(
        Effect.scoped,
        Effect.catch((error) => Effect.logWarning(error.message)),
        Effect.repeat(Schedule.spaced("1 second")),
        Effect.asVoid,
        Effect.forkScoped,
      )
      sources.set(source, fiber)
    }
    for (const [source, fiber] of sources) {
      if (active.has(source)) continue
      yield* Fiber.interrupt(fiber)
      sources.delete(source)
    }
  })
  yield* discover.pipe(Effect.repeat(Schedule.spaced("1 second")), Effect.forkScoped)
})

function parseNativeBlocks(input: string): readonly [string, OpenCodeEvent[]] {
  let buffer = input.replaceAll("\r\n", "\n")
  const events: OpenCodeEvent[] = []
  for (;;) {
    const end = buffer.indexOf("\n\n")
    if (end < 0) break
    const block = buffer.slice(0, end)
    buffer = buffer.slice(end + 2)
    const data = block
      .split("\n")
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trimStart())
      .join("\n")
    if (data.length > 8 * 1024 * 1024) throw new Error("Remote event exceeds frame limit")
    if (data) events.push(decode(data))
  }
  if (buffer.length > 8 * 1024 * 1024) throw new Error("Remote event exceeds frame limit")
  return [buffer, events]
}
