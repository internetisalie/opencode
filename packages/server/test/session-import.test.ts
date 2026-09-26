import { expect } from "bun:test"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { Effect, Schema } from "effect"
import fs from "node:fs/promises"
import path from "node:path"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const SessionResponse = Schema.Struct({ data: Schema.toEncoded(Session.Info) })
const SessionsResponse = Schema.Struct({ data: Schema.Array(Schema.toEncoded(Session.Info)) })

const setup = (database = ":memory:") =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make({
      app: { version: "test" },
      database: { path: database },
      fs: { filewatcher: false },
    })
    return (path: string, body?: unknown, status = 200, method = body === undefined ? "GET" : "POST") =>
      Effect.promise(async () => {
        const response = await handler(
          new Request(`http://opencode.local${path}`, {
            method,
            headers: { "content-type": "application/json" },
            body: body === undefined ? undefined : JSON.stringify(body),
          }),
        )
        const json: unknown = await response.json()
        expect(response.status, JSON.stringify(json)).toBe(status)
        return json
      })
  })

it.live("preserves imported parentID through HTTP import, read, and parent filter", () =>
  Effect.gen(function* () {
    const request = yield* setup()
    const parent = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", { title: "Parent" }))
    const id = Session.ID.create()
    const imported = Schema.decodeUnknownSync(SessionResponse)(
      yield* request("/api/experimental/session/import", {
        info: { ...parent.data, id, parentID: parent.data.id, title: "Imported child" },
        messages: [],
      }),
    )
    const read = Schema.decodeUnknownSync(SessionResponse)(yield* request(`/api/session/${id}`))
    const children = Schema.decodeUnknownSync(SessionsResponse)(
      yield* request(`/api/session?parentID=${parent.data.id}`),
    )
    expect({
      imported: imported.data.parentID,
      read: read.data.parentID,
      children: children.data.map((child) => child.id),
    }).toEqual({ imported: parent.data.id, read: parent.data.id, children: [id] })
  }).pipe(Effect.scoped),
)
;["missing", "self"].forEach((parent) => {
  it.live(`rejects a ${parent} parent without creating the imported session`, () =>
    Effect.gen(function* () {
      const request = yield* setup()
      const template = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", {}))
      const id = Session.ID.create()
      const parentID = parent === "self" ? id : Session.ID.create()
      const error = yield* request(
        "/api/experimental/session/import",
        { info: { ...template.data, id, parentID }, messages: [] },
        404,
      )
      expect(error).toMatchObject({ _tag: "SessionNotFoundError", sessionID: parentID })
      expect(yield* request(`/api/session/${id}`, undefined, 404)).toMatchObject({
        _tag: "SessionNotFoundError",
        sessionID: id,
      })
    }).pipe(Effect.scoped),
  )
})

it.live("mirrors a leaf transcript idempotently and appends settled messages", () =>
  Effect.gen(function* () {
    const request = yield* setup()
    const template = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", { title: "Template" }))
    const id = Session.ID.create()
    const info = {
      ...template.data,
      id,
      title: "Leaf session",
      time: { ...template.data.time, updated: template.data.time.updated + 1000 },
    }
    const first = { source: "loom-workspace-a", info, messages: [] }
    const created = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/experimental/session/mirror", first))
    expect(created.data.id).toBe(id)
    expect(created.data.title).toBe("Leaf session")
    expect(yield* request("/api/experimental/session/mirror", first)).toMatchObject({ data: { id } })

    const message = {
      id: SessionMessage.ID.create(),
      type: "user" as const,
      text: "First leaf prompt",
      time: { created: info.time.updated + 1 },
    }
    const second = {
      ...first,
      info: { ...info, title: "Leaf session updated", time: { ...info.time, updated: info.time.updated + 2000 } },
      messages: [message],
    }
    expect(yield* request("/api/experimental/session/mirror", second)).toMatchObject({
      data: { id, title: "Leaf session updated" },
    })
    expect(yield* request("/api/experimental/session/mirror", second)).toMatchObject({ data: { id } })
    expect(yield* request(`/api/session/${id}/message`)).toMatchObject({ data: [message] })
    const sessions = Schema.decodeUnknownSync(SessionsResponse)(yield* request("/api/session?limit=100"))
    expect(sessions.data.map((session) => session.id)).toContain(id)
    expect(yield* request(`/api/experimental/session/${id}/export`)).toMatchObject({
      data: { info: { id }, messages: [message] },
    })
    expect(yield* request(`/api/session/${id}/prompt`, { text: "Must run on the leaf" }, 409)).toMatchObject({
      _tag: "ConflictError",
      resource: id,
    })
    expect(yield* request(`/api/session/${id}`, undefined, 409, "DELETE")).toMatchObject({
      _tag: "ConflictError",
      resource: id,
    })
    expect(yield* request(`/api/session/${id}`)).toMatchObject({ data: { id } })
  }).pipe(Effect.scoped),
)

it.live("rejects mirror takeover, divergence, and transcript truncation", () =>
  Effect.gen(function* () {
    const request = yield* setup()
    const template = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", {}))
    const id = Session.ID.create()
    const info = {
      ...template.data,
      id,
      time: { ...template.data.time, updated: template.data.time.updated + 1000 },
    }
    const message = {
      id: SessionMessage.ID.create(),
      type: "user" as const,
      text: "Original",
      time: { created: info.time.updated + 1 },
    }
    const snapshot = { source: "loom-workspace-a", info, messages: [message] }
    yield* request("/api/experimental/session/mirror", snapshot)
    for (const attempted of [
      { ...snapshot, source: "loom-workspace-b" },
      { ...snapshot, messages: [{ ...message, text: "Changed" }] },
      { ...snapshot, messages: [] },
      { ...snapshot, info: { ...info, time: { ...info.time, updated: info.time.updated - 1 } } },
    ]) {
      expect(yield* request("/api/experimental/session/mirror", attempted, 409)).toMatchObject({
        _tag: "ConflictError",
      })
    }
    expect(
      yield* request(
        "/api/experimental/session/mirror",
        {
          ...snapshot,
          info: template.data,
        },
        409,
      ),
    ).toMatchObject({ _tag: "ConflictError" })
  }).pipe(Effect.scoped),
)

it.live("retains mirrored sessions and ownership across a server restart", () =>
  Effect.gen(function* () {
    const directory = yield* Effect.acquireRelease(
      Effect.promise(() => fs.mkdtemp(path.join(process.cwd(), ".session-mirror-test-"))),
      (directory) => Effect.promise(() => fs.rm(directory, { recursive: true, force: true })),
    )
    const database = path.join(directory, "opencode.db")
    const snapshot = yield* Effect.scoped(
      Effect.gen(function* () {
        const request = yield* setup(database)
        const template = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", {}))
        const info = { ...template.data, id: Session.ID.create() }
        const snapshot = { source: "loom-workspace-a", info, messages: [] }
        yield* request("/api/experimental/session/mirror", snapshot)
        return snapshot
      }),
    )
    yield* Effect.scoped(
      Effect.gen(function* () {
        const request = yield* setup(database)
        expect(yield* request(`/api/session/${snapshot.info.id}`)).toMatchObject({
          data: { id: snapshot.info.id },
        })
        expect(yield* request("/api/experimental/session/mirror", snapshot)).toMatchObject({
          data: { id: snapshot.info.id },
        })
        expect(yield* request(`/api/session/${snapshot.info.id}`, undefined, 409, "DELETE")).toMatchObject({
          _tag: "ConflictError",
          resource: snapshot.info.id,
        })
      }),
    )
  }).pipe(Effect.scoped),
)
