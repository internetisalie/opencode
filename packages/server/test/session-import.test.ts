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
const MessagesResponse = Schema.Struct({ data: Schema.Array(Schema.toEncoded(SessionMessage.Info)) })

const setup = (database = ":memory:", mediaBlob = false) =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make({
      app: { version: "test" },
      database: { path: database },
      config: { project: false, content: JSON.stringify({ experimental: { media_blob: mediaBlob } }) },
      models: { fetch: false },
      fs: { filewatcher: false },
    })
    return (path: string, body?: unknown, status = 200, method = body === undefined ? "GET" : "POST", media = false) =>
      Effect.promise(async () => {
        const response = await handler(
          new Request(`http://opencode.local${path}`, {
            method,
            headers: {
              "content-type": "application/json",
              ...(media ? { "X-OpenCode-Capabilities": "experimental-media-uri" } : {}),
            },
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

it.live("upgrades native data in transcripts exported before v2.1", () =>
  Effect.gen(function* () {
    const request = yield* setup()
    const template = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", {}))
    const id = Session.ID.create()
    yield* request("/api/experimental/session/import", {
      info: { ...template.data, id },
      messages: [
        {
          id: "msg_legacy",
          type: "assistant",
          agent: "build",
          model: { providerID: "anthropic", id: "claude" },
          providerState: { finish: "kept" },
          content: [
            { type: "text", text: "Answer", state: { signature: "text" } },
            { type: "reasoning", text: "Thinking", state: { signature: "reasoning" } },
            {
              type: "tool",
              id: "call",
              name: "web_search",
              executed: true,
              providerState: { call: "kept" },
              providerResultState: { result: "kept" },
              state: { status: "completed", input: {}, content: [{ type: "text", text: "Found" }] },
              time: { created: 1, ran: 1, completed: 2 },
            },
          ],
          time: { created: 1, completed: 2 },
        },
        {
          id: "msg_compaction",
          type: "compaction",
          status: "completed",
          reason: "manual",
          summary: "Summary",
          recent: "[User]: kept",
          providerState: { compaction: "kept" },
          time: { created: 3 },
        },
        { id: "msg_moved", type: "location-switched", location: { directory: "/moved" }, time: { created: 4 } },
      ],
    })
    expect(yield* request(`/api/session/${id}/message?order=asc`)).toMatchObject({
      data: [
        {
          id: "msg_legacy",
          native: { finish: "kept" },
          content: [
            { type: "text", native: { signature: "text" } },
            { type: "reasoning", native: { signature: "reasoning" } },
            { type: "tool", native: { call: "kept", result: "kept" } },
          ],
        },
        { id: "msg_compaction", native: { compaction: "kept" }, metadata: { recent: "[User]: kept" } },
        { id: "msg_moved", location: { directory: "/moved" } },
      ],
    })
  }).pipe(Effect.scoped),
)

it.live("rejects an invalid imported message without creating the session", () =>
  Effect.gen(function* () {
    const request = yield* setup()
    const template = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", {}))
    const id = Session.ID.create()
    const error = yield* request(
      "/api/experimental/session/import",
      { info: { ...template.data, id }, messages: [{ id: "msg_invalid", type: "assistant" }] },
      400,
    )
    expect(error).toMatchObject({
      _tag: "InvalidRequestError",
      kind: "Payload",
      message: expect.stringContaining(`["messages"][0]`),
    })
    expect(yield* request(`/api/session/${id}`, undefined, 404)).toMatchObject({ _tag: "SessionNotFoundError" })
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

it.live("compares hydrated mirror prefixes exactly and exports media-bearing appended suffixes", () =>
  Effect.gen(function* () {
    const request = yield* setup(":memory:", true)
    const template = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", {}))
    const id = Session.ID.create()
    const info = {
      ...template.data,
      id,
      time: { ...template.data.time, updated: template.data.time.updated + 1000 },
    }
    const file = {
      data: "Zmlyc3Q=",
      mime: "text/plain",
      source: { type: "inline" as const },
      name: "first.txt",
    }
    const user = {
      id: SessionMessage.ID.create(),
      type: "user" as const,
      text: "Initial attachment",
      files: [file],
      time: { created: 100 },
    }
    const assistant = {
      id: SessionMessage.ID.create(),
      type: "assistant" as const,
      agent: "build",
      model: { providerID: "test", id: "test" },
      content: [
        {
          type: "tool" as const,
          id: "call_media",
          name: "read",
          executed: true,
          state: {
            status: "completed" as const,
            input: {},
            content: [{ type: "file" as const, uri: "data:text/plain;base64,Zmlyc3Q=", mime: "text/plain" }] as const,
          },
          time: { created: 110, ran: 111, completed: 112 },
        },
      ],
      finish: "stop" as const,
      time: { created: 110, completed: 120 },
    }
    const snapshot = { source: "loom-workspace-media", info, messages: [user, assistant] }
    yield* request("/api/experimental/session/mirror", snapshot)
    // Opted-in reads prove this exercises stored blobs, not just an inline import.
    const refs = yield* request(`/api/session/${id}/message?order=asc`, undefined, 200, "GET", true)
    expect(refs).toMatchObject({
      data: [
        { files: [{ uri: expect.stringMatching(/^media:[a-f0-9]{64}$/), bytes: 5 }] },
        { content: [{ state: { content: [{ uri: expect.stringMatching(/^media:[a-f0-9]{64}$/) }] } }] },
      ],
    })
    const initial = yield* request(`/api/experimental/session/${id}/export`)
    expect(initial).toMatchObject({ data: { messages: snapshot.messages } })
    yield* request("/api/experimental/session/mirror", snapshot)
    expect(yield* request(`/api/experimental/session/${id}/export`)).toEqual(initial)

    const suffix = { ...user, id: SessionMessage.ID.create(), files: [{ ...file, data: "c2Vjb25k", name: "second.txt" }] }
    const appended = {
      ...snapshot,
      info: { ...info, time: { ...info.time, updated: info.time.updated + 1000 } },
      messages: [...snapshot.messages, suffix],
    }
    yield* request("/api/experimental/session/mirror", appended)
    yield* request("/api/experimental/session/mirror", appended)
    expect(Schema.decodeUnknownSync(MessagesResponse)(yield* request(`/api/session/${id}/message?order=asc`)).data).toEqual(
      appended.messages,
    )
    const exported = yield* request(`/api/experimental/session/${id}/export`)
    expect(exported).toMatchObject({ data: { messages: appended.messages } })

    for (const changed of [
      { ...user, files: [{ ...file, data: "Y2hhbmdlZA==" }] },
      { ...user, files: [{ ...file, name: "renamed.txt" }] },
    ]) {
      expect(
        yield* request(
          "/api/experimental/session/mirror",
          { ...appended, messages: [changed, assistant, suffix] },
          409,
        ),
      ).toMatchObject({ _tag: "ConflictError" })
      expect(yield* request(`/api/experimental/session/${id}/export`)).toEqual(exported)
    }
  }).pipe(Effect.scoped),
)

;[
  {
    name: "unpaired busy and viewed times",
    time: { busy: 120, viewed: 130 },
    expected: { created: 90, updated: 200 },
  },
  {
    name: "busy and viewed times beyond idle",
    time: { busy: 120, idle: 110, viewed: 130 },
    expected: { created: 90, updated: 200, busy: 110, idle: 110, viewed: 110 },
  },
  {
    name: "settled busy and viewed times",
    time: { busy: 100, idle: 110, viewed: 105 },
    expected: { created: 90, updated: 200, busy: 100, idle: 110, viewed: 105 },
  },
].forEach(({ name, time, expected }) => {
  it.live(`retains upstream import normalization of ${name} on identical mirror refreshes`, () =>
    Effect.gen(function* () {
      const request = yield* setup()
      const template = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", {}))
      const info = { ...template.data, time: { created: 90, updated: 200, ...time } }
      const imported = Schema.decodeUnknownSync(SessionResponse)(
        yield* request("/api/experimental/session/import", { info: { ...info, id: Session.ID.create() }, messages: [] }),
      )
      const snapshot = { source: "loom-workspace-times", info: { ...info, id: Session.ID.create() }, messages: [] }
      const first = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/experimental/session/mirror", snapshot))
      expect(first.data.time).toEqual(expected)
      expect(imported.data.time).toEqual({ ...expected, updated: imported.data.time.updated })
      const repeated = Schema.decodeUnknownSync(SessionResponse)(
        yield* request("/api/experimental/session/mirror", snapshot),
      )
      expect(repeated.data).toEqual(first.data)
    }).pipe(Effect.scoped),
  )
})

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
