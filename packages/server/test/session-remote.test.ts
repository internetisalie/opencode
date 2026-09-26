import { expect } from "bun:test"
import { Session } from "@opencode/schema/session"
import { Effect, Schema } from "effect"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const SessionResponse = Schema.Struct({ data: Schema.toEncoded(Session.Info) })

it.live("forwards live mirrored session writes through the configured proxy", () =>
  Effect.gen(function* () {
    const received: Array<{ path: string; authorization: string | null; cookie: string | null; body: string }> = []
    const proxy = yield* Effect.acquireRelease(
      Effect.sync(() =>
        Bun.serve({
          hostname: "127.0.0.1",
          port: 0,
          fetch: async (request) => {
            received.push({
              path: new URL(request.url).pathname + new URL(request.url).search,
              authorization: request.headers.get("authorization"),
              cookie: request.headers.get("cookie"),
              body: await request.text(),
            })
            if (new URL(request.url).pathname.endsWith("/prompt_async"))
              return new Response(
                new ReadableStream({
                  start(controller) {
                    controller.enqueue(new TextEncoder().encode("data: first\n\n"))
                    controller.enqueue(new TextEncoder().encode("data: second\n\n"))
                    controller.close()
                  },
                }),
                { headers: { "content-type": "text/event-stream" } },
              )
            if (new URL(request.url).pathname.endsWith("/message"))
              return Response.json({ data: ["live leaf message"] })
            return Response.json({ accepted: true }, { status: 202 })
          },
        }),
      ),
      (proxy) => Effect.sync(() => proxy.stop(true)),
    )
    const handler = yield* ServerFetch.make({
      app: { version: "test" },
      database: { path: ":memory:" },
      fs: { filewatcher: false },
      password: "stable-password",
      remoteProxy: { url: `http://127.0.0.1:${proxy.port}`, token: "server-only-token" },
    })
    const stableAuthorization = `Basic ${Buffer.from("opencode:stable-password").toString("base64")}`
    const send = (route: string, body: unknown, headers?: Record<string, string>) =>
      handler(
        new Request(`http://opencode.local${route}`, {
          method: "POST",
          headers: { "content-type": "application/json", authorization: stableAuthorization, ...headers },
          body: JSON.stringify(body),
        }),
      )
    const templateResponse = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/session", {
          method: "POST",
          headers: { "content-type": "application/json", authorization: stableAuthorization },
          body: "{}",
        }),
      ),
    )
    const template = Schema.decodeUnknownSync(SessionResponse)(yield* Effect.promise(() => templateResponse.json()))
    const id = Session.ID.create()
    const mirrored = yield* Effect.promise(() =>
      send("/api/experimental/session/mirror", {
        source: "workspace-a",
        info: { ...template.data, id },
        messages: [],
      }),
    )
    expect(mirrored.status).toBe(200)
    const unauthorized = yield* Effect.promise(() =>
      send(`/api/session/${id}/prompt`, { text: "Denied" }, { authorization: "Basic invalid" }),
    )
    expect(unauthorized.status).toBe(401)
    expect(received).toHaveLength(0)

    const prompt = yield* Effect.promise(() =>
      send(
        `/api/session/${id}/prompt`,
        { text: "Continue on the leaf" },
        {
          cookie: "stable=private",
        },
      ),
    )
    expect(prompt.status).toBe(202)
    expect(yield* Effect.promise(() => prompt.json())).toEqual({ accepted: true })
    const stream = yield* Effect.promise(() =>
      send(`/api/session/${id}/prompt_async`, { text: "Stream from the leaf" }),
    )
    expect(stream.status).toBe(200)
    expect(stream.headers.get("content-type")).toContain("text/event-stream")
    expect(yield* Effect.promise(() => stream.text())).toBe("data: first\n\ndata: second\n\n")
    const read = yield* Effect.promise(() =>
      handler(
        new Request(`http://opencode.local/api/session/${id}/message?limit=1`, {
          headers: { authorization: stableAuthorization },
        }),
      ),
    )
    expect(read.status).toBe(200)
    expect(yield* Effect.promise(() => read.json())).toEqual({ data: ["live leaf message"] })
    expect(received).toEqual([
      {
        path: `/remote/workspace-a/api/session/${id}/prompt`,
        authorization: "Bearer server-only-token",
        cookie: null,
        body: '{"text":"Continue on the leaf"}',
      },
      {
        path: `/remote/workspace-a/api/session/${id}/prompt_async`,
        authorization: "Bearer server-only-token",
        cookie: null,
        body: '{"text":"Stream from the leaf"}',
      },
      {
        path: `/remote/workspace-a/api/session/${id}/message?limit=1`,
        authorization: "Bearer server-only-token",
        cookie: null,
        body: "",
      },
    ])
    proxy.stop(true)
    const unavailable = yield* Effect.promise(() => send(`/api/session/${id}/prompt`, { text: "Still on the leaf" }))
    expect(unavailable.status).toBe(502)
    expect(yield* Effect.promise(() => unavailable.json())).toEqual({ code: "remote_unavailable" })
    const retained = yield* Effect.promise(() =>
      handler(
        new Request(`http://opencode.local/api/session/${id}/message`, {
          headers: { authorization: stableAuthorization },
        }),
      ),
    )
    expect(retained.status).toBe(200)
    expect(yield* Effect.promise(() => retained.json())).toMatchObject({ data: [] })
  }).pipe(Effect.scoped),
)
