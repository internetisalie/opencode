import { SessionStore } from "@opencode/core/session/store"
import { Session } from "@opencode/schema/session"
import { Effect, Option, Schema, Scope } from "effect"
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import type { ServerOptions } from "../options"
import { ServerAuth } from "../auth"
import { authorizedRequest, unauthorizedResponse } from "./authorization"

type App = Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  unknown,
  HttpServerRequest.HttpServerRequest | Scope.Scope
>

const decodeSessionID = Schema.decodeUnknownOption(Session.ID)

export function forwardMirroredSession(
  app: App,
  config: ServerOptions["remoteProxy"],
  sessions: SessionStore.Interface,
  password?: string,
): App {
  if (!config) return app
  const base = new URL(config.url)
  if (
    (base.protocol !== "http:" && base.protocol !== "https:") ||
    base.pathname !== "/" ||
    base.search ||
    base.hash ||
    base.username ||
    base.password ||
    !config.token ||
    /[\r\n]/.test(config.token)
  )
    throw new Error("Invalid remote session proxy configuration")
  const auth = password ? ServerAuth.Config.of({ password: Option.some(password), username: "opencode" }) : undefined

  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    if (request.method === "HEAD") return yield* app
    const url = new URL(request.url, "http://localhost")
    const match = /^\/api\/(?:experimental\/)?session\/(ses_[^/]+)(?:\/|$)/.exec(url.pathname)
    const id = decodeSessionID(match?.[1])
    if (Option.isNone(id)) return yield* app
    const readThrough = request.method === "GET"
    if (readThrough && (!match?.[0].endsWith("/") || url.pathname.endsWith("/export"))) return yield* app
    const source = yield* sessions.mirrorSource(id.value)
    if (!source) return yield* app
    if (auth && !(yield* authorizedRequest(request, auth))) return unauthorizedResponse(request)

    const target = new URL(base)
    target.pathname = `/remote/${encodeURIComponent(source)}${url.pathname}`
    target.search = url.search
    const web = yield* HttpServerRequest.toWeb(request)
    const headers = new Headers(web.headers)
    headers.delete("authorization")
    headers.delete("proxy-authorization")
    headers.delete("cookie")
    headers.delete("host")
    headers.delete("connection")
    headers.delete("content-length")
    headers.set("authorization", `Bearer ${config.token}`)
    headers.set("accept-encoding", "identity")
    const forwarded = new Request(target, {
      method: web.method,
      headers,
      body: web.body,
      signal: web.signal,
      duplex: "half",
    } as RequestInit & { duplex: "half" })
    return yield* Effect.tryPromise(() => fetch(forwarded, { redirect: "manual" })).pipe(
      Effect.flatMap((response) => {
        if (!readThrough || ![404, 502, 503].includes(response.status))
          return Effect.succeed(HttpServerResponse.fromWeb(response))
        return Effect.promise(() => response.body?.cancel() ?? Promise.resolve()).pipe(Effect.andThen(app))
      }),
      Effect.catch(() =>
        readThrough
          ? app
          : Effect.succeed(HttpServerResponse.jsonUnsafe({ code: "remote_unavailable" }, { status: 502 })),
      ),
    )
  })
}
