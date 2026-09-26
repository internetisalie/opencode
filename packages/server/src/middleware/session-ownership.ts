import { SessionStore } from "@opencode/core/session/store"
import { Session } from "@opencode/schema/session"
import { ConflictError } from "@opencode/protocol/errors"
import { SessionOwnership } from "@opencode/protocol/middleware/session-ownership"
import { Effect, Layer, Option, Schema } from "effect"
import { HttpRouter, HttpServerRequest } from "effect/unstable/http"

const decodeSessionID = Schema.decodeUnknownOption(Session.ID)

export const sessionOwnershipLayer = Layer.effect(
  SessionOwnership,
  Effect.gen(function* () {
    const sessions = yield* SessionStore.Service
    return SessionOwnership.of((effect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        if (request.method === "GET" || request.method === "HEAD") return yield* effect
        const route = yield* HttpRouter.RouteContext
        const id = decodeSessionID(route.params.sessionID)
        if (Option.isNone(id)) return yield* effect
        if (!(yield* sessions.isMirror(id.value))) return yield* effect
        return yield* new ConflictError({
          resource: id.value,
          message: "This session is owned by a remote OpenCode instance",
        })
      }),
    )
  }),
)
