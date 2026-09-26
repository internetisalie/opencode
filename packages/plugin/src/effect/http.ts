import type { Effect, Scope } from "effect"
import type { Registration } from "./registration.js"

/** Receives a request with `/api/plugins/<plugin-id>` removed from its URL. */
export interface Handler {
  readonly fetch: (request: Request) => Response | Promise<Response>
}

export interface HttpDomain {
  readonly register: (handler: Handler) => Effect.Effect<Registration, never, Scope.Scope>
}
