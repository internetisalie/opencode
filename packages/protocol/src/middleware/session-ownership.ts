import { HttpApiMiddleware } from "effect/http-api"
import { ConflictError } from "../errors.js"

export class SessionOwnership extends HttpApiMiddleware.Service<SessionOwnership>()(
  "@opencode/HttpApiSessionOwnership",
  { error: ConflictError },
) {}
