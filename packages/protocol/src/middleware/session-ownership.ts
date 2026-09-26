import { HttpApiMiddleware } from "effect/unstable/httpapi"
import { ConflictError } from "../errors.js"

export class SessionOwnership extends HttpApiMiddleware.Service<SessionOwnership>()(
  "@opencode/HttpApiSessionOwnership",
  { error: ConflictError },
) {}
