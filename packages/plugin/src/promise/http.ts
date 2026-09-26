import type { Handler } from "../effect/http.js"

export interface HttpDomain {
  readonly register: (handler: Handler) => Promise<{ readonly dispose: () => Promise<void> }>
}
