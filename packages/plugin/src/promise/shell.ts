import type { Session } from "@opencode/schema/session"
import type { Hooks } from "./registration.js"

export interface ShellCreateBefore {
  /** The Session the command runs in. Absent when the call has none. */
  readonly sessionID?: Session.ID
  command: string
  cwd: string
  timeout: number
  shell: string
  env: Record<string, string | undefined>
}

export interface ShellHooks {
  readonly "create.before": ShellCreateBefore
}

export interface ShellDomain {
  readonly hook: Hooks<ShellHooks>
}
