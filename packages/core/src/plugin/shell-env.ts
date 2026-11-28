export * as ShellEnvPlugin from "./shell-env.js"

import matter from "gray-matter"
import path from "path"
import { define } from "@opencode/plugin/effect/plugin"
import type { ShellCreateBefore } from "@opencode/plugin/effect/shell"
import type { Session } from "@opencode/schema/session"
import { FSUtil } from "@opencode/util/fs-util"
import { Effect, Option, Schema } from "effect"

const SUPERVISOR_DIR = ".supervisor"
const MANIFEST_FILE = "manifest.json"
const CARD_FILE = ".loom.yml"

// A lane's session title is its address, `<project>.<lane>`.
const LANE_ADDRESS = /^([a-z0-9-]+)\.[a-z0-9-]+$/

const decodeManifest = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({ members: Schema.Array(Schema.Struct({ dir: Schema.String, role: Schema.String })) }),
  ),
)
const decodeCard = Schema.decodeUnknownOption(
  Schema.Struct({ profile: Schema.Struct({ project: Schema.Struct({ name: Schema.String }) }) }),
)

export const laneFromTitle = (title: string | undefined) => {
  const match = title === undefined ? null : LANE_ADDRESS.exec(title)
  return match ? { lane: title!, project: match[1]! } : undefined
}

const ancestors = (directory: string) => {
  const chain = [directory]
  for (let parent = path.dirname(directory); parent !== chain[chain.length - 1]; parent = path.dirname(parent))
    chain.push(parent)
  return chain
}

export const Plugin = define({
  id: "opencode.shell.env",
  effect: Effect.fn("ShellEnvPlugin")(function* (ctx) {
    const fs = yield* FSUtil.Service

    const readSafe = (file: string) =>
      fs.readFileStringSafe(file).pipe(
        Effect.catch((error) =>
          Effect.logWarning("shell environment could not read a file", { file, error: String(error) }).pipe(
            Effect.as(undefined),
          ),
        ),
      )

    // The project a dispatched seat works for: the card of the manifest's project member, in the workspace that holds
    // the session's directory.
    const seatProject = Effect.fn("ShellEnvPlugin.seatProject")(function* (directory: string) {
      for (const container of ancestors(directory)) {
        const text = yield* readSafe(path.join(container, SUPERVISOR_DIR, MANIFEST_FILE))
        if (text === undefined) continue
        const manifest = Option.getOrUndefined(decodeManifest(text))
        const members = manifest?.members.filter((member) => member.role === "project") ?? []
        if (members.length !== 1) return undefined
        const card = yield* readSafe(path.join(container, members[0]!.dir, CARD_FILE))
        if (card === undefined) return undefined
        // gray-matter parses the card's YAML; options bypass its module-global content cache.
        const parsed = Option.getOrUndefined(decodeCard(matter(`---\n${card}\n---\n`, {}).data))
        return parsed?.profile.project.name
      }
      return undefined
    })

    const origin = Effect.fn("ShellEnvPlugin.origin")(function* (sessionID: Session.ID) {
      const session = yield* ctx.session.get({ sessionID }).pipe(
        Effect.catch((error) =>
          Effect.logWarning("shell environment could not read the session", {
            sessionID,
            error: String(error),
          }).pipe(Effect.as(undefined)),
        ),
      )
      if (!session) return {}
      const lane = laneFromTitle(session.title)
      if (lane) return lane
      const project = yield* seatProject(session.location.directory)
      return project === undefined ? {} : { project }
    })

    yield* ctx.shell.hook("create.before", (event: ShellCreateBefore) =>
      Effect.gen(function* () {
        if (event.sessionID === undefined) return
        delete event.env.LOOM_LANE
        delete event.env.LOOM_PROJECT
        const found: { lane?: string; project?: string } = yield* origin(event.sessionID)
        event.env.LOOM_SESSION_ID = event.sessionID
        event.env.LOOM_HARNESS = "opencode"
        if (found.lane !== undefined) event.env.LOOM_LANE = found.lane
        if (found.project !== undefined) event.env.LOOM_PROJECT = found.project
      }),
    )
  }),
})
