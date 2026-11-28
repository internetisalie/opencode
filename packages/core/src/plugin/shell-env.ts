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

export const Plugin = define({
  id: "opencode.shell.env",
  effect: Effect.fn("ShellEnvPlugin")(function* (ctx) {
    const fs = yield* FSUtil.Service

    // A workspace file that cannot be used never stops a shell call: the file is named in a warning and the session
    // has no project.
    const warn = (what: string, file: string, error: unknown) =>
      Effect.logWarning(`shell environment ${what}`, { file, error: String(error) }).pipe(Effect.as(undefined))

    const readSafe = (file: string) =>
      fs.readFileStringSafe(file).pipe(Effect.catch((error) => warn("could not read a file", file, error)))

    const parseCard = (file: string, card: string) =>
      Effect.try({
        // gray-matter parses the card's YAML; options bypass its module-global content cache.
        try: () => matter(`---\n${card}\n---\n`, {}).data,
        catch: (error) => error,
      }).pipe(
        Effect.catch((error) => warn("could not parse a card", file, error)),
        Effect.flatMap((data) => {
          if (data === undefined) return Effect.succeed(undefined)
          const parsed = decodeCard(data)
          return Option.isSome(parsed)
            ? Effect.succeed(parsed.value.profile.project.name)
            : warn("found a card without profile.project.name", file, "schema decode failed")
        }),
      )

    // The project a dispatched seat works for: the card of the manifest's project member, in the nearest workspace
    // above the session's directory.
    const seatProject = Effect.fn("ShellEnvPlugin.seatProject")(function* (directory: string) {
      const found = yield* fs
        .up({ targets: [path.join(SUPERVISOR_DIR, MANIFEST_FILE)], start: directory, mode: "first" })
        .pipe(Effect.catch((error) => warn("could not search for a manifest", directory, error).pipe(Effect.as([]))))
      const manifestFile = found[0]
      if (manifestFile === undefined) return undefined
      const text = yield* readSafe(manifestFile)
      if (text === undefined) return undefined
      const manifest = decodeManifest(text)
      if (Option.isNone(manifest))
        return yield* warn("found a manifest that does not decode", manifestFile, "schema decode failed")
      const members = manifest.value.members.filter((member) => member.role === "project")
      if (members.length !== 1) return undefined
      const container = path.dirname(path.dirname(manifestFile))
      const cardFile = path.join(container, members[0]!.dir, CARD_FILE)
      const card = yield* readSafe(cardFile)
      if (card === undefined) return undefined
      return yield* parseCard(cardFile, card)
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
