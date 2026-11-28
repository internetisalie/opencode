export * as ShellEnvPlugin from "./shell-env.js"

import matter from "gray-matter"
import path from "path"
import { define } from "@opencode/plugin/effect/plugin"
import type { ShellCreateBefore } from "@opencode/plugin/effect/shell"
import type { Session } from "@opencode/schema/session"
import { FSUtil } from "@opencode/util/fs-util"
import { Effect, Schema } from "effect"

const SUPERVISOR_DIR = ".supervisor"
const MANIFEST_FILE = "manifest.json"
const CARD_FILE = ".loom.yml"

// A lane's session title is its address, `<project>.<lane>`.
const LANE_ADDRESS = /^([a-z0-9-]+)\.[a-z0-9-]+$/

const decodeManifest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({ members: Schema.Array(Schema.Struct({ dir: Schema.String, role: Schema.String })) }),
  ),
)
const decodeCard = Schema.decodeUnknownEffect(
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
    const warn = (what: string, file: string, detail: unknown) =>
      Effect.logWarning(`shell environment ${what}`, { file, detail: String(detail) }).pipe(Effect.as(undefined))

    const readText = (file: string) =>
      fs.readFileString(file).pipe(Effect.catch((error) => warn("could not read a file", file, error)))

    const parseCard = (file: string, card: string) =>
      Effect.try({
        // gray-matter parses the card's YAML; options bypass its module-global content cache.
        try: () => matter(`---\n${card}\n---\n`, {}).data,
        catch: (error) => error,
      }).pipe(
        Effect.flatMap((data) => decodeCard(data)),
        Effect.map((parsed) => parsed.profile.project.name),
        Effect.catch((error) => warn("could not use a card", file, error)),
      )

    // The project a dispatched seat works for: the card of the manifest's project member, in the nearest workspace
    // above the session's directory.
    const seatProject = Effect.fn("ShellEnvPlugin.seatProject")(function* (directory: string) {
      const found = yield* fs
        .up({ targets: [path.join(SUPERVISOR_DIR, MANIFEST_FILE)], start: directory, mode: "first" })
        .pipe(Effect.catch((error) => warn("could not search for a manifest", directory, error).pipe(Effect.as([]))))
      const manifestFile = found[0]
      if (manifestFile === undefined) return undefined
      const text = yield* readText(manifestFile)
      if (text === undefined) return undefined
      const manifest = yield* decodeManifest(text).pipe(
        Effect.catch((error) => warn("could not use a manifest", manifestFile, error)),
      )
      if (manifest === undefined) return undefined
      const members = manifest.members.filter((member) => member.role === "project")
      if (members.length !== 1)
        return yield* warn(
          "found a manifest without one project member",
          manifestFile,
          `${members.length} project members`,
        )
      const cardFile = path.join(path.dirname(path.dirname(manifestFile)), members[0]!.dir, CARD_FILE)
      const card = yield* readText(cardFile)
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
