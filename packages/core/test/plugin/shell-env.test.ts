import fs from "fs/promises"
import path from "path"
import { expect } from "bun:test"
import { Plugin } from "@opencode/core/plugin"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { PluginHost } from "@opencode/core/plugin/host"
import { ShellEnvPlugin } from "@opencode/core/plugin/shell-env"
import { Session } from "@opencode/core/session"
import { AbsolutePath } from "@opencode/core/schema"
import { Location } from "@opencode/core/location"
import { Effect, Logger } from "effect"
import { testEffect } from "../lib/effect"
import { withTempDir } from "../fixture/tmpdir"
import { PluginTestLayer } from "./fixture"

const it = testEffect(PluginTestLayer)

type Env = Record<string, string | undefined>

// A workspace as materialization leaves it: a manifest under .supervisor and the project member's card.
const workspace = (
  root: string,
  project: string,
  // Replacements for the manifest text and the card text, to build a workspace whose files do not decode.
  files: { manifest?: string; card?: string } = {},
) =>
  Effect.promise(async () => {
    await fs.mkdir(path.join(root, ".supervisor"), { recursive: true })
    await fs.mkdir(path.join(root, "base-member"), { recursive: true })
    await fs.mkdir(path.join(root, "project-member"), { recursive: true })
    await fs.writeFile(
      path.join(root, ".supervisor", "manifest.json"),
      files.manifest ??
        JSON.stringify({
          version: 1,
          members: [
            { dir: "base-member", role: "base" },
            { dir: "project-member", role: "project" },
          ],
        }),
    )
    await fs.writeFile(
      path.join(root, "project-member", ".loom.yml"),
      files.card ?? `profile:\n  project: { name: ${project}, repository: ${project} }\n`,
    )
  })

// What a shell call still gets when the workspace gives no project: the session and the harness.
const noProject = (sessionID: Session.ID) => ({ LOOM_SESSION_ID: sessionID, LOOM_HARNESS: "opencode" })

const setup = Effect.gen(function* () {
  const plugins = yield* Plugin.Service
  const sessions = yield* Session.Service
  const hooks = yield* PluginHooks.Service
  const context = yield* PluginHost.make(plugins, "test")
  yield* ShellEnvPlugin.Plugin.effect(context)
  const create = (title: string, directory: string) =>
    sessions.create({ title, location: Location.Ref.make({ directory: AbsolutePath.make(directory) }) })
  const shellEnv = (sessionID: Session.ID | undefined, env: Env = {}) =>
    Effect.gen(function* () {
      const payload = { command: "true", cwd: ".", timeout: 0, shell: "sh", env, ...(sessionID ? { sessionID } : {}) }
      yield* hooks.trigger("shell", "create.before", payload)
      return payload.env
    })
  return { create, shellEnv }
})

it.live("a seat session reads its project from the workspace manifest and has no lane", () =>
  withTempDir((tmp) =>
    Effect.gen(function* () {
      const { create, shellEnv } = yield* setup
      yield* workspace(tmp.path, "loom-delivery")
      const seat = yield* create("implementer", path.join(tmp.path, "project-member", "subdir"))
      expect(yield* shellEnv(seat.id)).toEqual({
        LOOM_PROJECT: "loom-delivery",
        LOOM_SESSION_ID: seat.id,
        LOOM_HARNESS: "opencode",
      })
    }),
  ),
)

it.live("concurrent shell calls in different workspaces each read their own session and project", () =>
  withTempDir((tmp) =>
    Effect.gen(function* () {
      const { create, shellEnv } = yield* setup
      const first = path.join(tmp.path, "first")
      const second = path.join(tmp.path, "second")
      yield* workspace(first, "alpha")
      yield* workspace(second, "beta")
      const a = yield* create("seat a", first)
      const b = yield* create("seat b", second)
      const results = yield* Effect.all(
        Array.from({ length: 20 }, (_, index) => shellEnv(index % 2 === 0 ? a.id : b.id)),
        { concurrency: "unbounded" },
      )
      expect(results).toEqual(
        Array.from({ length: 20 }, (_, index) =>
          index % 2 === 0
            ? { LOOM_PROJECT: "alpha", LOOM_SESSION_ID: a.id, LOOM_HARNESS: "opencode" }
            : { LOOM_PROJECT: "beta", LOOM_SESSION_ID: b.id, LOOM_HARNESS: "opencode" },
        ),
      )
    }),
  ),
)

it.live("a session with no manifest and no lane title has no project and no lane", () =>
  withTempDir((tmp) =>
    Effect.gen(function* () {
      const { create, shellEnv } = yield* setup
      const plain = yield* create("Fix the README.md", tmp.path)
      // The server's own environment may carry a lane; the session does not inherit it.
      expect(yield* shellEnv(plain.id, { LOOM_LANE: "loom.other", LOOM_PROJECT: "other", PATH: "/bin" })).toEqual({
        PATH: "/bin",
        LOOM_SESSION_ID: plain.id,
        LOOM_HARNESS: "opencode",
      })
    }),
  ),
)

it.live("a lane-titled session reads its address and project from the title", () =>
  withTempDir((tmp) =>
    Effect.gen(function* () {
      const { create, shellEnv } = yield* setup
      yield* workspace(tmp.path, "from-manifest")
      const lane = yield* create("loom.opencode", tmp.path)
      expect(yield* shellEnv(lane.id)).toEqual({
        LOOM_LANE: "loom.opencode",
        LOOM_PROJECT: "loom",
        LOOM_SESSION_ID: lane.id,
        LOOM_HARNESS: "opencode",
      })
    }),
  ),
)

it.live("a shell call with no session id leaves the environment unchanged", () =>
  Effect.gen(function* () {
    const { shellEnv } = yield* setup
    expect(yield* shellEnv(undefined, { PATH: "/bin", LOOM_LANE: "kept.lane" })).toEqual({
      PATH: "/bin",
      LOOM_LANE: "kept.lane",
    })
  }),
)

// A workspace file the plugin cannot use never stops the shell call: the session and the harness are still set and
// the session has no project.
// The warnings an effect logs, as text.
const withWarnings = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const warnings: string[] = []
    const value = yield* effect.pipe(
      Effect.provide(
        Logger.layer([
          Logger.make((entry) => {
            if (entry.logLevel === "Warn") warnings.push(JSON.stringify(entry.message))
          }),
        ]),
      ),
    )
    return { value, warnings }
  })

// `warned` is the workspace file the warning names; undefined where the workspace is valid and no file is at fault.
const unusable = (
  name: string,
  files: { manifest?: string; card?: string },
  warned: ".supervisor/manifest.json" | "project-member/.loom.yml" | undefined,
  damage: (root: string) => Promise<void> = async () => {},
) =>
  it.live(`${name} leaves the shell call running with no project`, () =>
    withTempDir((tmp) =>
      Effect.gen(function* () {
        const { create, shellEnv } = yield* setup
        yield* workspace(tmp.path, "loom-delivery", files)
        yield* Effect.promise(() => damage(tmp.path))
        const seat = yield* create("implementer", path.join(tmp.path, "project-member"))
        const result = yield* withWarnings(shellEnv(seat.id))
        expect(result.value).toEqual(noProject(seat.id))
        if (warned === undefined) return
        expect(result.warnings).toHaveLength(1)
        expect(result.warnings[0]).toContain(JSON.stringify(path.join(tmp.path, warned)).slice(1, -1))
      }),
    ),
  )

unusable("a card with a YAML syntax error", { card: "profile:\n  project: { name: x\n  bad: [unclosed\n" }, "project-member/.loom.yml")
unusable("a card without profile.project.name", { card: "profile:\n  harness: opencode\n" }, "project-member/.loom.yml")
unusable("a manifest that is not JSON", { manifest: "{ not json" }, ".supervisor/manifest.json")
unusable(
  "a manifest whose members are not a list",
  { manifest: JSON.stringify({ members: "project-member" }) },
  ".supervisor/manifest.json",
)
unusable(
  "a manifest with two project members",
  {
    manifest: JSON.stringify({
      members: [
        { dir: "project-member", role: "project" },
        { dir: "base-member", role: "project" },
      ],
    }),
  },
  undefined,
)
unusable("a manifest that cannot be read", {}, ".supervisor/manifest.json", async (root) => {
  await fs.rm(path.join(root, ".supervisor", "manifest.json"))
  await fs.mkdir(path.join(root, ".supervisor", "manifest.json"))
})
unusable("a card that cannot be read", {}, "project-member/.loom.yml", async (root) => {
  await fs.rm(path.join(root, "project-member", ".loom.yml"))
  await fs.mkdir(path.join(root, "project-member", ".loom.yml"))
})

it.live("a session id that names no session leaves the shell call running with the session and harness", () =>
  Effect.gen(function* () {
    const { shellEnv } = yield* setup
    const missing = Session.ID.make("ses_missing_shell_env")
    expect(yield* shellEnv(missing)).toEqual(noProject(missing))
  }),
)
