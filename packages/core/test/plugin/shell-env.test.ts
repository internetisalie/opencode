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

// The warnings an effect logs, each as the logged values: the message, then its fields.
const withWarnings = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const warnings: unknown[] = []
    const value = yield* effect.pipe(
      Effect.provide(
        Logger.layer([
          Logger.make((entry) => {
            if (entry.logLevel === "Warn") warnings.push(entry.message)
          }),
        ]),
      ),
    )
    return { value, warnings }
  })

type Warned = { message: string; file: string; detail: unknown }

// A workspace file the plugin cannot use never stops the shell call: the session and the harness are still set, the
// session has no project, and one warning names the file and what was wrong with it. `warned` is the whole warning,
// with `file` relative to the workspace root; undefined where nothing may be logged.
const unusable = (
  name: string,
  files: { manifest?: string; card?: string },
  warned: Warned | undefined,
  damage: (root: string) => Promise<void> = async () => {},
  repair: (root: string) => Promise<void> = async () => {},
  test: typeof it.live = it.live,
) =>
  test(`${name} leaves the shell call running with no project`, () =>
    withTempDir((tmp) =>
      Effect.gen(function* () {
        const { create, shellEnv } = yield* setup
        yield* workspace(tmp.path, "loom-delivery", files)
        yield* Effect.promise(() => damage(tmp.path))
        const seat = yield* create("implementer", path.join(tmp.path, "project-member"))
        const result = yield* withWarnings(shellEnv(seat.id)).pipe(
          Effect.ensuring(Effect.promise(() => repair(tmp.path))),
        )
        expect(result.value).toEqual(noProject(seat.id))
        expect(result.warnings).toEqual(
          warned === undefined
            ? []
            : [[warned.message, { file: path.join(tmp.path, warned.file), detail: warned.detail }]],
        )
      }),
    ),
  )

const MANIFEST = ".supervisor/manifest.json"
const CARD = "project-member/.loom.yml"

unusable(
  "a card with a YAML syntax error",
  { card: "profile:\n  project: { name: x\n  bad: [unclosed\n" },
  { message: "shell environment could not use a card", file: CARD, detail: expect.stringContaining("YAMLException") },
)
unusable(
  "a card without profile.project.name",
  { card: "profile:\n  harness: opencode\n" },
  { message: "shell environment could not use a card", file: CARD, detail: expect.stringContaining("project") },
)
unusable(
  "a manifest that is not JSON",
  { manifest: "{ not json" },
  { message: "shell environment could not use a manifest", file: MANIFEST, detail: expect.stringContaining("JSON") },
)
unusable(
  "a manifest whose members are not a list",
  { manifest: JSON.stringify({ members: "project-member" }) },
  { message: "shell environment could not use a manifest", file: MANIFEST, detail: expect.stringContaining("members") },
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
  { message: "shell environment found a manifest without one project member", file: MANIFEST, detail: "2 project members" },
)
unusable(
  "a manifest with no project member",
  { manifest: JSON.stringify({ members: [{ dir: "base-member", role: "base" }] }) },
  { message: "shell environment found a manifest without one project member", file: MANIFEST, detail: "0 project members" },
)
unusable(
  "a manifest that cannot be read",
  {},
  { message: "shell environment could not read a file", file: MANIFEST, detail: expect.any(String) },
  async (root) => {
    await fs.rm(path.join(root, ".supervisor", "manifest.json"))
    await fs.mkdir(path.join(root, ".supervisor", "manifest.json"))
  },
)
unusable(
  "a card that cannot be read",
  {},
  { message: "shell environment could not read a file", file: CARD, detail: expect.any(String) },
  async (root) => {
    await fs.rm(path.join(root, "project-member", ".loom.yml"))
    await fs.mkdir(path.join(root, "project-member", ".loom.yml"))
  },
)
unusable(
  "a card that does not exist",
  {},
  { message: "shell environment could not read a file", file: CARD, detail: expect.stringContaining("NotFound") },
  async (root) => {
    await fs.rm(path.join(root, "project-member", ".loom.yml"))
  },
)

// Permission modes do not bind root.
const permissioned = process.getuid?.() === 0 ? it.live.skip : it.live
unusable(
  "a manifest the process may not read",
  {},
  { message: "shell environment could not read a file", file: MANIFEST, detail: expect.stringContaining("PermissionDenied") },
  (root) => fs.chmod(path.join(root, ".supervisor", "manifest.json"), 0),
  (root) => fs.chmod(path.join(root, ".supervisor", "manifest.json"), 0o644),
  permissioned,
)
unusable(
  "a card the process may not read",
  {},
  { message: "shell environment could not read a file", file: CARD, detail: expect.stringContaining("PermissionDenied") },
  (root) => fs.chmod(path.join(root, "project-member", ".loom.yml"), 0),
  (root) => fs.chmod(path.join(root, "project-member", ".loom.yml"), 0o644),
  permissioned,
)
// The search for the manifest fails where a directory on the way cannot be searched; the warning names the session's
// directory, the search's start.
unusable(
  "a workspace directory the process may not search",
  {},
  { message: "shell environment could not search for a manifest", file: "project-member", detail: expect.stringContaining("PermissionDenied") },
  (root) => fs.chmod(path.join(root, ".supervisor"), 0),
  (root) => fs.chmod(path.join(root, ".supervisor"), 0o755),
  permissioned,
)

// A title is a lane address only when the whole title is `<project>.<lane>`.
for (const title of ["x.y z", "a.b.c", "see x.y"]) {
  it.live(`the title "${title}" is not a lane address`, () =>
    withTempDir((tmp) =>
      Effect.gen(function* () {
        const { create, shellEnv } = yield* setup
        const session = yield* create(title, tmp.path)
        expect(yield* shellEnv(session.id)).toEqual(noProject(session.id))
      }),
    ),
  )
}

it.live("a session id that names no session leaves the shell call running with the session and harness", () =>
  Effect.gen(function* () {
    const { shellEnv } = yield* setup
    const missing = Session.ID.make("ses_missing_shell_env")
    expect(yield* shellEnv(missing)).toEqual(noProject(missing))
  }),
)
