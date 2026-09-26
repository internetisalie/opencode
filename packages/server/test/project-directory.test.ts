import fs from "node:fs/promises"
import path from "node:path"
import { $ } from "bun"
import { expect } from "bun:test"
import { Effect } from "effect"
import { OpenCode } from "@opencode/client"
import { tmpdir } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { startServer } from "./fixture/server"

it.live(
  "associates a running markerless directory, keeps Git precedence, and restores classification after removal",
  () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.acquireDisposable(Effect.promise(() => tmpdir("opencode-project-association-")))
      const checkout = path.join(tmp.path, "checkout")
      const workspace = path.join(tmp.path, "workspace")
      const nested = path.join(workspace, "nested")
      const other = path.join(tmp.path, "other")
      yield* Effect.promise(async () => {
        await fs.mkdir(checkout)
        await fs.mkdir(nested, { recursive: true })
        await fs.mkdir(other)
        await $`git init`.cwd(checkout).quiet()
        await $`git config user.email test@opencode.test`.cwd(checkout).quiet()
        await $`git config user.name Test`.cwd(checkout).quiet()
        await $`git commit --allow-empty -m root`.cwd(checkout).quiet()
        await $`git init`.cwd(other).quiet()
      })
      const server = yield* startServer(path.join(tmp.path, "config"))
      const api = OpenCode.make({ baseUrl: server.base, headers: server.headers })
      yield* Effect.promise(async () => {
        const owner = await api.session.create({ location: { directory: checkout } })
        const before = await api.session.create({ location: { directory: workspace } })
        expect(before.projectID).not.toBe(owner.projectID)

        const list = await api.project.directoryCreate({
          projectID: owner.projectID,
          directory: workspace,
          strategy: "loom",
        })
        expect(list).toContainEqual({ directory: workspace, strategy: "loom" })
        expect(await api.project.directories({ projectID: owner.projectID })).toContainEqual({
          directory: workspace,
          strategy: "loom",
        })
        expect((await api.session.create({ location: { directory: workspace } })).projectID).toBe(owner.projectID)
        expect((await api.session.create({ location: { directory: nested } })).projectID).not.toBe(owner.projectID)
        await api.project.directoryCreate({ projectID: owner.projectID, directory: other })
        expect((await api.session.create({ location: { directory: other } })).projectID).not.toBe(owner.projectID)
        await expect(api.project.directoryCreate({ projectID: "missing", directory: workspace })).rejects.toMatchObject(
          {
            name: "ProjectNotFoundError",
          },
        )
        await expect(
          api.project.directoryCreate({ projectID: owner.projectID, directory: "relative" }),
        ).rejects.toMatchObject({
          name: "InvalidRequestError",
        })

        await api.project.directoryRemove({ projectID: owner.projectID, directory: workspace })
        expect((await api.session.create({ location: { directory: workspace } })).projectID).toBe(before.projectID)
      })
    }),
)
