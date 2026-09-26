import { Project } from "@opencode/core/project"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { InvalidRequestError, ProjectNotFoundError } from "@opencode/protocol/errors"
import { FSUtil } from "@opencode/util/fs-util"
import { Effect, RcMap } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { isAbsolute } from "node:path"
import { Api } from "../api"

export const ProjectHandler = HttpApiBuilder.group(Api, "server.project", (handlers) =>
  Effect.gen(function* () {
    const project = yield* Project.Service
    const fs = yield* FSUtil.Service
    const locations = yield* LocationServiceMap.Service
    const invalidateDirectory = Effect.fn("ProjectHttpApi.invalidateDirectory")(function* (directory: string) {
      const canonical = yield* fs.resolve(directory)
      const refs = Array.from(yield* RcMap.keys(locations.rcMap))
      const matching = yield* Effect.filter(refs, (ref) =>
        fs.resolve(ref.directory).pipe(Effect.map((value) => value === canonical)),
      )
      yield* Effect.forEach(matching, (ref) => locations.invalidate(ref), { discard: true })
    })

    return handlers
      .handle("project.list", () => project.list())
      .handle("project.update", (ctx) =>
        project
          .update({ ...ctx.payload, projectID: ctx.params.projectID })
          .pipe(Effect.catchTag("Project.NotFoundError", missingProject)),
      )
      .handle("project.directories", (ctx) =>
        project.directories(ctx.params.projectID).pipe(Effect.catchTag("Project.NotFoundError", missingProject)),
      )
      .handle("project.directoryCreate", (ctx) =>
        Effect.gen(function* () {
          if (!isAbsolute(ctx.payload.directory))
            return yield* new InvalidRequestError({ message: "Directory must be absolute", field: "directory" })
          const result = yield* project
            .associate({ ...ctx.payload, projectID: ctx.params.projectID })
            .pipe(Effect.catchTag("Project.NotFoundError", missingProject))
          yield* invalidateDirectory(ctx.payload.directory)
          return result
        }),
      )
      .handle("project.directoryRemove", (ctx) =>
        Effect.gen(function* () {
          if (!isAbsolute(ctx.query.directory))
            return yield* new InvalidRequestError({ message: "Directory must be absolute", field: "directory" })
          const result = yield* project
            .dissociate({ ...ctx.query, projectID: ctx.params.projectID })
            .pipe(Effect.catchTag("Project.NotFoundError", missingProject))
          yield* invalidateDirectory(ctx.query.directory)
          return result
        }),
      )
  }),
)

function missingProject(error: Project.NotFoundError) {
  return Effect.fail(
    new ProjectNotFoundError({ projectID: error.projectID, message: `Project not found: ${error.projectID}` }),
  )
}
