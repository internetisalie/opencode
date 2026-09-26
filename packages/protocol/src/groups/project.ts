import { Project } from "@opencode/schema/project"
import { AbsolutePath } from "@opencode/schema/schema"
import { Schema, Struct } from "effect"
import { HttpApiEndpoint, HttpApiGroup, OpenApi } from "effect/unstable/httpapi"
import { InvalidRequestError, ProjectNotFoundError } from "../errors.js"

const root = "/api/project"
const UpdatePayload = Schema.Struct(Struct.omit(Project.UpdateInput.fields, ["projectID"]))
const DirectoryPayload = Schema.Struct(Struct.omit(Project.AssociateInput.fields, ["projectID"]))

export const ProjectGroup = HttpApiGroup.make("server.project")
  .add(
    HttpApiEndpoint.get("project.list", root, {
      success: Schema.Array(Project.Info),
    }).annotateMerge(
      OpenApi.annotations({
        identifier: "project.list",
        summary: "List projects",
        description: "List known projects.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.patch("project.update", `${root}/:projectID`, {
      params: { projectID: Project.ID },
      payload: UpdatePayload,
      success: Project.Info,
      error: ProjectNotFoundError,
    }).annotateMerge(
      OpenApi.annotations({
        identifier: "project.update",
        summary: "Update project",
        description: "Update the project canonical directory, display metadata, and workspace commands.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.get("project.directories", `${root}/:projectID/directories`, {
      params: { projectID: Project.ID },
      success: Project.Directories,
      error: ProjectNotFoundError,
    }).annotateMerge(
      OpenApi.annotations({
        identifier: "project.directories",
        summary: "List project directories",
        description: "List known worktrees and explicit directory associations for a project.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("project.directoryCreate", `${root}/:projectID/directories`, {
      params: { projectID: Project.ID },
      payload: DirectoryPayload,
      success: Project.Directories,
      error: [ProjectNotFoundError, InvalidRequestError],
    }).annotateMerge(
      OpenApi.annotations({
        identifier: "project.directoryCreate",
        summary: "Associate a directory with a project",
        description: "Associate this exact directory with a project. VCS discovery takes precedence.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.delete("project.directoryRemove", `${root}/:projectID/directories`, {
      params: { projectID: Project.ID },
      query: Schema.Struct({ directory: AbsolutePath }),
      success: Project.Directories,
      error: [ProjectNotFoundError, InvalidRequestError],
    }).annotateMerge(
      OpenApi.annotations({
        identifier: "project.directoryRemove",
        summary: "Remove a directory association",
        description: "Remove an exact directory association from the project.",
      }),
    ),
  )
  .annotateMerge(
    OpenApi.annotations({
      title: "project",
      description: "Project routes.",
    }),
  )
