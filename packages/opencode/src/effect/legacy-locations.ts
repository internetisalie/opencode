import { Effect } from "effect"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Location } from "@opencode-ai/core/location"
import { registerDisposer } from "./instance-registry"

export const register = (locations: { readonly invalidate: (ref: Location.Ref) => Effect.Effect<void> }) =>
  Effect.acquireRelease(
    Effect.sync(() =>
      registerDisposer((directory) =>
        Effect.runPromise(locations.invalidate(Location.Ref.make({ directory: AbsolutePath.make(directory) }))),
      ),
    ),
    (off) => Effect.sync(off),
  ).pipe(Effect.asVoid)

export * as LegacyLocations from "./legacy-locations"
