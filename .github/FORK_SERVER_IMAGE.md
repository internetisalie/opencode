# Internetisalie OpenCode v2 server image

The `fork server` workflow runs for pull requests targeting `internetisalie-v2`, pushes to that branch, and tags matching `v2.*-internetisalie.*`. Manual runs validate and build but do not publish.

One Linux job typechecks `plugin`, `core`, and `server` in sequence, then runs the relevant project, session, plugin, and route tests one file at a time. It compiles one `opencode-linux-x64-baseline-musl` CLI target using `packages/cli/script/build.ts` with `--skip-web-ui`; OpenChamber supplies the UI in this deployment. The job builds a disposable Docker image and runs the binary's version check inside Alpine, where the musl runtime is available. The published server image uses that same binary; Docker does not compile OpenCode again. The image includes Bash, Git, and ripgrep, listens on port 4096, and starts `opencode serve`.

Only successful pushes to the fork publish `ghcr.io/internetisalie/opencode-stable`:

- An `internetisalie-v2` branch push writes the mutable `:internetisalie-v2` tag and an immutable `:sha-<12-character commit>` tag.
- A matching release tag writes `:<release tag>` and the same commit tag.

Pull requests and manual runs do not receive package write permission. The image is currently `linux/amd64` only. To support ARM workers, add a separate `linux-arm64-musl` binary build and a multi-platform Docker image after validating that target.

The fork bypasses upstream's broad Windows test matrix and issue-compliance automation. `fork server` is the Linux server gate for this branch; upstream's `check` workflow does not target `internetisalie-v2`.
