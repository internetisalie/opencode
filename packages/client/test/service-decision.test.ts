import { expect, test } from "bun:test"
import os from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { decide, type LocalService } from "../src/service-probe"

const protocolError =
  "Background service uses an incompatible health protocol. Update this client or explicitly restart the service."

test.each([
  ["selected-state", "xdg-state", "selected-state"],
  [undefined, "xdg-state", "xdg-state"],
  ["", "xdg-state", "xdg-state"],
  [undefined, undefined, path.join("home", ".local", "state")],
  ["", "", path.join("home", ".local", "state")],
] as const)("fallback resolves opencode state %p and XDG state %p", (opencode, xdg, expected) => {
  const root = path.join(os.tmpdir(), "opencode-service-fallback")
  const module = pathToFileURL(path.join(import.meta.dir, "../src/service-probe.ts")).href
  const result = Bun.spawnSync({
    cmd: [
      process.execPath,
      "-e",
      `const { fallback } = await import(${JSON.stringify(module)}); console.log(JSON.stringify(fallback()))`,
    ],
    env: {
      ...process.env,
      HOME: path.join(root, "home"),
      USERPROFILE: path.join(root, "home"),
      OPENCODE_DATA_HOME: undefined,
      OPENCODE_CACHE_HOME: undefined,
      OPENCODE_STATE_HOME: opencode ? path.join(root, opencode) : opencode,
      XDG_DATA_HOME: undefined,
      XDG_CACHE_HOME: undefined,
      XDG_CONFIG_HOME: undefined,
      XDG_STATE_HOME: xdg ? path.join(root, xdg) : xdg,
    },
    stdout: "pipe",
    stderr: "pipe",
  })
  expect(result.exitCode, result.stderr.toString()).toBe(0)
  expect(JSON.parse(result.stdout.toString())).toBe(path.join(root, expected, "opencode", "service.json"))
})

function service(input: Pick<LocalService, "state" | "compatible" | "version">) {
  return {
    info: { url: "http://127.0.0.1:4096", pid: 4242, version: input.version },
    endpoint: { url: "http://127.0.0.1:4096" },
    ...input,
  } satisfies LocalService
}

test("a missing health endpoint fails unless the caller requires a different version", () => {
  const missing = service({ version: "2.0.0", state: "ready", compatible: false })
  expect(decide(missing, {})).toEqual({ _tag: "fail", error: new Error(protocolError) })
  expect(decide(missing, { version: "2.0.0" })).toEqual({ _tag: "fail", error: new Error(protocolError) })
  expect(decide(missing, { version: (version) => version.startsWith("2.") })).toEqual({
    _tag: "fail",
    error: new Error(protocolError),
  })
  expect(decide(missing, { version: "3.0.0" })).toEqual({ _tag: "replace", pty: "handoff" })
  expect(decide(service({ state: "ready", compatible: false }), { version: "3.0.0" })).toEqual({
    _tag: "replace",
    pty: "handoff",
  })
})

test("a compatible service is reused when ready, awaited while starting, and fails when it failed", () => {
  expect(decide(service({ version: "2.0.0", state: "ready", compatible: true }), {})).toEqual({ _tag: "reuse" })
  expect(decide(service({ version: "2.0.0", state: "waiting", compatible: true }), { version: "2.0.0" })).toEqual({
    _tag: "wait",
  })
  expect(decide(service({ version: "2.0.0", state: "failed", compatible: true }), {})).toEqual({
    _tag: "fail",
    error: new Error("Background service failed to start"),
  })
})

test("a version mismatch replaces the service, handing off terminals only when it is ready", () => {
  expect(decide(service({ version: "1.0.0", state: "ready", compatible: true }), { version: "2.0.0" })).toEqual({
    _tag: "replace",
    pty: "handoff",
  })
  expect(decide(service({ version: "1.0.0", state: "waiting", compatible: true }), { version: "2.0.0" })).toEqual({
    _tag: "replace",
    pty: "clear",
  })
  expect(decide(service({ version: "1.0.0", state: "failed", compatible: true }), { version: "2.0.0" })).toEqual({
    _tag: "replace",
    pty: "clear",
  })
})
