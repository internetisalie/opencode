import { describe, expect, test } from "bun:test"
import os from "os"
import path from "path"
import { pathToFileURL } from "url"

const module = pathToFileURL(path.join(import.meta.dir, "../src/global-roots.ts")).href

describe("global roots", () => {
  test.each([undefined, ""])("uses XDG overrides when opencode overrides are %p", (override) => {
    const root = path.join(os.tmpdir(), "opencode-xdg-overrides")
    const env = {
      XDG_DATA_HOME: path.join(root, "data"),
      XDG_CACHE_HOME: path.join(root, "cache"),
      XDG_CONFIG_HOME: path.join(root, "config"),
      XDG_STATE_HOME: path.join(root, "state"),
    }

    expect(
      run({ ...env, OPENCODE_DATA_HOME: override, OPENCODE_CACHE_HOME: override, OPENCODE_STATE_HOME: override }),
    ).toEqual({
      data: path.join(env.XDG_DATA_HOME, "opencode"),
      cache: path.join(env.XDG_CACHE_HOME, "opencode"),
      config: path.join(env.XDG_CONFIG_HOME, "opencode"),
      state: path.join(env.XDG_STATE_HOME, "opencode"),
      tmp: path.join(os.tmpdir(), "opencode"),
    })
  })

  test.each(["data", "cache", "state"] as const)("opencode %s overrides only its matching XDG root", (root) => {
    const base = path.join(os.tmpdir(), "opencode-root-precedence")
    const selected = path.join(base, "selected", root)
    expect(
      run({
        XDG_DATA_HOME: path.join(base, "data"),
        XDG_CACHE_HOME: path.join(base, "cache"),
        XDG_CONFIG_HOME: path.join(base, "config"),
        XDG_STATE_HOME: path.join(base, "state"),
        [`OPENCODE_${root.toUpperCase()}_HOME`]: selected,
      }),
    ).toEqual({
      data: path.join(base, "data", "opencode"),
      cache: path.join(base, "cache", "opencode"),
      config: path.join(base, "config", "opencode"),
      state: path.join(base, "state", "opencode"),
      tmp: path.join(os.tmpdir(), "opencode"),
      [root]: path.join(selected, "opencode"),
    })
  })

  test.each([
    [undefined, undefined],
    [undefined, ""],
    ["", undefined],
    ["", ""],
  ])("opencode %p and XDG %p use home directory defaults", (opencode, xdg) => {
    const home = path.join(os.tmpdir(), "opencode-xdg-home")

    expect(
      run({
        OPENCODE_DATA_HOME: opencode,
        OPENCODE_CACHE_HOME: opencode,
        OPENCODE_STATE_HOME: opencode,
        XDG_DATA_HOME: xdg,
        XDG_CACHE_HOME: xdg,
        XDG_CONFIG_HOME: xdg,
        XDG_STATE_HOME: xdg,
        ...(process.platform === "win32" ? { USERPROFILE: home } : { HOME: home }),
      }),
    ).toEqual({
      data: path.join(home, ".local", "share", "opencode"),
      cache: path.join(home, ".cache", "opencode"),
      config: path.join(home, ".config", "opencode"),
      state: path.join(home, ".local", "state", "opencode"),
      tmp: path.join(os.tmpdir(), "opencode"),
    })
  })
})

function run(env: Record<string, string | undefined>) {
  const result = Bun.spawnSync({
    cmd: [
      process.execPath,
      "-e",
      `const { roots } = await import(${JSON.stringify(module)}); console.log(JSON.stringify(roots("opencode")))`,
    ],
    env: {
      ...process.env,
      OPENCODE_DATA_HOME: undefined,
      OPENCODE_CACHE_HOME: undefined,
      OPENCODE_STATE_HOME: undefined,
      XDG_DATA_HOME: undefined,
      XDG_CACHE_HOME: undefined,
      XDG_CONFIG_HOME: undefined,
      XDG_STATE_HOME: undefined,
      ...env,
    },
    stdout: "pipe",
    stderr: "pipe",
  })

  expect(result.exitCode, result.stderr.toString()).toBe(0)
  return JSON.parse(result.stdout.toString())
}
