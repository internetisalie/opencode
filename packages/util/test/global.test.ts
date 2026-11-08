import { describe, expect, test } from "bun:test"
import fs from "fs"
import os from "os"
import path from "path"
import { pathToFileURL } from "url"
import { Context, Effect, Layer } from "effect"
import { Global } from "../src/global.js"

describe("global", () => {
  test("importing the module does not create directories", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-global-import-"))
    const directories = ["data", "cache", "config", "state", "tmp"].map((directory) => path.join(root, directory))
    const module = pathToFileURL(path.join(import.meta.dir, "../src/global.ts")).href
    const result = Bun.spawnSync({
      cmd: [process.execPath, "-e", `const { Global } = await import(${JSON.stringify(module)}); void Global.Path.tmp`],
      env: {
        ...process.env,
        OPENCODE_DATA_HOME: undefined,
        OPENCODE_CACHE_HOME: undefined,
        OPENCODE_STATE_HOME: undefined,
        OPENCODE_CONFIG_DIR: undefined,
        XDG_DATA_HOME: directories[0],
        XDG_CACHE_HOME: directories[1],
        XDG_CONFIG_HOME: directories[2],
        XDG_STATE_HOME: directories[3],
        // os.tmpdir() reads TMPDIR on POSIX and TEMP on Windows.
        TMPDIR: directories[4],
        TEMP: directories[4],
      },
      stderr: "pipe",
    })

    expect(result.exitCode, result.stderr.toString()).toBe(0)
    directories.forEach((directory) => expect(fs.existsSync(path.join(directory, "opencode"))).toBe(false))
    fs.rmSync(root, { recursive: true, force: true })
  })

  test("building layerWith creates service directories and preserves an explicit tmp", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-global-layer-"))
    const directories = {
      data: path.join(root, "data"),
      cache: path.join(root, "cache"),
      config: path.join(root, "config"),
      state: path.join(root, "state"),
      log: path.join(root, "log"),
      bin: path.join(root, "cache", "bin"),
      repos: path.join(root, "repos"),
      tmp: path.join(root, "nested", "..", "tmp"),
    }

    const context = await Effect.runPromise(Effect.scoped(Layer.build(Global.layerWith(directories))))

    Object.values(directories).forEach((directory) => expect(fs.statSync(directory).isDirectory()).toBe(true))
    expect(Context.get(context, Global.Service)).toEqual({ home: Global.Path.home, ...directories })
    fs.rmSync(root, { recursive: true, force: true })
  })

  test("building a layer with default tmp creates and canonicalizes it", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-global-layer-"))
    const directories = ["data", "cache", "config", "state", "tmp"].map((directory) => path.join(root, directory))
    const module = pathToFileURL(path.join(import.meta.dir, "../src/global.ts")).href
    const result = Bun.spawnSync({
      cmd: [
        process.execPath,
        "-e",
        `
          import { Context, Effect, Layer } from "effect"
          const { Global } = await import(${JSON.stringify(module)})
          const context = await Effect.runPromise(Effect.scoped(Layer.build(Global.layerWith({}))))
          process.stdout.write(Context.get(context, Global.Service).tmp)
        `,
      ],
      cwd: path.join(import.meta.dir, ".."),
      env: {
        ...process.env,
        OPENCODE_DATA_HOME: undefined,
        OPENCODE_CACHE_HOME: undefined,
        OPENCODE_STATE_HOME: undefined,
        OPENCODE_CONFIG_DIR: undefined,
        XDG_DATA_HOME: directories[0],
        XDG_CACHE_HOME: directories[1],
        XDG_CONFIG_HOME: directories[2],
        XDG_STATE_HOME: directories[3],
        // os.tmpdir() reads TMPDIR on POSIX and TEMP on Windows.
        TMPDIR: directories[4],
        TEMP: directories[4],
      },
      stdout: "pipe",
      stderr: "pipe",
    })

    expect(result.exitCode, result.stderr.toString()).toBe(0)
    // The layer canonicalizes through the native realpath, which on Windows also expands 8.3 short
    // names such as RUNNER~1 that the JavaScript realpathSync leaves in place.
    expect(result.stdout.toString()).toBe(fs.realpathSync.native(path.join(directories[4], "opencode")))
    const created = [
      path.join(directories[0], "opencode"),
      path.join(directories[1], "opencode", "bin"),
      path.join(directories[2], "opencode"),
      path.join(directories[3], "opencode"),
      path.join(directories[0], "opencode", "log"),
      path.join(directories[0], "opencode", "repos"),
      path.join(directories[4], "opencode"),
    ]
    created.forEach((directory) => expect(fs.statSync(directory).isDirectory()).toBe(true))
    fs.rmSync(root, { recursive: true, force: true })
  })

  test("default paths and layers adopt opencode roots while config and tmp keep their own sources", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-global-roots-"))
    const module = pathToFileURL(path.join(import.meta.dir, "../src/global.ts")).href
    const result = Bun.spawnSync({
      cmd: [
        process.execPath,
        "-e",
        `
          import { Context, Effect, Layer } from "effect"
          import { LayerNode } from "@opencode/util/effect/layer-node"
          const { Global } = await import(${JSON.stringify(module)})
          const defaults = await Effect.runPromise(Effect.scoped(Layer.build(Global.layerWith({}))))
          const configured = await Effect.runPromise(Effect.scoped(Layer.build(LayerNode.compile(Global.node))))
          console.log(JSON.stringify({
            path: Global.Path,
            made: Global.make(),
            defaults: Context.get(defaults, Global.Service),
            configured: Context.get(configured, Global.Service),
          }))
        `,
      ],
      cwd: path.join(import.meta.dir, ".."),
      env: {
        ...process.env,
        // Bun's own transpiler cache would otherwise create the XDG cache directory.
        BUN_RUNTIME_TRANSPILER_CACHE_PATH: "0",
        HOME: path.join(root, "home"),
        USERPROFILE: path.join(root, "home"),
        OPENCODE_TEST_HOME: undefined,
        OPENCODE_DATA_HOME: path.join(root, "selected-data"),
        OPENCODE_CACHE_HOME: path.join(root, "selected-cache"),
        OPENCODE_STATE_HOME: path.join(root, "selected-state"),
        XDG_DATA_HOME: path.join(root, "xdg-data"),
        XDG_CACHE_HOME: path.join(root, "xdg-cache"),
        XDG_STATE_HOME: path.join(root, "xdg-state"),
        XDG_CONFIG_HOME: path.join(root, "config"),
        OPENCODE_CONFIG_DIR: path.join(root, "config-override"),
        TMPDIR: path.join(root, "tmp"),
        TEMP: path.join(root, "tmp"),
      },
      stdout: "pipe",
      stderr: "pipe",
    })

    try {
      expect(result.exitCode, result.stderr.toString()).toBe(0)
      const expected = {
        home: path.join(root, "home"),
        data: path.join(root, "selected-data", "opencode"),
        cache: path.join(root, "selected-cache", "opencode"),
        state: path.join(root, "selected-state", "opencode"),
        config: path.join(root, "config", "opencode"),
        tmp: path.join(root, "tmp", "opencode"),
        bin: path.join(root, "selected-cache", "opencode", "bin"),
        log: path.join(root, "selected-data", "opencode", "log"),
        repos: path.join(root, "selected-data", "opencode", "repos"),
      }
      const canonical = fs.realpathSync.native(expected.tmp)
      expect(JSON.parse(result.stdout.toString())).toEqual({
        path: expected,
        made: expected,
        defaults: { ...expected, tmp: canonical },
        configured: { ...expected, tmp: canonical, config: path.join(root, "config-override") },
      })
      const created = [
        expected.data,
        expected.cache,
        expected.state,
        expected.config,
        expected.bin,
        expected.log,
        expected.repos,
      ]
      created.forEach((directory) => expect(fs.statSync(directory).isDirectory()).toBe(true))
      expect(fs.readdirSync(root).sort()).toEqual([
        "config",
        "config-override",
        "selected-cache",
        "selected-data",
        "selected-state",
        "tmp",
      ])
    } finally {
      fs.rmSync(root, { recursive: true, force: true })
    }
  })
})
