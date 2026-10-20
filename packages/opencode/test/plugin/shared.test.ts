import { describe, expect, test } from "bun:test"
import { checkPluginCompatibility, parsePluginSpecifier } from "../../src/plugin/shared"

describe("parsePluginSpecifier", () => {
  test("parses standard npm package without version", () => {
    expect(parsePluginSpecifier("acme")).toEqual({
      pkg: "acme",
      version: "latest",
    })
  })

  test("parses standard npm package with version", () => {
    expect(parsePluginSpecifier("acme@1.0.0")).toEqual({
      pkg: "acme",
      version: "1.0.0",
    })
  })

  test("parses scoped npm package without version", () => {
    expect(parsePluginSpecifier("@opencode/acme")).toEqual({
      pkg: "@opencode/acme",
      version: "latest",
    })
  })

  test("parses scoped npm package with version", () => {
    expect(parsePluginSpecifier("@opencode/acme@1.0.0")).toEqual({
      pkg: "@opencode/acme",
      version: "1.0.0",
    })
  })

  test("parses package with git+https url", () => {
    expect(parsePluginSpecifier("acme@git+https://github.com/opencode/acme.git")).toEqual({
      pkg: "acme",
      version: "git+https://github.com/opencode/acme.git",
    })
  })

  test("parses scoped package with git+https url", () => {
    expect(parsePluginSpecifier("@opencode/acme@git+https://github.com/opencode/acme.git")).toEqual({
      pkg: "@opencode/acme",
      version: "git+https://github.com/opencode/acme.git",
    })
  })

  test("parses package with git+ssh url containing another @", () => {
    expect(parsePluginSpecifier("acme@git+ssh://git@github.com/opencode/acme.git")).toEqual({
      pkg: "acme",
      version: "git+ssh://git@github.com/opencode/acme.git",
    })
  })

  test("parses scoped package with git+ssh url containing another @", () => {
    expect(parsePluginSpecifier("@opencode/acme@git+ssh://git@github.com/opencode/acme.git")).toEqual({
      pkg: "@opencode/acme",
      version: "git+ssh://git@github.com/opencode/acme.git",
    })
  })

  test("parses unaliased git+ssh url", () => {
    expect(parsePluginSpecifier("git+ssh://git@github.com/opencode/acme.git")).toEqual({
      pkg: "git+ssh://git@github.com/opencode/acme.git",
      version: "",
    })
  })

  test("parses npm alias using the alias name", () => {
    expect(parsePluginSpecifier("acme@npm:@opencode/acme@1.0.0")).toEqual({
      pkg: "acme",
      version: "npm:@opencode/acme@1.0.0",
    })
  })

  test("parses bare npm protocol specifier using the target package", () => {
    expect(parsePluginSpecifier("npm:@opencode/acme@1.0.0")).toEqual({
      pkg: "@opencode/acme",
      version: "1.0.0",
    })
  })

  test("parses unversioned npm protocol specifier", () => {
    expect(parsePluginSpecifier("npm:@opencode/acme")).toEqual({
      pkg: "@opencode/acme",
      version: "latest",
    })
  })
})

describe("checkPluginCompatibility", () => {
  const pkg = (range: string) => ({ json: { engines: { opencode: range } } }) as never

  test("accepts a prerelease build of a version the range names", async () => {
    await expect(checkPluginCompatibility("x", "1.18.32-internetisalie.2", pkg(">=1.18.31"))).resolves.toBeUndefined()
    await expect(checkPluginCompatibility("x", "1.18.32-internetisalie.2", pkg(">=1.18.32"))).resolves.toBeUndefined()
  })

  test("still rejects a build older than the range", async () => {
    await expect(checkPluginCompatibility("x", "1.18.30-internetisalie.1", pkg(">=1.18.31"))).rejects.toThrow(
      "Plugin requires opencode >=1.18.31 but running 1.18.30-internetisalie.1",
    )
  })

  test("checks the build's base version, so a prerelease inside the range is not ordered against its tag", async () => {
    await expect(
      checkPluginCompatibility("x", "1.18.32-internetisalie.2", pkg(">=1.18.32-internetisalie.3")),
    ).resolves.toBeUndefined()
  })

  test("accepts a plain release", async () => {
    await expect(checkPluginCompatibility("x", "1.18.32", pkg(">=1.18.31"))).resolves.toBeUndefined()
  })
})
