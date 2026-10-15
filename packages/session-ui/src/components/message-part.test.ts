import { describe, expect, test } from "bun:test"
import stripAnsi from "strip-ansi"
import { readPartText } from "./message-part-text"

describe("readPartText", () => {
  test("returns empty string when accum is undefined and part text is undefined", () => {
    expect(readPartText(undefined, { id: "part_1" })).toBe("")
  })

  test("returns trimmed part text when accum is undefined", () => {
    expect(readPartText(undefined, { id: "part_1", text: "  hello  " })).toBe("hello")
  })

  test("prefers accum value over part text when accum has a hit", () => {
    expect(readPartText({ part_1: "  from accum  " }, { id: "part_1", text: "from part" })).toBe("from accum")
  })

  test("falls back to part text when accum misses", () => {
    expect(readPartText({ other_part: "ignored" }, { id: "part_1", text: "  from part  " })).toBe("from part")
  })

  test("returns empty string for whitespace-only text", () => {
    expect(readPartText(undefined, { id: "part_1", text: "   \n\t  " })).toBe("")
  })

  test("trims leading and trailing whitespace", () => {
    expect(readPartText(undefined, { id: "part_1", text: "\n  body  \n" })).toBe("body")
  })
})

// Exercise the renderer's memo directly without loading its browser-only JSX dependencies.
const source = await Bun.file(new URL("./message-part.tsx", import.meta.url)).text()

describe("shell display text", () => {
  const body = source.split('name: "shell",')[1]?.match(/const text = createMemo\(\(\) => \{([\s\S]*?)\n    \}\)/)?.[1]
  if (!body) throw new Error("shell text memo not found")
  const text = new Function("props", "stripAnsi", body)

  test.each([
    ["hello\n\n\n<shell_metadata>\nexit=0\n</shell_metadata>", "hello\n"],
    ["(no output)\n\n<shell_metadata>\nexit=3\n</shell_metadata>", "(no output)"],
    ["partial\n\n<shell_metadata>\nUser aborted the command\n</shell_metadata>", "partial"],
    [
      "before\n\n<shell_metadata>\ncommand-owned\n</shell_metadata>\n\n<shell_metadata>\nexit=0\n</shell_metadata>",
      "before\n\n<shell_metadata>\ncommand-owned\n</shell_metadata>",
    ],
    [
      "hello\n\n<shell_metadata>\ncommand-owned\n</shell_metadata>\nafter",
      "hello\n\n<shell_metadata>\ncommand-owned\n</shell_metadata>\nafter",
    ],
    ["\u001b[31mhello\u001b[0m\r\n", "hello\n"],
  ])("presents %j as %j", (output, expected) => {
    const props = { input: { command: "test" }, output, metadata: { output: "fallback" } }
    expect(text(props, stripAnsi)).toBe("$ test\n\n" + expected)
    expect(props.output).toBe(output)
  })

  test("strips the block from the metadata output fallback", () => {
    expect(
      text(
        { input: { command: "test" }, metadata: { output: "hello\n\n<shell_metadata>\nexit=0\n</shell_metadata>" } },
        stripAnsi,
      ),
    ).toBe("$ test\n\nhello")
  })
})
