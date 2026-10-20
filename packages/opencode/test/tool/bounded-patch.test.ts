import { describe, expect, test } from "bun:test"
import { createTwoFilesPatch, parsePatch } from "diff"
import { boundedPatch, cutPatch, MAX_DIFF_CHARS, patchHeader, trimDiff } from "../../src/tool/edit"

const rows = (count: number, tag: string) => Array.from({ length: count }, (_, i) => `${tag} ${i} padding`).join("\n") + "\n"
const patchOf = (file: string, before: string, after: string) => trimDiff(createTwoFilesPatch(file, file, before, after))

// What apply_patch joins into metadata.diff: added, deleted, updated and deleted files in one string.
const whole = () =>
  [
    patchOf("a.txt", "", rows(300, "a")),
    patchHeader("gone-1.txt"),
    patchOf("b.txt", rows(50, "old"), rows(60, "new")),
    ...Array.from({ length: 20 }, (_, i) => patchHeader(`gone-${i + 2}.txt`)),
    patchOf("c.txt", "", rows(200, "c")),
  ].join("\n")

describe("cutPatch", () => {
  test("every cut point of a multi-file diff is a diff a parser accepts", () => {
    const diff = whole()
    for (let limit = 40; limit < diff.length; limit += 29) {
      const cut = cutPatch(diff, limit)
      expect(cut.length).toBeLessThanOrEqual(limit)
      expect(() => parsePatch(cut)).not.toThrow()
    }
  })

  test("a cut inside the first hunk keeps its start and rewrites its counts", () => {
    const diff = patchOf("f.txt", "a\nb\nc\n", "x\nb\nc\n")
    const limit = diff.indexOf("\n+x") + 1
    const cut = cutPatch(diff, limit)
    const hunk = parsePatch(cut)[0].hunks[0]
    expect(hunk.oldLines).toBe(1)
    expect(hunk.newLines).toBe(0)
    expect(cut).toContain("@@ -1,1 +0,0 @@")
  })

  test("a diff under the limit is returned unchanged", () => {
    const diff = patchOf("f.txt", "a\n", "b\n")
    expect(cutPatch(diff, diff.length)).toBe(diff)
  })

  test("later hunks are dropped when the cut lands in an earlier one", () => {
    const before = rows(400, "line")
    const after = before.replace("line 5 ", "LINE 5 ").replace("line 350 ", "LINE 350 ")
    const diff = patchOf("f.txt", before, after)
    expect(parsePatch(diff)[0].hunks).toHaveLength(2)
    const second = diff.indexOf("\n@@ ", diff.indexOf("\n@@ ") + 1)
    const cut = cutPatch(diff, second - 5)
    expect(parsePatch(cut)[0].hunks).toHaveLength(1)
  })
})

describe("boundedPatch", () => {
  test("a small change is the plain trimmed diff", () => {
    expect(boundedPatch("f.txt", "a\n", "b\n")).toBe(patchOf("f.txt", "a\n", "b\n"))
  })

  test("a file over four times the limit is a header only and no diff is built", () => {
    const huge = "x".repeat(MAX_DIFF_CHARS * 4 + 1)
    expect(boundedPatch("big.txt", "", huge)).toBe(patchHeader("big.txt"))
    expect(boundedPatch("big.txt", huge, "")).toBe(patchHeader("big.txt"))
  })

  test("non-ASCII text is cut on a line, never inside a character", () => {
    const text = Array.from({ length: 6000 }, (_, i) => `línea ${i} 日本語 😀 relleno relleno`).join("\n") + "\n"
    const cut = boundedPatch("u.txt", "", text)
    expect(cut.length).toBeLessThanOrEqual(MAX_DIFF_CHARS)
    expect(cut.endsWith("\n")).toBe(true)
    expect(/[\ud800-\udbff](?![\udc00-\udfff])/.test(cut)).toBe(false)
    expect(() => parsePatch(cut)).not.toThrow()
  })
})
