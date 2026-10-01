import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { LLMAISDK } from "../../src/session/llm/ai-sdk"

type AISDKEvent = Parameters<typeof LLMAISDK.toLLMEvents>[1]
type AdapterEvents = Effect.Effect.Success<ReturnType<typeof LLMAISDK.toLLMEvents>>

const usage = {
  inputTokens: 1,
  outputTokens: 1,
  totalTokens: 2,
  inputTokenDetails: { noCacheTokens: 1, cacheReadTokens: 0, cacheWriteTokens: 0 },
  outputTokenDetails: { textTokens: 1, reasoningTokens: 0 },
} satisfies Extract<AISDKEvent, { type: "finish-step" }>["usage"]

describe("session.llm.ai-sdk tool names", () => {
  test.each(["tool-result", "tool-error"] as const)(
    "retains the name after %s and finish-step until the stream finishes",
    async (ending) => {
      const state = LLMAISDK.adapterState()
      const run = (event: AISDKEvent): Promise<AdapterEvents> => Effect.runPromise(LLMAISDK.toLLMEvents(state, event))

      await run({ type: "tool-call", toolCallId: "call-1", toolName: "lookup", input: {} })
      expect(
        await run(
          ending === "tool-result"
            ? { type: ending, toolCallId: "call-1", toolName: "lookup", input: {}, output: "ok" }
            : { type: ending, toolCallId: "call-1", toolName: "lookup", input: {}, error: new Error("boom") },
        ),
      ).toMatchObject([{ type: ending, id: "call-1", name: "lookup" }])
      await run({
        type: "finish-step",
        response: { id: "response-1", timestamp: new Date(0), modelId: "test-model" },
        finishReason: "tool-calls",
        rawFinishReason: "tool_calls",
        usage,
        providerMetadata: undefined,
      })

      expect(await run({ type: "tool-input-delta", id: "call-1", delta: "{}" })).toMatchObject([
        { type: "tool-input-delta", id: "call-1", name: "lookup", text: "{}" },
      ])
      expect(await run({ type: "tool-input-end", id: "call-1" })).toMatchObject([
        { type: "tool-input-end", id: "call-1", name: "lookup" },
      ])

      await run({ type: "finish", finishReason: "stop", rawFinishReason: "stop", totalUsage: usage })
      expect(state.toolNames).toEqual({})
      expect(await run({ type: "tool-input-delta", id: "call-1", delta: "{}" })).toMatchObject([
        { type: "tool-input-delta", id: "call-1", name: "unknown", text: "{}" },
      ])
      expect(await run({ type: "tool-input-end", id: "call-1" })).toMatchObject([
        { type: "tool-input-end", id: "call-1", name: "unknown" },
      ])
    },
  )
})
