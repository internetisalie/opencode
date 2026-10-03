import { constants } from "node:fs"
import { access, mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises"
import { createServer } from "node:net"
import type { Socket } from "node:net"
import { join } from "node:path"
import { Info, ReadResult, Snapshot } from "@opencode/schema/persistent-pty"
import type { PersistentPty } from "@opencode/schema/persistent-pty"
import { Cause, Effect, Exit, Schema, Scope } from "effect"
import { HttpServer } from "effect/unstable/http"
import { ServerProcess } from "../../src/process"

export interface FixtureOptions {
  readonly root: string
  readonly nativeBinary: string
}
export interface CreateProgram {
  readonly command: string
  readonly args: readonly string[]
  readonly title: string
  readonly env: Readonly<Record<string, string>>
}
export interface PassivePtyFixture {
  readonly base: string
  readonly root: string
  readonly database: string
  readonly create: (options: {
    readonly caseID: string
    readonly sessionID: string
    readonly program: CreateProgram
  }) => Effect.Effect<PersistentPty.Info, Error>
  readonly list: (sessionID: string) => Effect.Effect<PersistentPty.Info[], Error>
  readonly snapshot: (ptyID: string) => Effect.Effect<PersistentPty.Snapshot, Error>
  readonly readCurrent: (sessionID: string) => Effect.Effect<PersistentPty.ReadResult | null, Error>
  readonly remove: (ptyID: string) => Effect.Effect<void, Error>
  readonly releaseGate: (options: {
    readonly caseID: string
    readonly stage: "mid" | "final"
  }) => Effect.Effect<void, Error>
  readonly close: () => Effect.Effect<void>
}
interface RequestOptions {
  readonly method: "GET" | "POST" | "DELETE"
  readonly pathname: string
  readonly body?: unknown
}
interface RequestOwner {
  readonly base: string
  readonly password: string
}

export function makePassivePtyFixture(options: FixtureOptions): Effect.Effect<PassivePtyFixture, unknown, Scope.Scope> {
  return Effect.gen(function* () {
    yield* filesystem(async () => {
      for (const key of [
        "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_CONFIG_HOME", "XDG_STATE_HOME", "XDG_RUNTIME_DIR",
        "TMPDIR", "TMP", "TEMP", "OPENCODE_CONFIG_DIR", "OPENCODE_PTY_RUNTIME_DIR", "OPENCODE_TEST_HOME",
      ]) {
        if (!process.env[key]) throw new Error(`blocked: launcher must set ${key} before importing the server`)
      }
      if (!(await stat(options.root)).isDirectory()) throw new Error("blocked: fixture parent is not a directory")
      if (!(await stat(options.nativeBinary)).isFile()) throw new Error("blocked: native executable is not a file")
      await access(options.nativeBinary, constants.X_OK)
    })
    const root = yield* Effect.acquireRelease(
      filesystem(() => mkdtemp(join(options.root, "owner-"))),
      (directory) => filesystem(() => rm(directory, { recursive: true, force: true })).pipe(Effect.orDie),
    )
    const parent = yield* Scope.Scope
    const child = yield* Scope.fork(parent)
    const password = crypto.randomUUID()
    const database = join(root, "opencode.db")
    const server = yield* ServerProcess.start<never, never>({
      hostname: "127.0.0.1", port: 0, password, app: { version: "passive-pty-test" },
      database: { path: database }, config: { directory: join(root, "config"), project: false },
      fs: { filewatcher: false },
    }).pipe(Effect.provideService(Scope.Scope, child))
    let closed = false
    const close = (): Effect.Effect<void> => Effect.suspend(() => {
      if (closed) return Effect.void
      closed = true
      return Scope.close(child, Exit.void)
    })
    const owner = { base: HttpServer.formatAddress(server.address), password }
    return methods({ ...owner, root, database, close })
  })
}

function methods(owner: RequestOwner & Pick<PassivePtyFixture, "root" | "database" | "close">): PassivePtyFixture {
  const cases = new Map<string, "pending" | "created">()
  const terminal = (sessionID: string): string => `/api/experimental/session/${encodeURIComponent(sessionID)}/terminal`
  const pty = (id: string): string => `/api/experimental/persistent-pty/${encodeURIComponent(id)}`
  return {
    base: owner.base, root: owner.root, database: owner.database, close: owner.close,
    create: (options) => create(owner, cases, options),
    list: (id) => request(owner, { method: "GET", pathname: terminal(id) }, Schema.Struct({ data: Schema.Array(Info) })).pipe(Effect.map((values) => Array.from(values))),
    snapshot: (id) => request(owner, { method: "GET", pathname: `${pty(id)}/snapshot` }, Schema.Struct({ data: Snapshot })),
    readCurrent: (id) => request(owner, { method: "GET", pathname: `${terminal(id)}/read` }, Schema.Struct({ data: Schema.NullOr(ReadResult) })),
    remove: (id) => emptyRequest(owner, { method: "DELETE", pathname: pty(id) }),
    releaseGate: (options) => filesystem(async () => {
      if (cases.get(options.caseID) !== "created") throw new Error(`uncreated-case:${options.caseID}`)
      if (options.stage !== "mid" && options.stage !== "final") throw new Error("invalid gate stage")
      await writeFile(join(owner.root, "gates", options.caseID, options.stage), "")
    }),
  }
}

function create(
  owner: RequestOwner & Pick<PassivePtyFixture, "root">,
  cases: Map<string, "pending" | "created">,
  options: Parameters<PassivePtyFixture["create"]>[0],
): Effect.Effect<PersistentPty.Info, Error> {
  return Effect.gen(function* () {
    yield* filesystem(async () => {
      if (!/^[A-Z][A-Za-z0-9]*$/.test(options.caseID)) throw new Error("invalid-case-id")
      if (cases.has(options.caseID)) throw new Error(`duplicate-case:${options.caseID}`)
      cases.set(options.caseID, "pending")
      await mkdir(join(owner.root, "gates", options.caseID), { recursive: true })
    })
    const info = yield* request(owner, {
      method: "POST", pathname: `/api/experimental/session/${encodeURIComponent(options.sessionID)}/terminal`,
      body: { ...options.program, cwd: owner.root },
    }, Schema.Struct({ data: Info }))
    cases.set(options.caseID, "created")
    return info
  })
}

function request<A>(owner: RequestOwner, options: RequestOptions, schema: Schema.Codec<{ readonly data: A }, unknown>): Effect.Effect<A, Error> {
  return Effect.tryPromise({
    try: async (signal) => {
      const response = await fetch(new URL(options.pathname, owner.base), {
        method: options.method, signal,
        headers: { authorization: `Basic ${btoa(`opencode:${owner.password}`)}`, "content-type": "application/json" },
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
      })
      if (!response.ok) throw new Error(`HTTP${response.status}:${options.pathname}:${await response.text()}`)
      const json: unknown = await response.json()
      return Schema.decodeUnknownSync(Schema.toCodecJson(schema))(json).data
    },
    catch: toError,
  }).pipe(Effect.timeoutOrElse({ duration: "5 seconds", orElse: () => Effect.fail(new Error("request-timeout")) }))
}

function emptyRequest(owner: RequestOwner, options: RequestOptions): Effect.Effect<void, Error> {
  return Effect.tryPromise({
    try: async (signal) => {
      const response = await fetch(new URL(options.pathname, owner.base), {
        method: options.method, signal, headers: { authorization: `Basic ${btoa(`opencode:${owner.password}`)}` },
      })
      if (response.status !== 204) throw new Error(`HTTP${response.status}:${await response.text()}`)
      if ((await response.arrayBuffer()).byteLength !== 0) throw new Error("DELETE returned a body")
    }, catch: toError,
  }).pipe(Effect.timeoutOrElse({ duration: "5 seconds", orElse: () => Effect.fail(new Error("request-timeout")) }))
}

function filesystem<A>(run: () => Promise<A>): Effect.Effect<A, Error> {
  return Effect.tryPromise({ try: run, catch: toError })
}
function toError(cause: unknown): Error {
  return cause instanceof Error ? cause : new Error(String(cause))
}

export interface ReadFault {
  readonly request: Effect.Effect<PersistentPty.Snapshot, Error>
  readonly ready: Effect.Effect<void, Error>
  readonly disconnected: Effect.Effect<void, Error>
  readonly sockets: () => number
}

export function makeReadFault(read: Effect.Effect<PersistentPty.Snapshot, Error>, mode: "loss" | "hold"): Effect.Effect<ReadFault, Error, Scope.Scope> {
  return Effect.gen(function* () {
    const ready = Promise.withResolvers<void>()
    const disconnected = Promise.withResolvers<void>()
    const sockets = new Set<Socket>()
    const server = yield* Effect.acquireRelease(Effect.sync(() => createServer((socket) => {
      sockets.add(socket)
      socket.once("error", (error) => ready.reject(error))
      socket.once("close", () => {
        sockets.delete(socket)
        disconnected.resolve()
      })
      socket.once("data", () => {
        void Effect.runPromise(read).then(() => {
          ready.resolve()
          if (mode === "loss") socket.destroy()
        }, (cause: unknown) => { ready.reject(cause); socket.destroy() })
      })
    })), (owned) => filesystem(() => new Promise<void>((resolve, reject) => {
      owned.listening ? owned.close((error) => error ? reject(error) : resolve()) : resolve()
      for (const socket of sockets) socket.destroy()
    })).pipe(Effect.orDie))
    yield* filesystem(() => new Promise<void>((resolve, reject) => {
      server.once("error", reject)
      server.listen(0, "127.0.0.1", resolve)
    }))
    const address = server.address()
    if (!address || typeof address === "string") return yield* Effect.fail(new Error("fault relay has no port"))
    return {
      request: request({ base: `http://127.0.0.1:${address.port}`, password: "relay-only" }, { method: "GET", pathname: "/held-snapshot" }, Schema.Struct({ data: Snapshot })),
      ready: filesystem(() => ready.promise).pipe(Effect.timeoutOrElse({ duration: "5 seconds", orElse: () => Effect.fail(new Error("relay-ready-timeout")) })),
      disconnected: filesystem(() => disconnected.promise).pipe(Effect.timeoutOrElse({ duration: "5 seconds", orElse: () => Effect.fail(new Error("relay-disconnect-timeout")) })),
      sockets: () => sockets.size,
    }
  })
}

type ConsumerResource = { kind: "request" } | { kind: "timer"; cancel: () => void } | { kind: "disposed" }
export interface TestConsumer {
  readonly dispose: () => Promise<void>
  readonly status: () => ConsumerResource["kind"]
  readonly callbacks: () => number
  readonly timerFirings: () => number
  readonly errors: readonly string[]
}
export function makeTestConsumer(request: Effect.Effect<PersistentPty.Snapshot, Error>): TestConsumer {
  const controller = new AbortController()
  const errors: string[] = []
  let resource: ConsumerResource = { kind: "request" }
  let callbacks = 0
  let timerFirings = 0
  let disposing: Promise<void> | undefined
  const inactive = (): boolean => resource.kind === "disposed"
  const observed = request.pipe(Effect.onExit((exit) => Effect.sync(() => {
    if (Exit.isFailure(exit)) errors.push(Cause.hasInterruptsOnly(exit.cause) ? "cancelled" : `request-error:${Cause.pretty(exit.cause)}`)
  })))
  const settled = (async (): Promise<void> => {
    while (!inactive()) {
      resource = { kind: "request" }
      await Effect.runPromise(observed, { signal: controller.signal }).then(() => {
        if (!inactive()) callbacks++
      }, (cause: unknown) => {
        if (!errors.length) errors.push(`request-error:${String(cause)}`)
      })
      if (inactive()) return
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          timerFirings++
          resolve()
        }, 100)
        resource = { kind: "timer", cancel: () => { clearTimeout(timer); resolve() } }
      })
    }
  })()
  return { status: () => resource.kind, callbacks: () => callbacks, timerFirings: () => timerFirings, errors, dispose: () => {
    if (disposing) return disposing
    if (resource.kind === "timer") resource.cancel()
    resource = { kind: "disposed" }
    controller.abort()
    return disposing = settled
  } }
}
