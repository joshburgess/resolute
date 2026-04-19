// Thin facade over the napi-rs generated ../index.js.
// Responsibilities:
//   1. Re-export the native classes and factory functions.
//   2. Wrap async methods that can throw pg-wired errors so they carry
//      structured fields (code, severity, detail, hint, position, kind).
//   3. Monkey-patch conn.query / pool.query / pipeline.push onto a binary
//      wire-protocol path using a per-OID codec registry.

import type {
  ColumnarResult,
  ConnectOptions,
  FieldInfo,
  CancelToken as NativeCancelToken,
  Connection as NativeConnection,
  Notifications as NativeNotifications,
  Pipeline as NativePipeline,
  Pool as NativePool,
  RowStream as NativeRowStream,
  Notification,
  QueryResult,
  RawQueryResult,
} from "../index.js"

// eslint-disable-next-line @typescript-eslint/no-var-requires
const native = require("../index.js") as {
  Connection: { new (): NativeConnection; prototype: NativeConnection }
  Pool: { new (): NativePool; prototype: NativePool }
  Pipeline: { new (): NativePipeline; prototype: NativePipeline }
  RowStream: { new (): NativeRowStream; prototype: NativeRowStream }
  Notifications: {
    new (): NativeNotifications
    prototype: NativeNotifications
  }
  CancelToken: { new (): NativeCancelToken; prototype: NativeCancelToken }
  connect(options: ConnectOptions): Promise<NativeConnection>
  createPool(options: ConnectOptions, size: number): Promise<NativePool>
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export type PgErrorKind = "pg" | "io" | "protocol" | "closed"

export interface PgErrorPayload {
  kind: PgErrorKind
  code?: string
  message: string
  severity?: string
  detail?: string
  hint?: string
  position?: string
}

const ERR_PREFIX = "pg-wired-error:"

export class PgError extends Error {
  override readonly name = "PgError"
  readonly kind: PgErrorKind
  readonly code?: string
  readonly severity?: string
  readonly detail?: string
  readonly hint?: string
  readonly position?: string

  constructor(payload: PgErrorPayload) {
    super(payload.message || "pg-wired error")
    this.kind = payload.kind
    if (payload.kind === "pg") {
      if (payload.code) this.code = payload.code
      if (payload.severity) this.severity = payload.severity
      if (payload.detail) this.detail = payload.detail
      if (payload.hint) this.hint = payload.hint
      if (payload.position) this.position = payload.position
    }
  }
}

export class AbortError extends Error {
  override readonly name = "AbortError"
  readonly code = "ABORT_ERR"
  constructor(message = "The operation was aborted") {
    super(message)
  }
}

function reshape(err: unknown): unknown {
  if (!(err instanceof Error)) return err
  const msg = err.message || ""
  if (!msg.startsWith(ERR_PREFIX)) return err
  try {
    const payload = JSON.parse(msg.slice(ERR_PREFIX.length)) as PgErrorPayload
    const wrapped = new PgError(payload)
    if (err.stack) wrapped.stack = err.stack
    return wrapped
  } catch {
    return err
  }
}

type AsyncMethod = (this: unknown, ...args: unknown[]) => Promise<unknown>

function wrapAsync<F extends AsyncMethod>(fn: F): F {
  return async function (this: unknown, ...args: unknown[]) {
    try {
      return await fn.apply(this, args)
    } catch (e) {
      throw reshape(e)
    }
  } as F
}

// ---------------------------------------------------------------------------
// AbortSignal integration
// ---------------------------------------------------------------------------

export interface QueryOpts {
  signal?: AbortSignal
}

interface CancelSource {
  cancelToken(): NativeCancelToken
}

export async function withSignal<T>(
  conn: CancelSource,
  signal: AbortSignal | undefined,
  op: () => Promise<T>,
): Promise<T> {
  if (!signal) return op()
  if (signal.aborted) {
    throw signal.reason instanceof Error ? signal.reason : new AbortError(signal.reason)
  }
  const token = conn.cancelToken()
  let aborted = false
  const onAbort = () => {
    aborted = true
    token.cancel().catch(() => {})
  }
  signal.addEventListener("abort", onAbort, { once: true })
  try {
    return await op()
  } catch (e) {
    if (aborted) {
      throw signal.reason instanceof Error ? signal.reason : new AbortError(signal.reason)
    }
    throw e
  } finally {
    signal.removeEventListener("abort", onAbort)
  }
}

function extractSignalOpts(
  args: unknown[],
): [positional: unknown[], signal: AbortSignal | undefined] {
  if (args.length === 0) return [args, undefined]
  const last = args[args.length - 1]
  if (
    last !== null &&
    typeof last === "object" &&
    !Array.isArray(last) &&
    !Buffer.isBuffer(last) &&
    "signal" in (last as object)
  ) {
    return [args.slice(0, -1), (last as QueryOpts).signal]
  }
  return [args, undefined]
}

function wrapConnectionAsync<F extends AsyncMethod>(fn: F): F {
  return async function (this: CancelSource, ...args: unknown[]) {
    const [positional, signal] = extractSignalOpts(args)
    try {
      if (signal) {
        return await withSignal(this, signal, () => fn.apply(this, positional))
      }
      return await fn.apply(this, positional)
    } catch (e) {
      throw reshape(e)
    }
  } as F
}

// ---------------------------------------------------------------------------
// URL / options parsing
// ---------------------------------------------------------------------------

export function parsePgUrl(u: string): ConnectOptions {
  const url = new URL(u)
  if (url.protocol !== "postgres:" && url.protocol !== "postgresql:") {
    throw new Error(
      `pg-wired: unsupported URL scheme ${url.protocol} (expected postgres: or postgresql:)`,
    )
  }
  const out: ConnectOptions = {
    host: url.hostname || "127.0.0.1",
    port: url.port ? Number(url.port) : 5432,
    user: url.username ? decodeURIComponent(url.username) : "postgres",
    password: url.password ? decodeURIComponent(url.password) : "",
    database: url.pathname
      ? decodeURIComponent(url.pathname.replace(/^\//, "")) || "postgres"
      : "postgres",
  }
  const sslmode = url.searchParams.get("sslmode")
  if (sslmode) out.sslmode = sslmode
  return out
}

function normalizeOptions(optsOrUrl: ConnectOptions | string): ConnectOptions {
  if (typeof optsOrUrl === "string") return parsePgUrl(optsOrUrl)
  return optsOrUrl
}

// ---------------------------------------------------------------------------
// ColumnarResult accessor
// ---------------------------------------------------------------------------

export function columnarCell(result: ColumnarResult, row: number, col: number): string | null {
  const idx = row * result.cols + col
  const nullByte = result.nulls[idx >> 3]
  if (nullByte !== undefined && (nullByte & (1 << (idx & 7))) !== 0) return null
  const start = result.offsets[idx]
  const end = result.offsets[idx + 1]
  if (start === undefined || end === undefined) return null
  return result.data.slice(start, end)
}

// ---------------------------------------------------------------------------
// Binary wire codecs
// ---------------------------------------------------------------------------

export const TYPE_OID = Object.freeze({
  BOOL: 16,
  BYTEA: 17,
  CHAR: 18,
  NAME: 19,
  INT8: 20,
  INT2: 21,
  INT4: 23,
  TEXT: 25,
  OID: 26,
  JSON: 114,
  FLOAT4: 700,
  FLOAT8: 701,
  BPCHAR: 1042,
  VARCHAR: 1043,
  DATE: 1082,
  TIME: 1083,
  TIMESTAMP: 1114,
  TIMESTAMPTZ: 1184,
  NUMERIC: 1700,
  UUID: 2950,
  JSONB: 3802,
} as const)

export type TypeOid = (typeof TYPE_OID)[keyof typeof TYPE_OID]

// Epoch offset between PostgreSQL (2000-01-01 UTC) and JS (1970-01-01 UTC).
const PG_EPOCH_MS = 946684800000
const MICROS_PER_MS = 1000n
const MS_PER_DAY = 86400000
const PG_INT64_MIN = -0x8000000000000000n
const PG_INT64_MAX = 0x7fffffffffffffffn

function encodeBool(v: unknown): Buffer {
  const b = Buffer.alloc(1)
  b[0] = v ? 1 : 0
  return b
}
function decodeBoolBin(buf: Buffer): boolean {
  return buf[0] !== 0
}

function encodeInt2(v: unknown): Buffer {
  const b = Buffer.alloc(2)
  b.writeInt16BE(Number(v), 0)
  return b
}
function decodeInt2Bin(buf: Buffer): number {
  return buf.readInt16BE(0)
}

function encodeInt4(v: unknown): Buffer {
  const b = Buffer.alloc(4)
  b.writeInt32BE(Number(v), 0)
  return b
}
function decodeInt4Bin(buf: Buffer): number {
  return buf.readInt32BE(0)
}

function encodeInt8(v: unknown): Buffer {
  const b = Buffer.alloc(8)
  b.writeBigInt64BE(typeof v === "bigint" ? v : BigInt(v as number), 0)
  return b
}
function decodeInt8Bin(buf: Buffer): bigint {
  return buf.readBigInt64BE(0)
}

function encodeFloat4(v: unknown): Buffer {
  const b = Buffer.alloc(4)
  b.writeFloatBE(Number(v), 0)
  return b
}
function decodeFloat4Bin(buf: Buffer): number {
  return buf.readFloatBE(0)
}

function encodeFloat8(v: unknown): Buffer {
  const b = Buffer.alloc(8)
  b.writeDoubleBE(Number(v), 0)
  return b
}
function decodeFloat8Bin(buf: Buffer): number {
  return buf.readDoubleBE(0)
}

function encodeText(v: unknown): Buffer {
  return typeof v === "string" ? Buffer.from(v, "utf8") : Buffer.from(String(v), "utf8")
}
function decodeText(buf: Buffer): string {
  return buf.toString("utf8")
}

function encodeBytea(v: unknown): Buffer {
  if (Buffer.isBuffer(v)) return v
  if (v instanceof Uint8Array) return Buffer.from(v.buffer, v.byteOffset, v.byteLength)
  throw new TypeError("bytea param must be Buffer or Uint8Array")
}
function decodeByteaBin(buf: Buffer): Buffer {
  return Buffer.from(buf)
}
function decodeByteaText(buf: Buffer): Buffer {
  const s = buf.toString("utf8")
  if (s.startsWith("\\x")) return Buffer.from(s.slice(2), "hex")
  // Legacy octal-escape form. Rarely seen since bytea_output=hex is the
  // default since 9.0, but handle it for completeness.
  const out: number[] = []
  for (let i = 0; i < s.length; ) {
    if (s[i] === "\\" && s[i + 1] === "\\") {
      out.push(0x5c)
      i += 2
    } else if (s[i] === "\\" && /[0-7]/.test(s[i + 1] ?? "")) {
      out.push(Number.parseInt(s.slice(i + 1, i + 4), 8))
      i += 4
    } else {
      out.push(s.charCodeAt(i))
      i += 1
    }
  }
  return Buffer.from(out)
}

function encodeUuid(v: unknown): Buffer {
  if (Buffer.isBuffer(v) && v.length === 16) return v
  if (typeof v !== "string") {
    throw new TypeError("uuid param must be string or 16-byte Buffer")
  }
  const hex = v.replace(/-/g, "")
  if (hex.length !== 32) throw new Error(`invalid uuid: ${v}`)
  return Buffer.from(hex, "hex")
}
function decodeUuidBin(buf: Buffer): string {
  const h = buf.toString("hex")
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`
}

function encodeTimestamptz(v: unknown): Buffer {
  if (!(v instanceof Date)) throw new TypeError("timestamptz param must be a Date")
  const ms = BigInt(v.getTime())
  const micros = (ms - BigInt(PG_EPOCH_MS)) * MICROS_PER_MS
  const b = Buffer.alloc(8)
  b.writeBigInt64BE(micros, 0)
  return b
}
function decodeTimestamptzBin(buf: Buffer): Date {
  const micros = buf.readBigInt64BE(0)
  if (micros === PG_INT64_MAX) return new Date(8640000000000000)
  if (micros === PG_INT64_MIN) return new Date(-8640000000000000)
  const ms = Number(micros / MICROS_PER_MS) + PG_EPOCH_MS
  return new Date(ms)
}

function encodeDate(v: unknown): Buffer {
  if (!(v instanceof Date)) throw new TypeError("date param must be a Date")
  const utcMs = Date.UTC(v.getUTCFullYear(), v.getUTCMonth(), v.getUTCDate())
  const days = Math.round((utcMs - PG_EPOCH_MS) / MS_PER_DAY)
  const b = Buffer.alloc(4)
  b.writeInt32BE(days, 0)
  return b
}
function decodeDateBin(buf: Buffer): Date {
  const days = buf.readInt32BE(0)
  if (days === 0x7fffffff) return new Date(8640000000000000)
  if (days === -0x80000000) return new Date(-8640000000000000)
  return new Date(PG_EPOCH_MS + days * MS_PER_DAY)
}

function encodeJson(v: unknown): Buffer {
  return Buffer.from(typeof v === "string" ? v : JSON.stringify(v), "utf8")
}
function decodeJsonText(buf: Buffer): unknown {
  return JSON.parse(buf.toString("utf8"))
}
function encodeJsonb(v: unknown): Buffer {
  const body = typeof v === "string" ? v : JSON.stringify(v)
  const b = Buffer.alloc(1 + Buffer.byteLength(body, "utf8"))
  b[0] = 1 // version
  b.write(body, 1, "utf8")
  return b
}
function decodeJsonbBin(buf: Buffer): unknown {
  if (buf[0] !== 1) throw new Error(`unknown jsonb version: ${buf[0]}`)
  return JSON.parse(buf.subarray(1).toString("utf8"))
}

interface Codec {
  binary: boolean
  encode(v: unknown): Buffer
  decodeBin(buf: Buffer): unknown
  decodeText(buf: Buffer): unknown
}

function textCoerce<T>(coerce: (s: string) => T): (buf: Buffer) => T {
  return (buf) => coerce(buf.toString("utf8"))
}

const CODECS = new Map<number, Codec>([
  [
    TYPE_OID.BOOL,
    {
      binary: true,
      encode: encodeBool,
      decodeBin: decodeBoolBin,
      decodeText: (buf) => buf.toString("utf8") === "t",
    },
  ],
  [
    TYPE_OID.BYTEA,
    {
      binary: true,
      encode: encodeBytea,
      decodeBin: decodeByteaBin,
      decodeText: decodeByteaText,
    },
  ],
  [
    TYPE_OID.INT2,
    {
      binary: true,
      encode: encodeInt2,
      decodeBin: decodeInt2Bin,
      decodeText: textCoerce(Number),
    },
  ],
  [
    TYPE_OID.INT4,
    {
      binary: true,
      encode: encodeInt4,
      decodeBin: decodeInt4Bin,
      decodeText: textCoerce(Number),
    },
  ],
  [
    TYPE_OID.OID,
    {
      binary: true,
      encode: (v) => {
        const b = Buffer.alloc(4)
        b.writeUInt32BE(Number(v), 0)
        return b
      },
      decodeBin: (buf) => buf.readUInt32BE(0),
      decodeText: textCoerce(Number),
    },
  ],
  [
    TYPE_OID.INT8,
    {
      binary: true,
      encode: encodeInt8,
      decodeBin: decodeInt8Bin,
      decodeText: textCoerce(BigInt),
    },
  ],
  [
    TYPE_OID.FLOAT4,
    {
      binary: true,
      encode: encodeFloat4,
      decodeBin: decodeFloat4Bin,
      decodeText: textCoerce(Number),
    },
  ],
  [
    TYPE_OID.FLOAT8,
    {
      binary: true,
      encode: encodeFloat8,
      decodeBin: decodeFloat8Bin,
      decodeText: textCoerce(Number),
    },
  ],
  [TYPE_OID.TEXT, { binary: true, encode: encodeText, decodeBin: decodeText, decodeText }],
  [TYPE_OID.VARCHAR, { binary: true, encode: encodeText, decodeBin: decodeText, decodeText }],
  [TYPE_OID.BPCHAR, { binary: true, encode: encodeText, decodeBin: decodeText, decodeText }],
  [TYPE_OID.NAME, { binary: true, encode: encodeText, decodeBin: decodeText, decodeText }],
  [TYPE_OID.CHAR, { binary: true, encode: encodeText, decodeBin: decodeText, decodeText }],
  [
    TYPE_OID.UUID,
    {
      binary: true,
      encode: encodeUuid,
      decodeBin: decodeUuidBin,
      decodeText: (buf) => buf.toString("utf8"),
    },
  ],
  [
    TYPE_OID.TIMESTAMP,
    {
      binary: true,
      encode: encodeTimestamptz,
      decodeBin: decodeTimestamptzBin,
      decodeText: (buf) => new Date(`${buf.toString("utf8")}Z`),
    },
  ],
  [
    TYPE_OID.TIMESTAMPTZ,
    {
      binary: true,
      encode: encodeTimestamptz,
      decodeBin: decodeTimestamptzBin,
      decodeText: (buf) => new Date(buf.toString("utf8")),
    },
  ],
  [
    TYPE_OID.DATE,
    {
      binary: true,
      encode: encodeDate,
      decodeBin: decodeDateBin,
      decodeText: (buf) => new Date(`${buf.toString("utf8")}T00:00:00Z`),
    },
  ],
  [
    TYPE_OID.NUMERIC,
    {
      binary: false,
      encode: (v) => Buffer.from(String(v), "utf8"),
      decodeBin: (buf) => buf.toString("utf8"),
      decodeText: (buf) => buf.toString("utf8"),
    },
  ],
  [
    TYPE_OID.JSON,
    {
      binary: false,
      encode: encodeJson,
      decodeBin: decodeJsonText,
      decodeText: decodeJsonText,
    },
  ],
  [
    TYPE_OID.JSONB,
    {
      binary: true,
      encode: encodeJsonb,
      decodeBin: decodeJsonbBin,
      decodeText: decodeJsonText,
    },
  ],
  [
    TYPE_OID.TIME,
    {
      binary: false,
      encode: (v) => Buffer.from(String(v), "utf8"),
      decodeBin: (buf) => buf.toString("utf8"),
      decodeText: (buf) => buf.toString("utf8"),
    },
  ],
])

function inferOid(v: unknown): number {
  if (v === null || v === undefined) return 0
  if (typeof v === "boolean") return TYPE_OID.BOOL
  if (typeof v === "bigint") return TYPE_OID.INT8
  if (typeof v === "number") return TYPE_OID.FLOAT8
  if (typeof v === "string") return TYPE_OID.TEXT
  if (Buffer.isBuffer(v) || v instanceof Uint8Array) return TYPE_OID.BYTEA
  if (v instanceof Date) return TYPE_OID.TIMESTAMPTZ
  if (typeof v === "object") return TYPE_OID.JSONB
  return 0
}

interface EncodedParams {
  buffers: Array<Buffer | null>
  formats: number[]
  oids: number[]
}

function encodeParams(values: readonly unknown[], oids: readonly number[]): EncodedParams {
  const n = values.length
  const buffers: Array<Buffer | null> = new Array(n)
  const formats: number[] = new Array(n)
  const resolvedOids: number[] = new Array(n)
  for (let i = 0; i < n; i++) {
    const v = values[i]
    const declared = oids.length === n ? oids[i]! | 0 : 0
    const oid = declared || inferOid(v)
    resolvedOids[i] = oid
    if (v === null || v === undefined) {
      buffers[i] = null
      formats[i] = oid === 0 ? 0 : CODECS.get(oid)?.binary ? 1 : 0
      continue
    }
    if (Buffer.isBuffer(v)) {
      buffers[i] = v
      formats[i] = 1
      continue
    }
    const codec = oid ? CODECS.get(oid) : undefined
    if (codec) {
      buffers[i] = codec.encode(v)
      formats[i] = codec.binary ? 1 : 0
    } else {
      buffers[i] = Buffer.from(String(v), "utf8")
      formats[i] = 0
    }
  }
  return { buffers, formats, oids: resolvedOids }
}

function decodeCell(buf: Buffer | null | undefined, field: FieldInfo): unknown {
  if (buf === null || buf === undefined) return null
  const codec = CODECS.get(field.typeOid)
  if (!codec) {
    return field.format === 1 ? Buffer.from(buf) : buf.toString("utf8")
  }
  return field.format === 1 ? codec.decodeBin(buf) : codec.decodeText(buf)
}

function decodeRows(
  rows: ReadonlyArray<ReadonlyArray<Buffer | null | undefined>>,
  fields: readonly FieldInfo[],
): unknown[][] {
  const out: unknown[][] = new Array(rows.length)
  for (let r = 0; r < rows.length; r++) {
    const row = rows[r]!
    const decoded: unknown[] = new Array(row.length)
    for (let c = 0; c < row.length; c++) {
      decoded[c] = decodeCell(row[c], fields[c]!)
    }
    out[r] = decoded
  }
  return out
}

// ---------------------------------------------------------------------------
// Public query result type and Connection / Pool / Pipeline interfaces
// ---------------------------------------------------------------------------

export interface TypedQueryResult {
  fields: FieldInfo[]
  rows: unknown[][]
  commandTag: string
}

type NativeParams = Array<Buffer | undefined | null>

export interface Connection {
  query(
    sql: string,
    params?: readonly unknown[],
    oids?: readonly number[],
    opts?: QueryOpts,
  ): Promise<TypedQueryResult>
  queryRaw(
    sql: string,
    params: NativeParams,
    paramOids: number[],
    paramFormats: number[],
    resultFormats: number[],
    opts?: QueryOpts,
  ): Promise<RawQueryResult>
  simpleQuery(sql: string, opts?: QueryOpts): Promise<QueryResult>
  queryColumnar(
    sql: string,
    params: NativeParams,
    paramOids: number[],
    opts?: QueryOpts,
  ): Promise<ColumnarResult>
  simpleQueryColumnar(sql: string, opts?: QueryOpts): Promise<ColumnarResult>
  queryStream(
    sql: string,
    params: NativeParams,
    paramOids: number[],
    opts?: QueryOpts,
  ): Promise<RowStream>
  copyIn(sql: string, data: Buffer, opts?: QueryOpts): Promise<bigint>
  copyOut(sql: string, opts?: QueryOpts): Promise<Buffer>
  pipeline(): Pipeline
  cancelToken(): NativeCancelToken
  isAlive(): boolean
  backendPid(): number
  notifications(): Notifications | null
  close(): Promise<void>
  transaction<T>(fn: (tx: TxHandle) => Promise<T>, opts?: TxOpts): Promise<T>
}

export interface Pool {
  query(
    sql: string,
    params?: readonly unknown[],
    oids?: readonly number[],
    opts?: QueryOpts,
  ): Promise<TypedQueryResult>
  queryRaw(
    sql: string,
    params: NativeParams,
    paramOids: number[],
    paramFormats: number[],
    resultFormats: number[],
    opts?: QueryOpts,
  ): Promise<RawQueryResult>
  simpleQuery(sql: string, opts?: QueryOpts): Promise<QueryResult>
  size(): number
  aliveCount(): Promise<number>
  close(): Promise<void>
}

export interface Pipeline {
  push(sql: string, params?: readonly unknown[], oids?: readonly number[]): Pipeline
  len(): number
  execute(): Promise<TypedQueryResult[]>
  clear(): void
}

export interface RowStream {
  readonly fields: FieldInfo[]
  next(): Promise<Array<string | undefined | null> | null>
  nextBatch(max: number): Promise<Array<Array<string | undefined | null>>>
  close(): Promise<void>
  [Symbol.asyncIterator](): AsyncIterator<Array<string | undefined | null>>
}

export interface Notifications {
  next(): Promise<Notification | null>
  close(): Promise<void>
  [Symbol.asyncIterator](): AsyncIterator<Notification>
}

export type CancelToken = NativeCancelToken

// ---------------------------------------------------------------------------
// Prototype wrapping
// ---------------------------------------------------------------------------

function wrapMethods(
  proto: object,
  names: readonly string[],
  wrapper: <F extends AsyncMethod>(fn: F) => F = wrapAsync,
): void {
  for (const name of names) {
    const original = (proto as Record<string, unknown>)[name]
    if (typeof original !== "function") continue
    Object.defineProperty(proto, name, {
      value: wrapper(original as AsyncMethod),
      writable: true,
      configurable: true,
    })
  }
}

const CONN_METHODS_WITH_SIGNAL = [
  "query",
  "queryRaw",
  "simpleQuery",
  "queryColumnar",
  "simpleQueryColumnar",
  "queryStream",
  "copyIn",
  "copyOut",
] as const

wrapMethods(native.Connection.prototype as object, CONN_METHODS_WITH_SIGNAL, wrapConnectionAsync)
wrapMethods(native.Connection.prototype as object, ["close"])
wrapMethods(native.Pool.prototype as object, [
  "query",
  "queryRaw",
  "simpleQuery",
  "aliveCount",
  "close",
])
wrapMethods(native.Pipeline.prototype as object, ["execute"])
wrapMethods(native.RowStream.prototype as object, ["next", "nextBatch"])
wrapMethods(native.Notifications.prototype as object, ["next"])
wrapMethods(native.CancelToken.prototype as object, ["cancel"])

// ---------------------------------------------------------------------------
// connect / createPool
// ---------------------------------------------------------------------------

export async function connect(optsOrUrl: ConnectOptions | string): Promise<Connection> {
  try {
    const conn = await native.connect(normalizeOptions(optsOrUrl))
    return conn as unknown as Connection
  } catch (e) {
    throw reshape(e)
  }
}

export async function createPool(optsOrUrl: ConnectOptions | string, size: number): Promise<Pool> {
  try {
    const pool = await native.createPool(normalizeOptions(optsOrUrl), size)
    return pool as unknown as Pool
  } catch (e) {
    throw reshape(e)
  }
}

// ---------------------------------------------------------------------------
// Transaction helper
// ---------------------------------------------------------------------------

export type IsolationLevel = "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE"

export interface TxOpts {
  signal?: AbortSignal
  isolationLevel?: IsolationLevel | string
  readOnly?: boolean
  deferrable?: boolean
}

export interface TxHandle {
  query(
    sql: string,
    params?: readonly unknown[],
    oids?: readonly number[],
    opts?: QueryOpts,
  ): Promise<TypedQueryResult>
  queryRaw(
    sql: string,
    params: NativeParams,
    paramOids: number[],
    paramFormats: number[],
    resultFormats: number[],
    opts?: QueryOpts,
  ): Promise<RawQueryResult>
  simpleQuery(sql: string, opts?: QueryOpts): Promise<QueryResult>
  queryColumnar(
    sql: string,
    params: NativeParams,
    paramOids: number[],
    opts?: QueryOpts,
  ): Promise<ColumnarResult>
  simpleQueryColumnar(sql: string, opts?: QueryOpts): Promise<ColumnarResult>
  queryStream(
    sql: string,
    params: NativeParams,
    paramOids: number[],
    opts?: QueryOpts,
  ): Promise<RowStream>
  copyIn(sql: string, data: Buffer, opts?: QueryOpts): Promise<bigint>
  copyOut(sql: string, opts?: QueryOpts): Promise<Buffer>
  pipeline(): Pipeline
  cancelToken(): NativeCancelToken
  savepoint<T>(fn: (sp: TxHandle) => Promise<T>): Promise<T>
  savepoint<T>(name: string, fn: (sp: TxHandle) => Promise<T>): Promise<T>
}

interface TxState {
  spCounter: number
}

function makeTxHandle(conn: Connection, signal: AbortSignal | undefined, state: TxState): TxHandle {
  type AnyAsync = (...args: unknown[]) => Promise<unknown>
  const handle = {} as Record<string, unknown>
  for (const name of CONN_METHODS_WITH_SIGNAL) {
    const method = (conn as unknown as Record<string, unknown>)[name]
    if (typeof method !== "function") continue
    handle[name] = function (this: unknown, ...args: unknown[]) {
      const [positional, userSignal] = extractSignalOpts(args)
      const merged = userSignal ?? signal
      const tail = merged ? [{ signal: merged }] : []
      return (method as AnyAsync).apply(conn, [...positional, ...tail])
    }
  }
  handle["pipeline"] = () => conn.pipeline()
  handle["cancelToken"] = () => conn.cancelToken()
  handle["savepoint"] = async (
    nameOrFn: string | ((sp: TxHandle) => Promise<unknown>),
    maybeFn?: (sp: TxHandle) => Promise<unknown>,
  ): Promise<unknown> => {
    const fn = typeof nameOrFn === "function" ? nameOrFn : maybeFn
    if (!fn) throw new Error("savepoint: callback is required")
    const rawName = typeof nameOrFn === "string" ? nameOrFn : `sp_${++state.spCounter}`
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(rawName)) {
      throw new Error(`invalid savepoint name: ${rawName}`)
    }
    const sigOpts = signal ? { signal } : undefined
    await conn.simpleQuery(`SAVEPOINT ${rawName}`, sigOpts)
    try {
      const result = await fn(makeTxHandle(conn, signal, state))
      await conn.simpleQuery(`RELEASE SAVEPOINT ${rawName}`, sigOpts)
      return result
    } catch (err) {
      try {
        await conn.simpleQuery(`ROLLBACK TO SAVEPOINT ${rawName}`)
      } catch {
        // best-effort
      }
      throw err
    }
  }
  return handle as unknown as TxHandle
}

export async function transaction<T>(
  conn: Connection,
  fn: (tx: TxHandle) => Promise<T>,
  opts: TxOpts = {},
): Promise<T> {
  const { signal, isolationLevel, readOnly, deferrable } = opts
  const parts = ["BEGIN"]
  if (isolationLevel) {
    const allowed = new Set(["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
    const level = isolationLevel.toUpperCase()
    if (!allowed.has(level)) {
      throw new Error(`invalid isolationLevel: ${isolationLevel}`)
    }
    parts.push(`ISOLATION LEVEL ${level}`)
  }
  if (readOnly) parts.push("READ ONLY")
  if (deferrable) parts.push("DEFERRABLE")
  const beginSql = parts.join(" ")

  const sigOpts = signal ? { signal } : undefined
  await conn.simpleQuery(beginSql, sigOpts)
  const state: TxState = { spCounter: 0 }
  try {
    const result = await fn(makeTxHandle(conn, signal, state))
    await conn.simpleQuery("COMMIT", sigOpts)
    return result
  } catch (err) {
    try {
      await conn.simpleQuery("ROLLBACK")
    } catch {
      // best-effort
    }
    throw err
  }
}
// Install transaction as a method on Connection.
;(native.Connection.prototype as unknown as Record<string, unknown>)["transaction"] = function (
  this: Connection,
  fn: (tx: TxHandle) => Promise<unknown>,
  opts?: TxOpts,
) {
  return transaction(this, fn, opts)
}

// ---------------------------------------------------------------------------
// Async iterable sugar
// ---------------------------------------------------------------------------

const ROW_STREAM_BATCH = 256
;(
  native.RowStream.prototype as unknown as {
    [Symbol.asyncIterator](): AsyncIterator<Array<string | undefined | null>>
  }
)[Symbol.asyncIterator] = function (this: NativeRowStream) {
  let batch: Array<Array<string | undefined | null>> | null = null
  let idx = 0
  let done = false
  return {
    next: async () => {
      if (done) return { done: true, value: undefined }
      if (batch !== null && idx < batch.length) {
        return { done: false, value: batch[idx++]! }
      }
      batch = await this.nextBatch(ROW_STREAM_BATCH)
      idx = 0
      if (!batch || batch.length === 0) {
        done = true
        return { done: true, value: undefined }
      }
      return { done: false, value: batch[idx++]! }
    },
    return: async () => {
      done = true
      await this.close()
      return { done: true, value: undefined }
    },
  }
}
;(
  native.Notifications.prototype as unknown as {
    [Symbol.asyncIterator](): AsyncIterator<Notification>
  }
)[Symbol.asyncIterator] = function (this: NativeNotifications) {
  return {
    next: async () => {
      const n = await this.next()
      if (n === null || n === undefined) {
        return { done: true, value: undefined }
      }
      return { done: false, value: n }
    },
    return: async () => {
      await this.close()
      return { done: true, value: undefined }
    },
  }
}

// ---------------------------------------------------------------------------
// Binary-native query / push wrappers
// ---------------------------------------------------------------------------

type RawCaller = (
  sql: string,
  params: NativeParams,
  paramOids: number[],
  paramFormats: number[],
  resultFormats: number[],
  opts?: QueryOpts,
) => Promise<RawQueryResult>

function buildBinaryQuery(): (
  this: Record<"queryRaw", RawCaller>,
  sql: string,
  params?: readonly unknown[],
  oidsOrOpts?: readonly number[] | QueryOpts,
  maybeOpts?: QueryOpts,
) => Promise<TypedQueryResult> {
  return async function (sql, params, oidsOrOpts, maybeOpts) {
    let oids: readonly number[] = []
    let opts: QueryOpts | undefined
    if (Array.isArray(oidsOrOpts)) {
      oids = oidsOrOpts as readonly number[]
      opts = maybeOpts
    } else if (
      oidsOrOpts !== undefined &&
      oidsOrOpts !== null &&
      typeof oidsOrOpts === "object" &&
      "signal" in (oidsOrOpts as object)
    ) {
      opts = oidsOrOpts as QueryOpts
    }
    const signal = opts?.signal
    const encoded = encodeParams(params ?? [], oids)
    const raw = await this.queryRaw(
      sql,
      encoded.buffers as NativeParams,
      encoded.oids,
      encoded.formats,
      [1],
      signal ? { signal } : undefined,
    )
    return {
      fields: raw.fields,
      rows: decodeRows(raw.rows, raw.fields),
      commandTag: raw.commandTag,
    }
  }
}

Object.defineProperty(native.Connection.prototype, "query", {
  value: buildBinaryQuery(),
  writable: true,
  configurable: true,
})
Object.defineProperty(native.Pool.prototype, "query", {
  value: buildBinaryQuery(),
  writable: true,
  configurable: true,
})

// Pipeline: replace push with a JS-value binary encoder, and wrap execute to
// decode each result's rows per-field.
type NativePipelinePush = (
  sql: string,
  params: NativeParams,
  paramOids: number[],
  paramFormats: number[],
  resultFormats: number[],
) => void
type NativePipelineExecute = (this: NativePipeline) => Promise<RawQueryResult[]>

const _nativePipelinePush = (native.Pipeline.prototype as unknown as { push: NativePipelinePush })
  .push
const _wrappedPipelineExecute = (
  native.Pipeline.prototype as unknown as { execute: NativePipelineExecute }
).execute

Object.defineProperty(native.Pipeline.prototype, "push", {
  value: function (
    this: NativePipeline,
    sql: string,
    params: readonly unknown[] = [],
    oids?: readonly number[],
  ) {
    const oidList = Array.isArray(oids) ? oids : []
    const encoded = encodeParams(params, oidList)
    _nativePipelinePush.call(
      this,
      sql,
      encoded.buffers as NativeParams,
      encoded.oids,
      encoded.formats,
      [1],
    )
    return this
  },
  writable: true,
  configurable: true,
})

Object.defineProperty(native.Pipeline.prototype, "execute", {
  value: async function (this: NativePipeline): Promise<TypedQueryResult[]> {
    const raws = await _wrappedPipelineExecute.call(this)
    const out: TypedQueryResult[] = new Array(raws.length)
    for (let i = 0; i < raws.length; i++) {
      const r = raws[i]!
      out[i] = {
        fields: r.fields,
        rows: decodeRows(r.rows, r.fields),
        commandTag: r.commandTag,
      }
    }
    return out
  },
  writable: true,
  configurable: true,
})

// ---------------------------------------------------------------------------
// binary export (lower-level codec surface, kept for queryRaw callers)
// ---------------------------------------------------------------------------

export const binary = Object.freeze({
  encodeBool,
  encodeInt2,
  encodeInt4,
  encodeInt8,
  encodeFloat4,
  encodeFloat8,
  encodeUuid,
  decodeBool: decodeBoolBin,
  decodeInt2: decodeInt2Bin,
  decodeInt4: decodeInt4Bin,
  decodeInt8: decodeInt8Bin,
  decodeFloat4: decodeFloat4Bin,
  decodeFloat8: decodeFloat8Bin,
  decodeUuid: decodeUuidBin,
  decodeText,
  decodeBytea: decodeByteaBin,
  TYPE_OID,
})

// ---------------------------------------------------------------------------
// Re-export native class values so `instanceof` checks work for consumers.
// ---------------------------------------------------------------------------

export const Connection = native.Connection as unknown as new () => Connection
export const Pool = native.Pool as unknown as new () => Pool
export const Pipeline = native.Pipeline as unknown as new () => Pipeline
export const RowStream = native.RowStream as unknown as new () => RowStream
export const Notifications = native.Notifications as unknown as new () => Notifications
export const CancelToken = native.CancelToken

export type { ConnectOptions, FieldInfo, QueryResult, RawQueryResult, ColumnarResult, Notification }
