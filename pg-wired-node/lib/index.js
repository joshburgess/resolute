'use strict';

// Thin facade over the napi-rs generated ../index.js.
// Responsibilities:
//   1. Re-export the native classes and factory functions.
//   2. Wrap async methods that can throw pg-wired errors so that thrown
//      Errors carry structured fields (`code`, `severity`, `detail`, `hint`,
//      `position`, `kind`) instead of a JSON-encoded message.

const native = require('../index.js');

const ERR_PREFIX = 'pg-wired-error:';

class PgError extends Error {
  constructor(payload) {
    super(payload.message || 'pg-wired error');
    this.name = 'PgError';
    this.kind = payload.kind;
    if (payload.kind === 'pg') {
      this.code = payload.code;
      this.severity = payload.severity;
      if (payload.detail) this.detail = payload.detail;
      if (payload.hint) this.hint = payload.hint;
      if (payload.position) this.position = payload.position;
    }
  }
}

function reshape(err) {
  if (!(err instanceof Error)) return err;
  const msg = err.message || '';
  if (!msg.startsWith(ERR_PREFIX)) return err;
  try {
    const payload = JSON.parse(msg.slice(ERR_PREFIX.length));
    const wrapped = new PgError(payload);
    wrapped.stack = err.stack;
    return wrapped;
  } catch {
    return err;
  }
}

function wrapAsync(fn) {
  return async function (...args) {
    try {
      return await fn.apply(this, args);
    } catch (e) {
      throw reshape(e);
    }
  };
}

class AbortError extends Error {
  constructor(message = 'The operation was aborted') {
    super(message);
    this.name = 'AbortError';
    this.code = 'ABORT_ERR';
  }
}

/// Run a connection operation under an AbortSignal. If `signal` fires while
/// `op()` is in flight, the connection's cancel token is sent, the pending
/// server work is aborted at the backend, and this function rejects with the
/// signal's reason (or an AbortError).
async function withSignal(conn, signal, op) {
  if (!signal) return op();
  if (signal.aborted) {
    throw signal.reason instanceof Error
      ? signal.reason
      : new AbortError(signal.reason);
  }
  const token = conn.cancelToken();
  let aborted = false;
  const onAbort = () => {
    aborted = true;
    token.cancel().catch(() => {});
  };
  signal.addEventListener('abort', onAbort, { once: true });
  try {
    return await op();
  } catch (e) {
    if (aborted) {
      throw signal.reason instanceof Error
        ? signal.reason
        : new AbortError(signal.reason);
    }
    throw e;
  } finally {
    signal.removeEventListener('abort', onAbort);
  }
}

/// Detect if the last argument is a plain options object `{ signal }`. Only
/// an exact shape match is treated as options. Anything else is passed
/// through as a positional argument.
function extractSignalOpts(args) {
  if (args.length === 0) return [args, undefined];
  const last = args[args.length - 1];
  if (
    last !== null &&
    typeof last === 'object' &&
    !Array.isArray(last) &&
    !Buffer.isBuffer(last) &&
    ('signal' in last)
  ) {
    return [args.slice(0, -1), last.signal];
  }
  return [args, undefined];
}

function wrapConnectionAsync(fn) {
  return async function (...args) {
    const [positional, signal] = extractSignalOpts(args);
    try {
      if (signal) {
        return await withSignal(this, signal, () =>
          fn.apply(this, positional),
        );
      }
      return await fn.apply(this, positional);
    } catch (e) {
      throw reshape(e);
    }
  };
}

function parsePgUrl(u) {
  const url = new URL(u);
  if (url.protocol !== 'postgres:' && url.protocol !== 'postgresql:') {
    throw new Error(
      `pg-wired: unsupported URL scheme ${url.protocol} (expected postgres: or postgresql:)`,
    );
  }
  const out = {
    host: url.hostname || '127.0.0.1',
    port: url.port ? Number(url.port) : 5432,
    user: url.username ? decodeURIComponent(url.username) : 'postgres',
    password: url.password ? decodeURIComponent(url.password) : '',
    database: url.pathname
      ? decodeURIComponent(url.pathname.replace(/^\//, '')) || 'postgres'
      : 'postgres',
  };
  const sslmode = url.searchParams.get('sslmode');
  if (sslmode) out.sslmode = sslmode;
  return out;
}

function normalizeOptions(optsOrUrl) {
  if (typeof optsOrUrl === 'string') return parsePgUrl(optsOrUrl);
  return optsOrUrl;
}

/// Read a single cell out of a ColumnarResult. Returns `null` for NULL cells,
/// else a substring view over the concatenated data string.
function columnarCell(result, row, col) {
  const idx = row * result.cols + col;
  const nullByte = result.nulls[idx >> 3];
  if (nullByte !== undefined && (nullByte & (1 << (idx & 7))) !== 0) return null;
  const start = result.offsets[idx];
  const end = result.offsets[idx + 1];
  return result.data.slice(start, end);
}

function wrapMethods(proto, names, wrapper = wrapAsync) {
  for (const name of names) {
    const original = proto[name];
    if (typeof original !== 'function') continue;
    Object.defineProperty(proto, name, {
      value: wrapper(original),
      writable: true,
      configurable: true,
    });
  }
}

// Connection methods accept an optional trailing `{ signal }` options object
// that wires an AbortSignal to the backend's cancel token.
const CONN_METHODS_WITH_SIGNAL = [
  'query',
  'queryRaw',
  'simpleQuery',
  'queryColumnar',
  'simpleQueryColumnar',
  'queryStream',
  'copyIn',
  'copyOut',
];
wrapMethods(
  native.Connection.prototype,
  CONN_METHODS_WITH_SIGNAL,
  wrapConnectionAsync,
);
// `close` has no in-flight query to cancel, use the plain wrapper.
wrapMethods(native.Connection.prototype, ['close']);
wrapMethods(native.Pool.prototype, ['query', 'queryRaw', 'simpleQuery', 'aliveCount', 'close']);
wrapMethods(native.Pipeline.prototype, ['execute']);
wrapMethods(native.RowStream.prototype, ['next', 'nextBatch']);
wrapMethods(native.Notifications.prototype, ['next']);
wrapMethods(native.CancelToken.prototype, ['cancel']);

async function connect(optsOrUrl) {
  try {
    return await native.connect(normalizeOptions(optsOrUrl));
  } catch (e) {
    throw reshape(e);
  }
}

async function createPool(optsOrUrl, size) {
  try {
    return await native.createPool(normalizeOptions(optsOrUrl), size);
  } catch (e) {
    throw reshape(e);
  }
}

/// Build a transaction handle around a connection. The handle proxies all
/// query methods straight to `conn`, threading `signal` into each call so an
/// outer abort cancels every step. `savepoint(name?, fn)` opens a SAVEPOINT,
/// runs the inner callback with a nested handle, then RELEASEs or ROLLBACKs TO
/// SAVEPOINT on success or failure.
function makeTxHandle(conn, signal, state) {
  const handle = {};
  for (const name of CONN_METHODS_WITH_SIGNAL) {
    if (typeof conn[name] !== 'function') continue;
    handle[name] = function (...args) {
      const [positional, userSignal] = extractSignalOpts(args);
      const merged = userSignal ?? signal;
      const tail = merged ? [{ signal: merged }] : [];
      return conn[name](...positional, ...tail);
    };
  }
  handle.pipeline = () => conn.pipeline();
  handle.cancelToken = () => conn.cancelToken();
  handle.savepoint = async function (nameOrFn, maybeFn) {
    const fn = typeof nameOrFn === 'function' ? nameOrFn : maybeFn;
    const rawName = typeof nameOrFn === 'string'
      ? nameOrFn
      : `sp_${++state.spCounter}`;
    // Identifier safety: SAVEPOINT names must be quoted if they contain
    // anything but [A-Za-z0-9_]. Keep it simple and reject bad names.
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(rawName)) {
      throw new Error(`invalid savepoint name: ${rawName}`);
    }
    await conn.simpleQuery(`SAVEPOINT ${rawName}`, { signal });
    try {
      const result = await fn(makeTxHandle(conn, signal, state));
      await conn.simpleQuery(`RELEASE SAVEPOINT ${rawName}`, { signal });
      return result;
    } catch (err) {
      try {
        await conn.simpleQuery(`ROLLBACK TO SAVEPOINT ${rawName}`);
      } catch {
        // Rollback best-effort. Outer tx will ROLLBACK on its own.
      }
      throw err;
    }
  };
  return handle;
}

/// Run `fn` inside a database transaction on `conn`. Commits on success,
/// rolls back on throw, and rethrows the original error.
///
/// Options:
/// - `signal`: AbortSignal; aborts the current step and rolls back.
/// - `isolationLevel`: "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE".
/// - `readOnly`: boolean; adds READ ONLY to the BEGIN.
/// - `deferrable`: boolean; adds DEFERRABLE (SERIALIZABLE READ ONLY only).
async function transaction(conn, fn, opts = {}) {
  const { signal, isolationLevel, readOnly, deferrable } = opts;
  const parts = ['BEGIN'];
  if (isolationLevel) {
    const allowed = new Set([
      'READ COMMITTED',
      'REPEATABLE READ',
      'SERIALIZABLE',
    ]);
    const level = isolationLevel.toUpperCase();
    if (!allowed.has(level)) {
      throw new Error(`invalid isolationLevel: ${isolationLevel}`);
    }
    parts.push(`ISOLATION LEVEL ${level}`);
  }
  if (readOnly) parts.push('READ ONLY');
  if (deferrable) parts.push('DEFERRABLE');
  const beginSql = parts.join(' ');

  await conn.simpleQuery(beginSql, { signal });
  const state = { spCounter: 0 };
  try {
    const result = await fn(makeTxHandle(conn, signal, state));
    await conn.simpleQuery('COMMIT', { signal });
    return result;
  } catch (err) {
    try {
      // ROLLBACK without the signal, so even if abort fired we still clean
      // up. If the connection is dead, this throws; swallow and rethrow the
      // original error.
      await conn.simpleQuery('ROLLBACK');
    } catch {
      // best-effort cleanup
    }
    throw err;
  }
}

native.Connection.prototype.transaction = function (fn, opts) {
  return transaction(this, fn, opts);
};

// Async iterable sugar over RowStream. Drains in batches of 256 to amortize
// the V8 boundary cost across many rows per napi call.
const ROW_STREAM_BATCH = 256;
native.RowStream.prototype[Symbol.asyncIterator] = function () {
  const self = this;
  let batch = null;
  let idx = 0;
  let done = false;
  return {
    next: async () => {
      if (done) return { done: true, value: undefined };
      if (batch !== null && idx < batch.length) {
        return { done: false, value: batch[idx++] };
      }
      batch = await self.nextBatch(ROW_STREAM_BATCH);
      idx = 0;
      if (!batch || batch.length === 0) {
        done = true;
        return { done: true, value: undefined };
      }
      return { done: false, value: batch[idx++] };
    },
    return: async () => {
      done = true;
      await self.close();
      return { done: true, value: undefined };
    },
  };
};

// Async iterable sugar over Notifications.
native.Notifications.prototype[Symbol.asyncIterator] = function () {
  const self = this;
  return {
    next: async () => {
      const n = await self.next();
      if (n === null || n === undefined) {
        return { done: true, value: undefined };
      }
      return { done: false, value: n };
    },
    return: async () => {
      await self.close();
      return { done: true, value: undefined };
    },
  };
};

// ---------------------------------------------------------------------------
// Binary wire codecs
// ---------------------------------------------------------------------------

const TYPE_OID = Object.freeze({
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
});

// Epoch offset between PostgreSQL (2000-01-01 UTC) and JS (1970-01-01 UTC).
const PG_EPOCH_MS = 946684800000;
const PG_EPOCH_MICROS = 946684800000000n;
const MICROS_PER_MS = 1000n;
const MS_PER_DAY = 86400000;

function encodeBool(v) {
  const b = Buffer.alloc(1);
  b[0] = v ? 1 : 0;
  return b;
}
function decodeBoolBin(buf) {
  return buf[0] !== 0;
}

function encodeInt2(v) {
  const b = Buffer.alloc(2);
  b.writeInt16BE(Number(v), 0);
  return b;
}
function decodeInt2Bin(buf) {
  return buf.readInt16BE(0);
}

function encodeInt4(v) {
  const b = Buffer.alloc(4);
  b.writeInt32BE(Number(v), 0);
  return b;
}
function decodeInt4Bin(buf) {
  return buf.readInt32BE(0);
}

function encodeInt8(v) {
  const b = Buffer.alloc(8);
  b.writeBigInt64BE(typeof v === 'bigint' ? v : BigInt(v), 0);
  return b;
}
function decodeInt8Bin(buf) {
  return buf.readBigInt64BE(0);
}

function encodeFloat4(v) {
  const b = Buffer.alloc(4);
  b.writeFloatBE(Number(v), 0);
  return b;
}
function decodeFloat4Bin(buf) {
  return buf.readFloatBE(0);
}

function encodeFloat8(v) {
  const b = Buffer.alloc(8);
  b.writeDoubleBE(Number(v), 0);
  return b;
}
function decodeFloat8Bin(buf) {
  return buf.readDoubleBE(0);
}

function encodeText(v) {
  return typeof v === 'string' ? Buffer.from(v, 'utf8') : Buffer.from(String(v), 'utf8');
}
function decodeText(buf) {
  return buf.toString('utf8');
}

function encodeBytea(v) {
  if (Buffer.isBuffer(v)) return v;
  if (v instanceof Uint8Array) return Buffer.from(v.buffer, v.byteOffset, v.byteLength);
  throw new TypeError('bytea param must be Buffer or Uint8Array');
}
function decodeByteaBin(buf) {
  return Buffer.from(buf);
}
function decodeByteaText(buf) {
  const s = buf.toString('utf8');
  if (s.startsWith('\\x')) return Buffer.from(s.slice(2), 'hex');
  // Legacy octal-escape form. Rarely seen since bytea_output=hex is the
  // default since 9.0, but handle it for completeness.
  const out = [];
  for (let i = 0; i < s.length; ) {
    if (s[i] === '\\' && s[i + 1] === '\\') { out.push(0x5c); i += 2; }
    else if (s[i] === '\\' && /[0-7]/.test(s[i + 1])) {
      out.push(parseInt(s.slice(i + 1, i + 4), 8));
      i += 4;
    } else { out.push(s.charCodeAt(i)); i += 1; }
  }
  return Buffer.from(out);
}

function encodeUuid(v) {
  if (Buffer.isBuffer(v) && v.length === 16) return v;
  if (typeof v !== 'string') throw new TypeError('uuid param must be string or 16-byte Buffer');
  const hex = v.replace(/-/g, '');
  if (hex.length !== 32) throw new Error(`invalid uuid: ${v}`);
  return Buffer.from(hex, 'hex');
}
function decodeUuidBin(buf) {
  const h = buf.toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

// Timestamp / timestamptz: i64 microseconds since 2000-01-01 UTC.
// PG uses i64 min/max sentinel values for -infinity / +infinity.
const PG_INT64_MIN = -0x8000000000000000n;
const PG_INT64_MAX = 0x7fffffffffffffffn;
function encodeTimestamptz(v) {
  if (!(v instanceof Date)) throw new TypeError('timestamptz param must be a Date');
  const ms = BigInt(v.getTime());
  const micros = (ms - BigInt(PG_EPOCH_MS)) * MICROS_PER_MS;
  const b = Buffer.alloc(8);
  b.writeBigInt64BE(micros, 0);
  return b;
}
function decodeTimestamptzBin(buf) {
  const micros = buf.readBigInt64BE(0);
  if (micros === PG_INT64_MAX) return new Date(8640000000000000); // +Infinity → JS max safe date
  if (micros === PG_INT64_MIN) return new Date(-8640000000000000);
  const ms = Number(micros / MICROS_PER_MS) + PG_EPOCH_MS;
  return new Date(ms);
}

// Date: i32 days since 2000-01-01 UTC.
function encodeDate(v) {
  if (!(v instanceof Date)) throw new TypeError('date param must be a Date');
  // Snap to UTC midnight of the given day.
  const utcMs = Date.UTC(v.getUTCFullYear(), v.getUTCMonth(), v.getUTCDate());
  const days = Math.round((utcMs - PG_EPOCH_MS) / MS_PER_DAY);
  const b = Buffer.alloc(4);
  b.writeInt32BE(days, 0);
  return b;
}
function decodeDateBin(buf) {
  const days = buf.readInt32BE(0);
  if (days === 0x7fffffff) return new Date(8640000000000000);
  if (days === -0x80000000) return new Date(-8640000000000000);
  return new Date(PG_EPOCH_MS + days * MS_PER_DAY);
}

// JSON/JSONB: we keep json as text; jsonb binary has a 1-byte version header.
function encodeJson(v) {
  return Buffer.from(typeof v === 'string' ? v : JSON.stringify(v), 'utf8');
}
function decodeJsonText(buf) {
  return JSON.parse(buf.toString('utf8'));
}
function encodeJsonb(v) {
  const body = typeof v === 'string' ? v : JSON.stringify(v);
  const b = Buffer.alloc(1 + Buffer.byteLength(body, 'utf8'));
  b[0] = 1; // version
  b.write(body, 1, 'utf8');
  return b;
}
function decodeJsonbBin(buf) {
  if (buf[0] !== 1) throw new Error(`unknown jsonb version: ${buf[0]}`);
  return JSON.parse(buf.slice(1).toString('utf8'));
}

// Registry: oid → { binary, encode(js) → Buffer, decodeBin(buf) → js, decodeText(buf) → js }
// `binary: true` means we request binary wire format both ways. If the user or
// the server returns text, `decodeText` still handles it.
function textDecoder(coerce) {
  return (buf) => coerce(buf.toString('utf8'));
}

const CODECS = new Map([
  [TYPE_OID.BOOL, {
    binary: true,
    encode: encodeBool,
    decodeBin: decodeBoolBin,
    decodeText: (buf) => buf.toString('utf8') === 't',
  }],
  [TYPE_OID.BYTEA, {
    binary: true,
    encode: encodeBytea,
    decodeBin: decodeByteaBin,
    decodeText: decodeByteaText,
  }],
  [TYPE_OID.INT2, {
    binary: true,
    encode: encodeInt2,
    decodeBin: decodeInt2Bin,
    decodeText: textDecoder(Number),
  }],
  [TYPE_OID.INT4, {
    binary: true,
    encode: encodeInt4,
    decodeBin: decodeInt4Bin,
    decodeText: textDecoder(Number),
  }],
  [TYPE_OID.OID, {
    binary: true,
    encode: (v) => {
      const b = Buffer.alloc(4);
      b.writeUInt32BE(Number(v), 0);
      return b;
    },
    decodeBin: (buf) => buf.readUInt32BE(0),
    decodeText: textDecoder(Number),
  }],
  [TYPE_OID.INT8, {
    binary: true,
    encode: encodeInt8,
    decodeBin: decodeInt8Bin,
    decodeText: textDecoder(BigInt),
  }],
  [TYPE_OID.FLOAT4, {
    binary: true,
    encode: encodeFloat4,
    decodeBin: decodeFloat4Bin,
    decodeText: textDecoder(Number),
  }],
  [TYPE_OID.FLOAT8, {
    binary: true,
    encode: encodeFloat8,
    decodeBin: decodeFloat8Bin,
    decodeText: textDecoder(Number),
  }],
  [TYPE_OID.TEXT, { binary: true, encode: encodeText, decodeBin: decodeText, decodeText }],
  [TYPE_OID.VARCHAR, { binary: true, encode: encodeText, decodeBin: decodeText, decodeText }],
  [TYPE_OID.BPCHAR, { binary: true, encode: encodeText, decodeBin: decodeText, decodeText }],
  [TYPE_OID.NAME, { binary: true, encode: encodeText, decodeBin: decodeText, decodeText }],
  [TYPE_OID.CHAR, { binary: true, encode: encodeText, decodeBin: decodeText, decodeText }],
  [TYPE_OID.UUID, {
    binary: true,
    encode: encodeUuid,
    decodeBin: decodeUuidBin,
    decodeText: (buf) => buf.toString('utf8'),
  }],
  [TYPE_OID.TIMESTAMP, {
    binary: true,
    encode: encodeTimestamptz,
    decodeBin: decodeTimestamptzBin,
    decodeText: (buf) => new Date(buf.toString('utf8') + 'Z'),
  }],
  [TYPE_OID.TIMESTAMPTZ, {
    binary: true,
    encode: encodeTimestamptz,
    decodeBin: decodeTimestamptzBin,
    decodeText: (buf) => new Date(buf.toString('utf8')),
  }],
  [TYPE_OID.DATE, {
    binary: true,
    encode: encodeDate,
    decodeBin: decodeDateBin,
    decodeText: (buf) => new Date(buf.toString('utf8') + 'T00:00:00Z'),
  }],
  // numeric: use text on both sides. Binary format is (ndigits, weight, sign,
  // dscale, digits[]) in base-10000, decoding it into a string is doable but
  // not worth the risk of subtle rounding. Text is already arbitrary precision.
  [TYPE_OID.NUMERIC, {
    binary: false,
    encode: (v) => Buffer.from(String(v), 'utf8'),
    decodeBin: (buf) => buf.toString('utf8'), // won't happen; kept for safety
    decodeText: (buf) => buf.toString('utf8'),
  }],
  [TYPE_OID.JSON, {
    binary: false,
    encode: encodeJson,
    decodeBin: decodeJsonText,
    decodeText: decodeJsonText,
  }],
  [TYPE_OID.JSONB, {
    binary: true,
    encode: encodeJsonb,
    decodeBin: decodeJsonbBin,
    decodeText: decodeJsonText,
  }],
  [TYPE_OID.TIME, {
    // Time is i64 micros since midnight in binary. Keep as text for simpler
    // JS consumption; callers who need microsecond precision can parse it.
    binary: false,
    encode: (v) => Buffer.from(String(v), 'utf8'),
    decodeBin: (buf) => buf.toString('utf8'),
    decodeText: (buf) => buf.toString('utf8'),
  }],
]);

// Infer a PostgreSQL OID from a JS value. Used when the caller doesn't pass
// an explicit oid list.
function inferOid(v) {
  if (v === null || v === undefined) return 0;
  if (typeof v === 'boolean') return TYPE_OID.BOOL;
  if (typeof v === 'bigint') return TYPE_OID.INT8;
  if (typeof v === 'number') return TYPE_OID.FLOAT8;
  if (typeof v === 'string') return TYPE_OID.TEXT;
  if (Buffer.isBuffer(v) || v instanceof Uint8Array) return TYPE_OID.BYTEA;
  if (v instanceof Date) return TYPE_OID.TIMESTAMPTZ;
  if (typeof v === 'object') return TYPE_OID.JSONB;
  return 0;
}

/// Encode a JS param array into (buffers, formats, oids) using the OID registry.
/// If `oids` is empty, each oid is inferred from its JS value. If an oid maps
/// to a known codec, we use its binary/text encoder; otherwise we stringify.
function encodeParams(values, oids) {
  const n = values.length;
  const buffers = new Array(n);
  const formats = new Array(n);
  const resolvedOids = new Array(n);
  for (let i = 0; i < n; i++) {
    const v = values[i];
    const declared = oids.length === n ? (oids[i] | 0) : 0;
    const oid = declared || inferOid(v);
    resolvedOids[i] = oid;
    if (v === null || v === undefined) {
      buffers[i] = null;
      formats[i] = oid === 0 ? 0 : (CODECS.get(oid)?.binary ? 1 : 0);
      continue;
    }
    // Buffer param passes through unchanged in binary format. Useful for
    // pre-encoded payloads and for types we don't cover.
    if (Buffer.isBuffer(v)) {
      buffers[i] = v;
      formats[i] = 1;
      continue;
    }
    const codec = oid ? CODECS.get(oid) : undefined;
    if (codec) {
      buffers[i] = codec.encode(v);
      formats[i] = codec.binary ? 1 : 0;
    } else {
      // Unknown OID: fall back to text. Works for most scalar types because
      // Postgres parses text input per type's input function.
      buffers[i] = Buffer.from(String(v), 'utf8');
      formats[i] = 0;
    }
  }
  return { buffers, formats, oids: resolvedOids };
}

function decodeCell(buf, field) {
  if (buf === null || buf === undefined) return null;
  const codec = CODECS.get(field.typeOid);
  if (!codec) {
    // Unknown type: return the raw bytes when binary, decoded text otherwise.
    return field.format === 1 ? Buffer.from(buf) : buf.toString('utf8');
  }
  return field.format === 1 ? codec.decodeBin(buf) : codec.decodeText(buf);
}

function decodeRows(rows, fields) {
  const out = new Array(rows.length);
  for (let r = 0; r < rows.length; r++) {
    const row = rows[r];
    const decoded = new Array(row.length);
    for (let c = 0; c < row.length; c++) {
      decoded[c] = decodeCell(row[c], fields[c]);
    }
    out[r] = decoded;
  }
  return out;
}

// Binary-native query that delegates to the wrapped queryRaw. The param list
// is encoded via the OID registry, result columns are requested uniformly in
// binary (format = [1]), and rows are decoded per `field.typeOid` to native
// JS values. Accepts a trailing `{signal}` (in either oids-slot or opts-slot)
// for abort support, which the wrapped queryRaw forwards to withSignal.
function buildBinaryQuery(rawMethodName) {
  return async function (sql, params = [], oids, opts) {
    if (
      oids !== undefined &&
      oids !== null &&
      !Array.isArray(oids) &&
      typeof oids === 'object' &&
      'signal' in oids
    ) {
      opts = oids;
      oids = undefined;
    }
    const signal = opts && typeof opts === 'object' ? opts.signal : undefined;
    const oidList = Array.isArray(oids) ? oids : [];
    const { buffers, formats, oids: resolvedOids } = encodeParams(params, oidList);
    const tail = signal ? [{ signal }] : [];
    const raw = await this[rawMethodName](
      sql,
      buffers,
      resolvedOids,
      formats,
      [1],
      ...tail,
    );
    return {
      fields: raw.fields,
      rows: decodeRows(raw.rows, raw.fields),
      commandTag: raw.commandTag,
    };
  };
}

// Replace the native (text-format) query with the binary-native wrapper on
// both Connection and Pool. This runs after wrapMethods, so `queryRaw` on
// both prototypes is already signal + error aware.
Object.defineProperty(native.Connection.prototype, 'query', {
  value: buildBinaryQuery('queryRaw'),
  writable: true,
  configurable: true,
});
Object.defineProperty(native.Pool.prototype, 'query', {
  value: buildBinaryQuery('queryRaw'),
  writable: true,
  configurable: true,
});

const binary = Object.freeze({
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
});

module.exports = {
  connect,
  createPool,
  Connection: native.Connection,
  Pool: native.Pool,
  Pipeline: native.Pipeline,
  RowStream: native.RowStream,
  Notifications: native.Notifications,
  CancelToken: native.CancelToken,
  PgError,
  AbortError,
  parsePgUrl,
  columnarCell,
  binary,
  withSignal,
  transaction,
};
