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

// Binary codec helpers for the PostgreSQL wire format. Use these to encode
// params for `queryRaw(sql, params, oids, paramFormats=[1, ...], ...)` and to
// decode binary-format result cells.
//
// Each encoder returns a Buffer of the exact server-side wire layout. Each
// decoder takes the raw cell Buffer and returns a native JS value.

const TYPE_OID = Object.freeze({
  BOOL: 16,
  BYTEA: 17,
  INT8: 20,
  INT2: 21,
  INT4: 23,
  TEXT: 25,
  OID: 26,
  FLOAT4: 700,
  FLOAT8: 701,
  UUID: 2950,
});

const binary = Object.freeze({
  // --- encoders (param) ---
  encodeBool(v) {
    const b = Buffer.alloc(1);
    b[0] = v ? 1 : 0;
    return b;
  },
  encodeInt2(v) {
    const b = Buffer.alloc(2);
    b.writeInt16BE(v | 0, 0);
    return b;
  },
  encodeInt4(v) {
    const b = Buffer.alloc(4);
    b.writeInt32BE(v | 0, 0);
    return b;
  },
  encodeInt8(v) {
    const b = Buffer.alloc(8);
    b.writeBigInt64BE(typeof v === 'bigint' ? v : BigInt(v), 0);
    return b;
  },
  encodeFloat4(v) {
    const b = Buffer.alloc(4);
    b.writeFloatBE(v, 0);
    return b;
  },
  encodeFloat8(v) {
    const b = Buffer.alloc(8);
    b.writeDoubleBE(v, 0);
    return b;
  },
  encodeUuid(v) {
    // v is a canonical hex string (with or without hyphens).
    const hex = v.replace(/-/g, '');
    if (hex.length !== 32) throw new Error(`invalid uuid: ${v}`);
    return Buffer.from(hex, 'hex');
  },

  // --- decoders (result) ---
  decodeBool(buf) {
    return buf[0] !== 0;
  },
  decodeInt2(buf) {
    return buf.readInt16BE(0);
  },
  decodeInt4(buf) {
    return buf.readInt32BE(0);
  },
  decodeInt8(buf) {
    return buf.readBigInt64BE(0);
  },
  decodeFloat4(buf) {
    return buf.readFloatBE(0);
  },
  decodeFloat8(buf) {
    return buf.readDoubleBE(0);
  },
  decodeUuid(buf) {
    const h = buf.toString('hex');
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
  },
  decodeText(buf) {
    return buf.toString('utf8');
  },
  decodeBytea(buf) {
    return buf;
  },

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
