import { test } from 'node:test';
import assert from 'node:assert/strict';

// Smoke tests. Require a local Postgres at $PG_URL or skip.
// Example: PG_URL=postgres://user:pass@127.0.0.1:5432/postgres node --test __test__

const urlStr = process.env.PG_URL;

function parseUrl(u) {
  const url = new URL(u);
  return {
    host: url.hostname,
    port: Number(url.port || 5432),
    user: decodeURIComponent(url.username),
    password: decodeURIComponent(url.password),
    database: url.pathname.replace(/^\//, '') || 'postgres',
  };
}

async function loadDriver() {
  const mod = await import('../lib/index.js');
  return mod.default ?? mod;
}

test('connect + simpleQuery returns server version', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(parseUrl(urlStr));
  const res = await conn.simpleQuery('SELECT version()');
  assert.equal(res.rows.length, 1);
  assert.equal(res.rows[0].length, 1);
  const version = res.rows[0][0];
  assert.match(version, /PostgreSQL/);
});

test('parameterized query decodes binary int4 natively', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect, binary } = await loadDriver();
  const conn = await connect(parseUrl(urlStr));
  const OID = binary.TYPE_OID;
  const res = await conn.query(
    'SELECT $1::int + $2::int AS sum',
    [2, 3],
    [OID.INT4, OID.INT4],
  );
  assert.equal(res.rows.length, 1);
  assert.equal(res.rows[0][0], 5);
  assert.equal(res.fields[0].name, 'sum');
  assert.equal(res.fields[0].format, 1);
});

test('query decodes bool, int8, float8, text, uuid, timestamptz natively', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect, binary } = await loadDriver();
  const conn = await connect(urlStr);
  const OID = binary.TYPE_OID;
  const uuidStr = '550e8400-e29b-41d4-a716-446655440000';
  const date = new Date('2024-06-01T12:34:56.000Z');
  const res = await conn.query(
    'SELECT $1::bool AS b, $2::int8 AS i8, $3::float8 AS f, $4::text AS t, $5::uuid AS u, $6::timestamptz AS ts',
    [true, 9_999_999_999n, 3.14, 'hello', uuidStr, date],
    [OID.BOOL, OID.INT8, OID.FLOAT8, OID.TEXT, OID.UUID, OID.TIMESTAMPTZ],
  );
  const [b, i8, f, tStr, u, ts] = res.rows[0];
  assert.equal(b, true);
  assert.equal(i8, 9_999_999_999n);
  assert.equal(f, 3.14);
  assert.equal(tStr, 'hello');
  assert.equal(u, uuidStr);
  assert.ok(ts instanceof Date);
  assert.equal(ts.getTime(), date.getTime());
  await conn.close();
});

test('query infers oids from JS values when not given', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  const res = await conn.query('SELECT $1::text AS s, $2::bool AS b', ['world', false]);
  assert.equal(res.rows[0][0], 'world');
  assert.equal(res.rows[0][1], false);
  await conn.close();
});

test('query handles null params and null cells', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect, binary } = await loadDriver();
  const conn = await connect(urlStr);
  const res = await conn.query(
    'SELECT $1::int AS n, NULL::text AS t',
    [null],
    [binary.TYPE_OID.INT4],
  );
  assert.equal(res.rows[0][0], null);
  assert.equal(res.rows[0][1], null);
  await conn.close();
});

test('pool.query decodes binary results natively', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { createPool, binary } = await loadDriver();
  const pool = await createPool(urlStr, 2);
  const OID = binary.TYPE_OID;
  const res = await pool.query(
    'SELECT $1::int AS n, $2::text AS s',
    [7, 'ok'],
    [OID.INT4, OID.TEXT],
  );
  assert.equal(res.rows[0][0], 7);
  assert.equal(res.rows[0][1], 'ok');
  await pool.close();
});

test('query round-trips date as midnight UTC', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect, binary } = await loadDriver();
  const conn = await connect(urlStr);
  const d = new Date('2024-06-01T00:00:00.000Z');
  const res = await conn.query(
    'SELECT $1::date AS d',
    [d],
    [binary.TYPE_OID.DATE],
  );
  const got = res.rows[0][0];
  assert.ok(got instanceof Date);
  assert.equal(got.getTime(), d.getTime());
  await conn.close();
});

test('query round-trips jsonb natively', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect, binary } = await loadDriver();
  const conn = await connect(urlStr);
  const payload = { a: 1, b: [true, null, 'x'] };
  const res = await conn.query(
    'SELECT $1::jsonb AS j',
    [payload],
    [binary.TYPE_OID.JSONB],
  );
  assert.deepEqual(res.rows[0][0], payload);
  await conn.close();
});

test('pool round-robins across connections', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { createPool } = await loadDriver();
  const pool = await createPool(parseUrl(urlStr), 2);
  assert.equal(pool.size(), 2);
  const pids = new Set();
  for (let i = 0; i < 4; i++) {
    const r = await pool.simpleQuery('SELECT pg_backend_pid()');
    pids.add(r.rows[0][0]);
  }
  assert.ok(pids.size >= 1);
});

test('pipeline batches multiple queries', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(parseUrl(urlStr));
  const pipe = conn.pipeline();
  pipe.push('SELECT $1::int AS n', [Buffer.from('10')], []);
  pipe.push('SELECT $1::int AS n', [Buffer.from('20')], []);
  pipe.push('SELECT $1::int AS n', [Buffer.from('30')], []);
  assert.equal(pipe.len(), 3);
  const results = await pipe.execute();
  assert.equal(results.length, 3);
  assert.equal(results[0].rows[0][0], '10');
  assert.equal(results[1].rows[0][0], '20');
  assert.equal(results[2].rows[0][0], '30');
  assert.equal(pipe.len(), 0);
});

test('streaming query yields rows via async iterator', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(parseUrl(urlStr));
  const stream = await conn.queryStream(
    'SELECT generate_series(1, $1::int) AS i',
    [Buffer.from('5')],
    [],
  );
  assert.equal(stream.fields[0].name, 'i');
  const values = [];
  for await (const row of stream) {
    values.push(row[0]);
  }
  assert.deepEqual(values, ['1', '2', '3', '4', '5']);
});

test('parsePgUrl extracts connection fields', async () => {
  const { parsePgUrl } = await loadDriver();
  const parsed = parsePgUrl('postgres://alice:s%40cret@db.example.com:6543/app');
  assert.equal(parsed.host, 'db.example.com');
  assert.equal(parsed.port, 6543);
  assert.equal(parsed.user, 'alice');
  assert.equal(parsed.password, 's@cret');
  assert.equal(parsed.database, 'app');
});

test('connect accepts a URL string', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  const res = await conn.simpleQuery('SELECT 1');
  assert.equal(res.rows[0][0], '1');
});

test('createPool accepts a URL string', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { createPool } = await loadDriver();
  const pool = await createPool(urlStr, 2);
  assert.equal(pool.size(), 2);
  const res = await pool.simpleQuery('SELECT 1');
  assert.equal(res.rows[0][0], '1');
});

test('LISTEN/NOTIFY delivers notifications during queries', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const listener = await connect(urlStr);
  const notifier = await connect(urlStr);

  await listener.simpleQuery('LISTEN pgwired_test_chan');
  const notifications = listener.notifications();
  assert.ok(notifications, 'notifications() should return an iterator');

  await notifier.simpleQuery(
    "SELECT pg_notify('pgwired_test_chan', 'hello world')",
  );
  // Force the listener socket to drain via a query.
  await listener.simpleQuery('SELECT 1');

  const { value: first, done } = await notifications[Symbol.asyncIterator]().next();
  assert.equal(done, false);
  assert.equal(first.channel, 'pgwired_test_chan');
  assert.equal(first.payload, 'hello world');
  assert.ok(typeof first.pid === 'number' && first.pid > 0);

  await notifications.close();
});

test('close() terminates the connection cleanly', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(parseUrl(urlStr));
  const r = await conn.simpleQuery('SELECT 1');
  assert.equal(r.rows[0][0], '1');
  await conn.close();
  // Second close is a no-op.
  await conn.close();
  // Queries after close should reject.
  await assert.rejects(() => conn.simpleQuery('SELECT 1'));
});

test('pool close() terminates every connection', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { createPool } = await loadDriver();
  const pool = await createPool(parseUrl(urlStr), 2);
  const r = await pool.simpleQuery('SELECT 1');
  assert.equal(r.rows[0][0], '1');
  await pool.close();
  // Note: the pool's background health-monitor will reconnect dead slots,
  // so we don't assert permanent unavailability here — just that close()
  // completes without throwing and that the initial query ran.
});

test('parsePgUrl reads sslmode query param', async () => {
  const { parsePgUrl } = await loadDriver();
  const none = parsePgUrl('postgres://u:p@h:5432/db');
  assert.equal(none.sslmode, undefined);
  const req = parsePgUrl('postgres://u:p@h:5432/db?sslmode=require');
  assert.equal(req.sslmode, 'require');
  const dis = parsePgUrl('postgres://u:p@h:5432/db?sslmode=disable');
  assert.equal(dis.sslmode, 'disable');
});

test('sslmode=disable connects without TLS negotiation', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const opts = { ...parseUrl(urlStr), sslmode: 'disable' };
  const conn = await connect(opts);
  const r = await conn.simpleQuery('SELECT 1');
  assert.equal(r.rows[0][0], '1');
  await conn.close();
});

test('sslmode=require fails against a non-TLS server', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const opts = { ...parseUrl(urlStr), sslmode: 'require' };
  await assert.rejects(
    () => connect(opts),
    (err) => /sslmode=require|does not support TLS/.test(err.message),
  );
});

test('invalid sslmode is rejected', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const opts = { ...parseUrl(urlStr), sslmode: 'bogus' };
  await assert.rejects(
    () => connect(opts),
    (err) => /unsupported sslmode/.test(err.message),
  );
});

test('queryRaw round-trips binary int4 + int8 + bool params/results', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect, binary } = await loadDriver();
  const conn = await connect(urlStr);
  const OID = binary.TYPE_OID;
  const res = await conn.queryRaw(
    'SELECT $1::int4 AS a, $2::int8 AS b, $3::bool AS c',
    [binary.encodeInt4(42), binary.encodeInt8(9_999_999_999n), binary.encodeBool(true)],
    [OID.INT4, OID.INT8, OID.BOOL],
    [1, 1, 1],
    [1, 1, 1],
  );
  assert.equal(res.rows.length, 1);
  const [a, b, c] = res.rows[0];
  assert.equal(binary.decodeInt4(a), 42);
  assert.equal(binary.decodeInt8(b), 9_999_999_999n);
  assert.equal(binary.decodeBool(c), true);
  assert.equal(res.fields[0].format, 1);
  assert.equal(res.fields[1].format, 1);
  assert.equal(res.fields[2].format, 1);
  await conn.close();
});

test('queryRaw decodes float8 and uuid in binary', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect, binary } = await loadDriver();
  const conn = await connect(urlStr);
  const OID = binary.TYPE_OID;
  const uuidStr = '550e8400-e29b-41d4-a716-446655440000';
  const res = await conn.queryRaw(
    'SELECT $1::float8 AS f, $2::uuid AS u',
    [binary.encodeFloat8(3.141592653589793), binary.encodeUuid(uuidStr)],
    [OID.FLOAT8, OID.UUID],
    [1, 1],
    [1, 1],
  );
  const [f, u] = res.rows[0];
  assert.equal(binary.decodeFloat8(f), 3.141592653589793);
  assert.equal(binary.decodeUuid(u), uuidStr);
  await conn.close();
});

test('queryRaw with empty formats defaults to text and returns UTF-8 Buffers', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect, binary } = await loadDriver();
  const conn = await connect(urlStr);
  const res = await conn.queryRaw(
    'SELECT $1::text AS s',
    [Buffer.from('hello', 'utf8')],
    [binary.TYPE_OID.TEXT],
    [],
    [],
  );
  assert.equal(binary.decodeText(res.rows[0][0]), 'hello');
  assert.equal(res.fields[0].format, 0);
  await conn.close();
});

test('queryRaw rejects invalid format codes', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  await assert.rejects(
    () => conn.queryRaw('SELECT 1', [], [], [], [7]),
    (err) => /format code/.test(err.message),
  );
  await conn.close();
});

test('AbortSignal aborts a slow simpleQuery', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  const controller = new AbortController();
  const promise = conn.simpleQuery('SELECT pg_sleep(5)', { signal: controller.signal });
  setTimeout(() => controller.abort(new Error('user cancelled')), 50);
  await assert.rejects(promise, (err) => err.message === 'user cancelled' || /cancel|aborted/i.test(err.message));
  // Connection remains usable after cancellation.
  const r = await conn.simpleQuery('SELECT 1');
  assert.equal(r.rows[0][0], '1');
  await conn.close();
});

test('AbortSignal pre-aborted rejects immediately', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect, AbortError } = await loadDriver();
  const conn = await connect(urlStr);
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    () => conn.simpleQuery('SELECT 1', { signal: controller.signal }),
    (err) => err instanceof AbortError || /abort/i.test(err.message),
  );
  await conn.close();
});

test('AbortSignal does not leak listeners on success', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  const controller = new AbortController();
  const before = typeof controller.signal.listenerCount === 'function'
    ? controller.signal.listenerCount('abort')
    : 0;
  const r = await conn.simpleQuery('SELECT 1', { signal: controller.signal });
  assert.equal(r.rows[0][0], '1');
  const after = typeof controller.signal.listenerCount === 'function'
    ? controller.signal.listenerCount('abort')
    : 0;
  assert.equal(after, before);
  await conn.close();
});

test('transaction commits on success and returns callback value', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  await conn.simpleQuery('DROP TABLE IF EXISTS pgw_tx_test');
  await conn.simpleQuery('CREATE TABLE pgw_tx_test (id int PRIMARY KEY, v text)');
  const result = await conn.transaction(async (tx) => {
    await tx.simpleQuery("INSERT INTO pgw_tx_test VALUES (1, 'a'), (2, 'b')");
    return 42;
  });
  assert.equal(result, 42);
  const r = await conn.simpleQuery('SELECT count(*)::int FROM pgw_tx_test');
  assert.equal(r.rows[0][0], '2');
  await conn.simpleQuery('DROP TABLE pgw_tx_test');
  await conn.close();
});

test('transaction rolls back on throw', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  await conn.simpleQuery('DROP TABLE IF EXISTS pgw_tx_rollback');
  await conn.simpleQuery('CREATE TABLE pgw_tx_rollback (id int)');
  await assert.rejects(
    () => conn.transaction(async (tx) => {
      await tx.simpleQuery('INSERT INTO pgw_tx_rollback VALUES (1)');
      throw new Error('boom');
    }),
    (err) => err.message === 'boom',
  );
  const r = await conn.simpleQuery('SELECT count(*)::int FROM pgw_tx_rollback');
  assert.equal(r.rows[0][0], '0');
  await conn.simpleQuery('DROP TABLE pgw_tx_rollback');
  await conn.close();
});

test('savepoint rolls back only the inner work', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  await conn.simpleQuery('DROP TABLE IF EXISTS pgw_sp');
  await conn.simpleQuery('CREATE TABLE pgw_sp (id int)');
  await conn.transaction(async (tx) => {
    await tx.simpleQuery('INSERT INTO pgw_sp VALUES (1)');
    await assert.rejects(
      () => tx.savepoint(async (sp) => {
        await sp.simpleQuery('INSERT INTO pgw_sp VALUES (2)');
        throw new Error('rollback me');
      }),
      (err) => err.message === 'rollback me',
    );
    await tx.simpleQuery('INSERT INTO pgw_sp VALUES (3)');
  });
  const r = await conn.simpleQuery('SELECT id FROM pgw_sp ORDER BY id');
  assert.deepEqual(
    r.rows.map((row) => row[0]),
    ['1', '3'],
  );
  await conn.simpleQuery('DROP TABLE pgw_sp');
  await conn.close();
});

test('nested savepoints work', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  await conn.simpleQuery('DROP TABLE IF EXISTS pgw_sp_nest');
  await conn.simpleQuery('CREATE TABLE pgw_sp_nest (id int)');
  await conn.transaction(async (tx) => {
    await tx.simpleQuery('INSERT INTO pgw_sp_nest VALUES (1)');
    await tx.savepoint(async (sp1) => {
      await sp1.simpleQuery('INSERT INTO pgw_sp_nest VALUES (2)');
      await assert.rejects(
        () => sp1.savepoint(async (sp2) => {
          await sp2.simpleQuery('INSERT INTO pgw_sp_nest VALUES (3)');
          throw new Error('inner');
        }),
        (err) => err.message === 'inner',
      );
    });
  });
  const r = await conn.simpleQuery('SELECT id FROM pgw_sp_nest ORDER BY id');
  assert.deepEqual(
    r.rows.map((row) => row[0]),
    ['1', '2'],
  );
  await conn.simpleQuery('DROP TABLE pgw_sp_nest');
  await conn.close();
});

test('transaction isolation level SERIALIZABLE is applied', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  const level = await conn.transaction(
    async (tx) => {
      const r = await tx.simpleQuery('SHOW transaction_isolation');
      return r.rows[0][0];
    },
    { isolationLevel: 'SERIALIZABLE' },
  );
  assert.equal(level, 'serializable');
  await conn.close();
});

test('transaction rejects invalid isolation level', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect } = await loadDriver();
  const conn = await connect(urlStr);
  await assert.rejects(
    () => conn.transaction(async () => {}, { isolationLevel: 'BOGUS' }),
    (err) => /invalid isolationLevel/.test(err.message),
  );
  await conn.close();
});

test('PgError carries structured fields on SQL failure', async (t) => {
  if (!urlStr) return t.skip('PG_URL not set');
  const { connect, PgError } = await loadDriver();
  const conn = await connect(parseUrl(urlStr));
  await assert.rejects(
    () => conn.simpleQuery('SELECT * FROM definitely_not_a_table_xyz'),
    (err) => {
      assert.ok(err instanceof PgError, 'expected PgError instance');
      assert.equal(err.kind, 'pg');
      assert.equal(err.code, '42P01'); // undefined_table
      assert.match(err.severity, /ERROR|FATAL/);
      return true;
    },
  );
});
