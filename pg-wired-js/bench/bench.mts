import pg from 'pg';
import postgres from 'postgres';
import pgw from 'pg-wired';

const { connect: pgwConnect, parsePgUrl, columnarCell, binary } = pgw;
const OID = binary.TYPE_OID;

const url = process.env.PG_URL;
if (!url) {
  console.error('PG_URL is required');
  process.exit(1);
}

const ITERATIONS = Number(process.env.ITER ?? 5_000);
const WARMUP = Number(process.env.WARMUP ?? 500);

interface Row {
  label: string;
  ops: string;
  meanUs: string;
  p50Us: string;
  p99Us: string;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx]!;
}

function summarize(label: string, samples: number[], totalMs: number): Row {
  samples.sort((a, b) => a - b);
  const p50 = percentile(samples, 50);
  const p99 = percentile(samples, 99);
  const mean = samples.reduce((a, b) => a + b, 0) / samples.length;
  const ops = (samples.length / totalMs) * 1000;
  return {
    label,
    ops: ops.toFixed(0),
    meanUs: (mean * 1000).toFixed(1),
    p50Us: (p50 * 1000).toFixed(1),
    p99Us: (p99 * 1000).toFixed(1),
  };
}

async function run(label: string, fn: () => Promise<unknown>): Promise<Row> {
  for (let i = 0; i < WARMUP; i++) await fn();
  const samples = new Array<number>(ITERATIONS);
  const t0 = performance.now();
  for (let i = 0; i < ITERATIONS; i++) {
    const s = performance.now();
    await fn();
    samples[i] = performance.now() - s;
  }
  const total = performance.now() - t0;
  return summarize(label, samples, total);
}

function printTable(title: string, rows: Row[]): void {
  console.log(`\n== ${title} ==`);
  const header = ['driver', 'ops/s', 'mean(us)', 'p50(us)', 'p99(us)'];
  const widths = header.map((h) => h.length);
  for (const r of rows) {
    widths[0] = Math.max(widths[0]!, r.label.length);
    widths[1] = Math.max(widths[1]!, r.ops.length);
    widths[2] = Math.max(widths[2]!, r.meanUs.length);
    widths[3] = Math.max(widths[3]!, r.p50Us.length);
    widths[4] = Math.max(widths[4]!, r.p99Us.length);
  }
  const pad = (s: string, w: number): string => s.padEnd(w);
  console.log(header.map((h, i) => pad(h, widths[i]!)).join('  '));
  console.log(widths.map((w) => '-'.repeat(w)).join('  '));
  for (const r of rows) {
    console.log(
      [r.label, r.ops, r.meanUs, r.p50Us, r.p99Us]
        .map((v, i) => pad(v, widths[i]!))
        .join('  '),
    );
  }
}

const pgClient = new pg.Client({ connectionString: url });
await pgClient.connect();

const { host, port, user, password, database } = parsePgUrl(url);
const sql = postgres({
  host,
  port,
  user,
  pass: password,
  database,
  max: 1,
  prepare: true,
  fetch_types: false,
});

const wired = await pgwConnect(url);

const r1: Row[] = [];
r1.push(
  await run('pg           ', async () => {
    await pgClient.query('SELECT 1');
  }),
);
r1.push(
  await run('postgres.js  ', async () => {
    await sql`SELECT 1`;
  }),
);
r1.push(
  await run('pg-wired     ', async () => {
    await wired.simpleQuery('SELECT 1');
  }),
);
printTable('SELECT 1 (simple)', r1);

const r2: Row[] = [];
r2.push(
  await run('pg           ', async () => {
    await pgClient.query('SELECT id, name, score FROM bench_rows WHERE id = $1', [1]);
  }),
);
r2.push(
  await run('postgres.js  ', async () => {
    await sql`SELECT id, name, score FROM bench_rows WHERE id = ${1}`;
  }),
);
r2.push(
  await run('pg-wired     ', async () => {
    await wired.query(
      'SELECT id, name, score FROM bench_rows WHERE id = $1',
      [1n],
      [OID.INT8],
    );
  }),
);
printTable('Parameterized point-select', r2);

const r3: Row[] = [];
r3.push(
  await run('pg           ', async () => {
    await pgClient.query('SELECT id, name, score FROM bench_rows ORDER BY id LIMIT 100');
  }),
);
r3.push(
  await run('postgres.js  ', async () => {
    await sql`SELECT id, name, score FROM bench_rows ORDER BY id LIMIT 100`;
  }),
);
r3.push(
  await run('pg-wired     ', async () => {
    await wired.simpleQuery('SELECT id, name, score FROM bench_rows ORDER BY id LIMIT 100');
  }),
);
r3.push(
  await run('pg-wired colm', async () => {
    const res = await wired.simpleQueryColumnar(
      'SELECT id, name, score FROM bench_rows ORDER BY id LIMIT 100',
    );
    for (let r = 0; r < res.rowCount; r++) {
      for (let c = 0; c < res.cols; c++) columnarCell(res, r, c);
    }
  }),
);
printTable('100-row scan', r3);

const BATCH = 10;
const r4: Row[] = [];
r4.push(
  await run('pg (seq)     ', async () => {
    for (let i = 0; i < BATCH; i++) {
      await pgClient.query('SELECT id FROM bench_rows WHERE id = $1', [i + 1]);
    }
  }),
);
r4.push(
  await run('postgres.js  ', async () => {
    await Promise.all(
      Array.from(
        { length: BATCH },
        (_, i) => sql`SELECT id FROM bench_rows WHERE id = ${i + 1}`,
      ),
    );
  }),
);
r4.push(
  await run('pg-wired pipe', async () => {
    const pipe = wired.pipeline();
    for (let i = 0; i < BATCH; i++) {
      pipe.push(
        'SELECT id FROM bench_rows WHERE id = $1',
        [BigInt(i + 1)],
        [OID.INT8],
      );
    }
    await pipe.execute();
  }),
);
printTable(`Batch of ${BATCH} point-selects`, r4);

const r5: Row[] = [];
r5.push(
  await run('pg           ', async () => {
    const res = await pgClient.query(
      'SELECT id, name, score FROM bench_rows ORDER BY id LIMIT 1000',
    );
    let count = 0;
    for (const _row of res.rows) count++;
    if (count !== 1000) throw new Error('bad count');
  }),
);
r5.push(
  await run('postgres.js  ', async () => {
    const rows = await sql`SELECT id, name, score FROM bench_rows ORDER BY id LIMIT 1000`;
    let count = 0;
    for (const _r of rows) count++;
    if (count !== 1000) throw new Error('bad count');
  }),
);
r5.push(
  await run('pg-wired stream', async () => {
    const stream = await wired.queryStream(
      'SELECT id, name, score FROM bench_rows ORDER BY id LIMIT $1::int',
      [Buffer.from('1000')],
      [],
    );
    let count = 0;
    for await (const _row of stream) count++;
    if (count !== 1000) throw new Error('bad count');
  }),
);
r5.push(
  await run('pg-wired buffered', async () => {
    const res = await wired.simpleQuery(
      'SELECT id, name, score FROM bench_rows ORDER BY id LIMIT 1000',
    );
    let count = 0;
    for (const _row of res.rows) count++;
    if (count !== 1000) throw new Error('bad count');
  }),
);
r5.push(
  await run('pg-wired columnar', async () => {
    const res = await wired.simpleQueryColumnar(
      'SELECT id, name, score FROM bench_rows ORDER BY id LIMIT 1000',
    );
    let count = 0;
    for (let r = 0; r < res.rowCount; r++) {
      for (let c = 0; c < res.cols; c++) columnarCell(res, r, c);
      count++;
    }
    if (count !== 1000) throw new Error('bad count');
  }),
);
printTable('Stream 1000 rows', r5);

await pgClient.end();
await sql.end();
process.exit(0);
